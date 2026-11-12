import { getKubeClients } from '../config/kubernetes.js';
const namespace = process.env.K8S_NAMESPACE || 'actorlab';
const deploymentName = process.env.WORKER_DEPLOYMENT || 'actor-worker';
const autoscalerName =
  process.env.WORKER_AUTOSCALER || 'actor-worker-autoscaler';

export async function getClusterStatus() {
  const { clients, error } = getKubeClients();
  if (!clients)
    return {
      connected: false,
      mode: 'Local Development Mode',
      message: error?.message || 'Kubernetes client is not configured',
    };
  try {
    const query = async () => {
      const pods = await clients.core.listNamespacedPod({
        namespace,
        labelSelector: 'app=actor-worker',
      });
      const nodes = await clients.core.listNode();
      const resourceMetrics = await getWorkerResourceMetrics();
      let deployment = null;
      let deploymentStatus = null;
      let autoscaler = null;
      let autoscalerStatus = null;
      try {
        const result = await clients.apps.readNamespacedDeployment({
          name: deploymentName,
          namespace,
        });
        deployment = result.spec;
        deploymentStatus = result.status;
      } catch {}
      try {
        const result =
          await clients.autoscaling.readNamespacedHorizontalPodAutoscaler({
            name: autoscalerName,
            namespace,
          });
        autoscaler = result.spec;
        autoscalerStatus = result.status;
      } catch {}
      return {
        connected: true,
        mode: 'Kubernetes',
        message: deployment
          ? 'Connected to Kubernetes'
          : `Cluster reachable; ${deploymentName} Deployment was not found in ${namespace}`,
        pods: pods.items,
        nodes: nodes.items,
        deployment,
        deploymentStatus,
        autoscaler,
        autoscalerStatus,
        resourceMetrics,
      };
    };
    let timer;
    return await Promise.race([
      query(),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('Kubernetes API request timed out')),
          Number(process.env.K8S_REQUEST_TIMEOUT_MS || 3000),
        );
      }),
    ]).finally(() => clearTimeout(timer));
  } catch (cause) {
    return {
      connected: false,
      mode: 'Local Development Mode',
      message: cause.message,
      pods: [],
    };
  }
}
export async function getPods() {
  const status = await getClusterStatus();
  return status.connected ? status.pods : [];
}

function quantity(value, cpu = false) {
  if (!value) return 0;
  const match = String(value).match(/^([\d.]+)([a-zA-Z]*)$/);
  if (!match) return 0;
  const number = Number(match[1]);
  const unit = match[2];
  if (cpu)
    return unit === 'n'
      ? number / 1e6
      : unit === 'u'
        ? number / 1e3
        : unit === 'm'
          ? number
          : number * 1000;
  const factors = {
    Ki: 1024,
    Mi: 1024 ** 2,
    Gi: 1024 ** 3,
    Ti: 1024 ** 4,
    k: 1000,
    M: 1000 ** 2,
    G: 1000 ** 3,
  };
  return number * (factors[unit] || 1);
}

export async function getWorkerResourceMetrics() {
  const { clients } = getKubeClients();
  if (!clients) return { available: false, workers: {} };
  try {
    // client-node v1 uses object parameters for CustomObjectsApi methods.
    // Positional arguments silently produce an invalid request, leaving the
    // dashboard with blank metrics even while the HPA can read them.
    const metrics = await clients.custom.listNamespacedCustomObject({
      group: 'metrics.k8s.io',
      version: 'v1beta1',
      namespace,
      plural: 'pods',
    });
    const pods = await clients.core.listNamespacedPod({
      namespace,
      labelSelector: 'app=actor-worker',
    });
    const podMap = new Map(pods.items.map((pod) => [pod.metadata?.name, pod]));
    const workers = {};
    for (const item of metrics.items || metrics.body?.items || []) {
      const pod = podMap.get(item.metadata?.name);
      if (!pod) continue;
      const usage = (item.containers || []).reduce(
        (sum, container) => ({
          cpu: sum.cpu + quantity(container.usage?.cpu, true),
          memory: sum.memory + quantity(container.usage?.memory),
        }),
        { cpu: 0, memory: 0 },
      );
      const requests = (pod.spec?.containers || []).reduce(
        (sum, container) => ({
          cpu: sum.cpu + quantity(container.resources?.requests?.cpu, true),
          memory: sum.memory + quantity(container.resources?.requests?.memory),
        }),
        { cpu: 0, memory: 0 },
      );
      workers[item.metadata.name] = {
        cpuMillicores: Math.round(usage.cpu),
        memoryBytes: Math.round(usage.memory),
        cpuPercentOfRequest: requests.cpu
          ? Math.round((usage.cpu / requests.cpu) * 100)
          : null,
        memoryPercentOfRequest: requests.memory
          ? Math.round((usage.memory / requests.memory) * 100)
          : null,
      };
    }
    return { available: Object.keys(workers).length > 0, workers };
  } catch {
    return { available: false, workers: {} };
  }
}
export async function getWorkers() {
  return (await getPods()).map((pod) => ({
    name: pod.metadata?.name,
    status: pod.status?.phase,
    ip: pod.status?.podIP,
    node: pod.spec?.nodeName,
  }));
}
export async function scaleWorkers(replicas) {
  const { clients } = getKubeClients();
  if (!clients)
    throw Object.assign(new Error('Kubernetes is unavailable'), {
      status: 503,
    });
  await clients.apps.patchNamespacedDeployment({
    name: deploymentName,
    namespace,
    body: [{ op: 'add', path: '/spec/replicas', value: replicas }],
  });
}
export async function getPodLogs(podName) {
  const { clients } = getKubeClients();
  if (!clients)
    throw Object.assign(new Error('Kubernetes is unavailable'), {
      status: 503,
    });
  const timeout = new Promise((_, reject) =>
    setTimeout(
      () =>
        reject(
          Object.assign(new Error('Kubernetes API request timed out'), {
            status: 504,
          }),
        ),
      Number(process.env.K8S_REQUEST_TIMEOUT_MS || 3000),
    ),
  );
  return Promise.race([
    clients.core.readNamespacedPodLog({
      name: podName,
      namespace,
      tailLines: 100,
    }),
    timeout,
  ]);
}
