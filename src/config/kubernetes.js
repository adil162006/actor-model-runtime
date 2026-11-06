import { KubeConfig, CoreV1Api, AppsV1Api, CustomObjectsApi } from '@kubernetes/client-node';

let clients;
let connectionError;
export function getKubeClients() {
  if (clients || connectionError) return { clients, error: connectionError };
  try {
    const config = new KubeConfig();
    if (
      process.env.KUBERNETES_SERVICE_HOST &&
      process.env.KUBERNETES_SERVICE_PORT
    ) {
      config.loadFromCluster();
    } else {
      config.loadFromDefault();
    }
    const server = config.getCurrentCluster()?.server;
    if (!server) throw new Error('Kubernetes API server is not configured');
    const serverUrl = new URL(server);
    if (
      !['http:', 'https:'].includes(serverUrl.protocol) ||
      !serverUrl.hostname
    ) {
      throw new Error(`Invalid Kubernetes API server URL: ${server}`);
    }
    clients = {
      core: config.makeApiClient(CoreV1Api),
      apps: config.makeApiClient(AppsV1Api),
      custom: config.makeApiClient(CustomObjectsApi),
    };
  } catch (error) {
    connectionError = error;
  }
  return { clients, error: connectionError };
}
