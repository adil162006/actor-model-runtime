import { Router } from 'express';
import Actor from '../models/Actor.js';
import Message from '../models/Message.js';
import ActorMigration from '../models/ActorMigration.js';
import {
  dashboardData,
  listWorkers,
  allRuntimeEvents,
  singletonStatus,
  rebalance,
  createActor,
  sendMessage,
  recordEvent,
  resetDemoData,
  rebalanceMinimumWorkers,
} from '../services/runtime.js';
import {
  getClusterStatus,
  scaleWorkers,
  getPodLogs,
} from '../services/kubernetes.js';
import { asyncHandler } from '../middleware/errors.js';
const router = Router();
const render = (res, view, page, locals = {}) =>
  res.render('layout', { page, view, ...locals });

router.get(
  '/',
  asyncHandler(async (_req, res) =>
    render(res, 'dashboard', 'Dashboard', {
      data: await dashboardData(),
      singleton: await singletonStatus(),
      cluster: await getClusterStatus(),
    }),
  ),
);
router.get(
  '/actors',
  asyncHandler(async (_req, res) => {
    const actors = await Actor.find().sort({ createdAt: -1 }).lean();
    const workers = await listWorkers();
    const activeIds = new Set(workers.map((worker) => worker.workerId));
    const pending = await Message.aggregate([
      { $match: { status: { $in: ['PENDING', 'PROCESSING'] } } },
      { $group: { _id: '$actorId', count: { $sum: 1 } } },
    ]);
    const counts = new Map(pending.map((item) => [item._id, item.count]));
    render(res, 'actors', 'Actors', {
      actors: actors.map((actor) => ({
        ...actor,
        pendingMessageCount: counts.get(actor.actorId) || 0,
        ownerStatus: !actor.workerId
          ? 'UNASSIGNED'
          : activeIds.has(actor.workerId)
            ? 'ACTIVE'
            : 'UNAVAILABLE',
      })),
    });
  }),
);
router.get(
  '/actors/:id',
  asyncHandler(async (req, res) => {
    const actor = await Actor.findOne({ actorId: req.params.id }).lean();
    if (!actor)
      return res.status(404).render('layout', {
        view: 'not-found',
        page: 'Not found',
        message: 'Actor not found',
      });
    const liveWorkers = await listWorkers();
    actor.ownerStatus = !actor.workerId
      ? 'UNASSIGNED'
      : liveWorkers.some((worker) => worker.workerId === actor.workerId)
        ? 'ACTIVE'
        : 'UNAVAILABLE';
    const messages = await Message.find({ actorId: actor.actorId })
      .sort({ createdAt: -1 })
      .limit(20)
      .lean();
    render(res, 'actor-details', actor.actorId, { actor, messages });
  }),
);
router.get(
  '/workers',
  asyncHandler(async (_req, res) => {
    const registry = await listWorkers()
      .then((workers) => ({ workers, available: true }))
      .catch(() => ({ workers: [], available: false }));
    const cluster = await getClusterStatus();
    const clusterWorkers = (cluster.pods || []).map((pod) => ({
      name: pod.metadata?.name,
      workerId: pod.metadata?.name,
      namespace: pod.metadata?.namespace,
      status: pod.status?.phase,
      ip: pod.status?.podIP,
      ready:
        pod.status?.conditions?.find((condition) => condition.type === 'Ready')
          ?.status === 'True',
      node: pod.spec?.nodeName,
      metrics: cluster.resourceMetrics?.workers?.[pod.metadata?.name] || null,
    }));
    return render(res, 'workers', 'Workers', {
      ...registry,
      actors: await Actor.find().lean(),
      clusterWorkers,
      cluster,
    });
  }),
);
router.get(
  '/rebalancing',
  asyncHandler(async (_req, res) => {
    const registry = await listWorkers()
      .then((workers) => ({ workers, available: true }))
      .catch(() => ({ workers: [], available: false }));
    return render(res, 'rebalancing', 'Rebalancing', {
      ...registry,
      minimumWorkers: rebalanceMinimumWorkers,
      autoRebalanceIntervalMs: Number(
        process.env.AUTO_REBALANCE_INTERVAL_MS || 10000,
      ),
      actors: await Actor.find().lean(),
      events: await allRuntimeEvents(20),
      migrations: await ActorMigration.find()
        .sort({ startedAt: -1 })
        .limit(30)
        .lean(),
    });
  }),
);
router.get(
  '/singletons',
  asyncHandler(async (_req, res) => {
    const registry = await listWorkers()
      .then((workers) => ({ workers, available: true }))
      .catch(() => ({ workers: [], available: false }));
    return render(res, 'singletons', 'Singletons', {
      state: await singletonStatus(),
      ...registry,
      actors: await Actor.find({ actorType: 'GlobalSchedulerActor' }).lean(),
      events: await allRuntimeEvents(20),
    });
  }),
);
router.get(
  '/cluster',
  asyncHandler(async (_req, res) => {
    const cluster = await getClusterStatus();
    render(res, 'cluster', 'Cluster', {
      cluster,
      pods: cluster.connected ? cluster.pods : [],
      logs: null,
      logError: null,
    });
  }),
);
router.get('/guide', (_req, res) =>
  render(res, 'guide', 'Operator guide', {
    heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS || 4000),
    workerTimeoutMs: Number(process.env.WORKER_TIMEOUT_MS || 15000),
    idleTimeoutMs: Number(process.env.ACTOR_IDLE_TIMEOUT_MS || 60000),
    rebalanceIntervalMs: Number(
      process.env.AUTO_REBALANCE_INTERVAL_MS || 10000,
    ),
    minimumWorkers: rebalanceMinimumWorkers,
  }),
);
router.post(
  '/cluster/scale',
  asyncHandler(async (req, res) => {
    const replicas = Number(req.body.replicas);
    if (!Number.isInteger(replicas) || replicas < 0 || replicas > 50)
      return res.redirect(
        '/cluster?error=Enter%20a%20replica%20count%20from%200%20to%2050',
      );
    await scaleWorkers(replicas);
    res.redirect('/cluster?message=Worker%20scale%20requested');
  }),
);
router.get(
  '/cluster/logs/:pod',
  asyncHandler(async (req, res) => {
    const cluster = await getClusterStatus();
    if (!cluster.connected)
      return res.status(503).render('layout', {
        view: 'cluster',
        page: 'Cluster',
        cluster,
        pods: [],
        logs: null,
        logError: 'Kubernetes is unavailable',
      });
    const logs = await getPodLogs(req.params.pod);
    render(res, 'cluster', 'Cluster', {
      cluster,
      pods: cluster.pods || [],
      logs,
      logError: null,
      selectedPod: req.params.pod,
    });
  }),
);
router.post(
  '/actors/create',
  asyncHandler(async (req, res) => {
    const actorId = String(req.body.actorId || '').trim();
    if (
      !/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,119}$/.test(actorId) ||
      !['CounterActor', 'BankAccountActor', 'GlobalSchedulerActor'].includes(
        req.body.actorType,
      )
    )
      return res.redirect('/actors?error=Invalid%20actor%20ID%20or%20type');
    try {
      const actor = await createActor({
        actorId,
        actorType: req.body.actorType,
      });
      res.redirect(`/actors/${encodeURIComponent(actor.actorId)}`);
    } catch (error) {
      res.redirect(`/actors?error=${encodeURIComponent(error.message)}`);
    }
  }),
);
router.post(
  '/demo/reset',
  asyncHandler(async (req, res) => {
    if (String(req.body.confirmation || '').trim() !== 'RESET DEMO')
      return res.redirect(
        '/actors?error=Type%20RESET%20DEMO%20to%20confirm%20the%20demo%20reset',
      );
    try {
      const result = await resetDemoData();
      res.redirect(
        `/actors?message=${encodeURIComponent(
          `Demo reset: removed ${result.actors} actors, ${result.messages} messages, and runtime history`,
        )}`,
      );
    } catch (error) {
      res.redirect(`/actors?error=${encodeURIComponent(error.message)}`);
    }
  }),
);
router.post(
  '/actors/:id/message',
  asyncHandler(async (req, res) => {
    try {
      await sendMessage(req.params.id, {
        operation: req.body.operation,
        payload: {
          amount: req.body.amount ? Number(req.body.amount) : undefined,
          name: req.body.name,
        },
      });
      res.redirect(
        `/actors/${encodeURIComponent(req.params.id)}?message=Message%20processed`,
      );
    } catch (error) {
      res.redirect(
        `/actors/${encodeURIComponent(req.params.id)}?error=${encodeURIComponent(error.message)}`,
      );
    }
  }),
);
router.post(
  '/actors/:id/status',
  asyncHandler(async (req, res) => {
    if (!['active', 'idle'].includes(req.body.status))
      return res.redirect(
        `/actors/${encodeURIComponent(req.params.id)}?error=Invalid%20actor%20status`,
      );
    const actor = await Actor.findOneAndUpdate(
      { actorId: req.params.id },
      { status: req.body.status },
    );
    if (!actor) return res.redirect('/actors?error=Actor%20not%20found');
    await recordEvent(
      'actor.status',
      `${req.params.id} marked ${req.body.status}`,
      { actorId: req.params.id },
    );
    res.redirect(
      `/actors/${encodeURIComponent(req.params.id)}?message=Actor%20status%20updated`,
    );
  }),
);
router.post(
  '/actors/:id/keep-warm',
  asyncHandler(async (req, res) => {
    const keepWarm = req.body.keepWarm === 'true';
    const actor = await Actor.findOneAndUpdate(
      { actorId: req.params.id },
      { keepWarm },
      { new: true },
    );
    if (!actor) return res.redirect('/actors?error=Actor%20not%20found');
    await recordEvent(
      'actor.keep_warm',
      `${actor.actorId} ${keepWarm ? 'will stay warm' : 'can idle normally'}`,
      { actorId: actor.actorId, keepWarm },
    );
    res.redirect(
      `/actors/${encodeURIComponent(actor.actorId)}?message=${encodeURIComponent(
        keepWarm
          ? 'Keep warm enabled: idle deactivation is disabled for this actor'
          : 'Keep warm disabled: normal idle deactivation is restored',
      )}`,
    );
  }),
);
router.post(
  '/rebalancing/trigger',
  asyncHandler(async (_req, res) => {
    try {
      const result = await rebalance();
      res.redirect(
        `/rebalancing?message=${encodeURIComponent(`Moved ${result.moved} actors`)}`,
      );
    } catch (error) {
      res.redirect(`/rebalancing?error=${encodeURIComponent(error.message)}`);
    }
  }),
);
router.post(
  '/singletons/register',
  asyncHandler(async (_req, res) => {
    try {
      await createActor({
        actorId: 'global-scheduler',
        actorType: 'GlobalSchedulerActor',
      });
      res.redirect('/singletons?message=GlobalSchedulerActor%20registered');
    } catch (error) {
      res.redirect(`/singletons?error=${encodeURIComponent(error.message)}`);
    }
  }),
);
export default router;
