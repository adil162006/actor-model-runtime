import { Router } from 'express';
import Actor from '../models/Actor.js';
import Message from '../models/Message.js';
import ActorMigration from '../models/ActorMigration.js';
import {
  createActor,
  sendMessage,
  registerWorker,
  heartbeat,
  rebalance,
  singletonStatus,
  dashboardData,
  listWorkers,
  allRuntimeEvents,
  recordEvent,
} from '../services/runtime.js';
import {
  getClusterStatus,
  scaleWorkers,
  getPodLogs,
} from '../services/kubernetes.js';
import { asyncHandler } from '../middleware/errors.js';
const router = Router();

router.get('/health', (_req, res) =>
  res.json({ ok: true, service: 'actorlab-api' }),
);
router.get(
  '/dashboard',
  asyncHandler(async (_req, res) => res.json(await dashboardData())),
);
router.get(
  '/actors',
  asyncHandler(async (_req, res) => {
    const actors = await Actor.find().sort({ createdAt: -1 }).lean();
    const pending = await Message.aggregate([
      { $match: { status: { $in: ['PENDING', 'PROCESSING'] } } },
      { $group: { _id: '$actorId', count: { $sum: 1 } } },
    ]);
    const counts = new Map(pending.map((item) => [item._id, item.count]));
    res.json(
      actors.map((actor) => ({
        ...actor,
        pendingMessageCount: counts.get(actor.actorId) || 0,
      })),
    );
  }),
);
router.post(
  '/actors',
  asyncHandler(async (req, res) => {
    const actorId = String(req.body.actorId || '').trim();
    const actorType = req.body.actorType;
    if (!/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,119}$/.test(actorId))
      return res.status(400).json({
        error:
          'actorId must be 1–120 letters, numbers, dots, underscores, colons or hyphens',
      });
    if (
      !['CounterActor', 'BankAccountActor', 'GlobalSchedulerActor'].includes(
        actorType,
      )
    )
      return res.status(400).json({ error: 'Unsupported actor type' });
    res.status(201).json(await createActor({ actorId, actorType }));
  }),
);
router.get(
  '/actors/:id',
  asyncHandler(async (req, res) => {
    const actor = await Actor.findOne({ actorId: req.params.id });
    if (!actor) return res.status(404).json({ error: 'Actor not found' });
    res.json(actor);
  }),
);
router.patch(
  '/actors/:id',
  asyncHandler(async (req, res) => {
    if (!['active', 'idle'].includes(req.body.status))
      return res.status(400).json({ error: 'status must be active or idle' });
    const actor = await Actor.findOneAndUpdate(
      { actorId: req.params.id },
      { status: req.body.status },
      { new: true },
    );
    if (!actor) return res.status(404).json({ error: 'Actor not found' });
    await recordEvent(
      'actor.status',
      `${actor.actorId} marked ${actor.status}`,
      { actorId: actor.actorId },
    );
    res.json(actor);
  }),
);
router.post(
  '/actors/:id/messages',
  asyncHandler(async (req, res) =>
    res.json(await sendMessage(req.params.id, req.body)),
  ),
);
router.get(
  '/actors/:id/messages',
  asyncHandler(async (req, res) =>
    res.json(
      await Message.find({ actorId: req.params.id })
        .sort({ createdAt: -1 })
        .limit(100)
        .lean(),
    ),
  ),
);
router.get(
  '/workers',
  asyncHandler(async (_req, res) => res.json(await listWorkers())),
);
router.post(
  '/workers/register',
  asyncHandler(async (req, res) => {
    const { workerId, url } = req.body;
    if (
      typeof workerId !== 'string' ||
      !/^[\w.-]{1,100}$/.test(workerId) ||
      typeof url !== 'string' ||
      !/^https?:\/\/[\w.:-]+$/.test(url)
    )
      return res.status(400).json({ error: 'Invalid workerId or URL' });
    res.status(201).json(await registerWorker({ ...req.body, workerId, url }));
  }),
);
router.post(
  '/workers/heartbeat',
  asyncHandler(async (req, res) => {
    const workers = await listWorkers();
    if (!workers.some((w) => w.workerId === req.body.workerId))
      return res.status(404).json({ error: 'Worker is not registered' });
    res.json(await heartbeat(req.body));
  }),
);
router.post(
  '/rebalance',
  asyncHandler(async (_req, res) => res.json(await rebalance())),
);
router.get(
  '/singletons',
  asyncHandler(async (_req, res) => res.json(await singletonStatus())),
);
router.get(
  '/cluster',
  asyncHandler(async (_req, res) => {
    const status = await getClusterStatus();
    res.json({
      ...status,
      nodes: status.connected
        ? (status.nodes || []).map((node) => ({
            name: node.metadata?.name,
            status:
              node.status?.conditions?.find(
                (condition) => condition.type === 'Ready',
              )?.status || 'Unknown',
          }))
        : [],
      deploymentStatus: status.deploymentStatus || null,
      resourceMetrics: status.resourceMetrics || {
        available: false,
        workers: {},
      },
      workers: status.connected
        ? (status.pods || []).map((pod) => ({
            name: pod.metadata?.name,
            status: pod.status?.phase,
            ip: pod.status?.podIP,
            node: pod.spec?.nodeName,
          }))
        : [],
    });
  }),
);
router.post(
  '/cluster/scale',
  asyncHandler(async (req, res) => {
    const replicas = Number(req.body.replicas);
    if (!Number.isInteger(replicas) || replicas < 0 || replicas > 50)
      return res
        .status(400)
        .json({ error: 'replicas must be an integer from 0 to 50' });
    await scaleWorkers(replicas);
    res.json({ ok: true, replicas });
  }),
);
router.get(
  '/cluster/pods/:name/logs',
  asyncHandler(async (req, res) =>
    res.type('text/plain').send(await getPodLogs(req.params.name)),
  ),
);
router.get(
  '/events',
  asyncHandler(async (_req, res) => res.json(await allRuntimeEvents())),
);
router.get(
  '/migrations',
  asyncHandler(async (_req, res) =>
    res.json(
      await ActorMigration.find().sort({ startedAt: -1 }).limit(100).lean(),
    ),
  ),
);
export default router;
