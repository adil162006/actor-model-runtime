import express from 'express';
import os from 'node:os';
import crypto from 'node:crypto';
import mongoose from 'mongoose';
import { connectDatabase } from '../config/database.js';
import { connectRedis, redis } from '../config/redis.js';
import {
  workerProcess,
  acquireSingleton,
  renewSingleton,
  recoverPendingMessages,
  deactivateIdleActors,
} from '../services/runtime.js';
import { errorHandler } from '../middleware/errors.js';
import Actor from '../models/Actor.js';
import Message from '../models/Message.js';

const app = express();
app.use(express.json({ limit: '32kb' }));
const kubernetesMode = Boolean(process.env.POD_NAME);
const workerId = kubernetesMode
  ? process.env.POD_NAME
  : process.env.WORKER_ID || `${os.hostname()}-local`;
const port = Number(process.env.WORKER_PORT || 3101);
const coordinator = process.env.COORDINATOR_URL || 'http://localhost:3000';
const advertisedHost =
  process.env.WORKER_ADVERTISE_HOST ||
  process.env.POD_IP ||
  (process.env.POD_NAME
    ? `${process.env.POD_NAME}.actor-worker-headless.actorlab.svc.cluster.local`
    : '127.0.0.1');
const tails = new Map();
const drainingActors = new Set();
const drainLocks = new Map();
let ready = false;
let activeActorCount = 0;
let pendingMessageCount = 0;
function enqueue(actorId, task) {
  const previous = tails.get(actorId) || Promise.resolve();
  const next = previous.then(task, task);
  const tracked = next.finally(() => {
    if (tails.get(actorId) === tracked) tails.delete(actorId);
  });
  tails.set(actorId, tracked);
  return tracked;
}

app.get('/health', (_req, res) => {
  const dependenciesReady =
    mongoose.connection.readyState === 1 && redis.isReady;
  res.status(ready && dependenciesReady ? 200 : 503).json({
    ok: ready && dependenciesReady,
    workerId,
    activeActors: activeActorCount,
    pendingMessageCount,
    inFlightActors: tails.size,
  });
});
async function refreshWorkerMetrics() {
  activeActorCount = await Actor.countDocuments({ workerId, status: 'active' });
  const actorIds = await Actor.distinct('actorId', { workerId });
  pendingMessageCount = await Message.countDocuments({
    actorId: { $in: actorIds },
    status: { $in: ['PENDING', 'PROCESSING'] },
  });
}
app.post('/internal/actors/:id/message', (req, res, next) => {
  if (!ready)
    return res.status(503).json({ error: 'Worker is not accepting work' });
  if (drainingActors.has(req.params.id))
    return res.status(503).json({ error: 'Actor is draining for migration' });
  return enqueue(req.params.id, () =>
    workerProcess(workerId, req.params.id, req.body),
  )
    .then((data) => res.json(data))
    .catch(next);
});
app.post('/internal/actors/:id/drain', async (req, res) => {
  const actorId = req.params.id;
  if (drainLocks.has(actorId))
    return res.json({ drained: true, alreadyDrained: true });
  drainingActors.add(actorId);
  await (tails.get(actorId) || Promise.resolve());
  const key = `actorlab:actor:${actorId}:execution`;
  const token = crypto.randomUUID();
  const deadline = Date.now() + 10000;
  while (!(await redis.set(key, token, { NX: true, PX: 120000 }))) {
    if (Date.now() >= deadline) {
      drainingActors.delete(actorId);
      return res
        .status(503)
        .json({ error: 'Actor execution did not drain in time' });
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  drainLocks.set(actorId, { key, token });
  res.json({ drained: true });
});
app.post('/internal/actors/:id/resume', (req, res) => {
  const actorId = req.params.id;
  const lock = drainLocks.get(actorId);
  if (lock) {
    redis
      .eval(
        `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
        { keys: [lock.key], arguments: [lock.token] },
      )
      .catch(() => {});
    drainLocks.delete(actorId);
  }
  drainingActors.delete(actorId);
  res.json({ resumed: true });
});
app.use(errorHandler);

let lease;
async function post(path, body) {
  const response = await fetch(`${coordinator}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(3000),
  });
  if (!response.ok)
    throw Object.assign(new Error(`Coordinator returned ${response.status}`), {
      status: response.status,
    });
  return response.json();
}
async function heartbeat() {
  await refreshWorkerMetrics();
  const info = {
    workerId,
    podName: process.env.POD_NAME || null,
    namespace: process.env.POD_NAMESPACE || null,
    podIP: process.env.POD_IP || null,
    url: `http://${advertisedHost}:${port}`,
    activeActors: activeActorCount,
    pendingMessageCount,
    host: os.hostname(),
    mode: kubernetesMode ? 'kubernetes' : process.env.RUNTIME_MODE || 'local',
    status: ready ? 'ACTIVE' : 'STARTING',
  };
  try {
    await post('/internal/workers/heartbeat', info);
  } catch (error) {
    if (error.status !== 404) throw error;
    await post('/internal/workers/register', info);
  }
  if (!lease)
    lease = await post('/internal/singletons/acquire', { workerId }).then(
      (r) => r.lease,
    );
  else
    lease = await post('/internal/singletons/renew', { lease }).then(
      (r) => r.lease,
    );
}

export async function startWorker() {
  await connectDatabase();
  await connectRedis();
  const server = app.listen(port, async () => {
    console.log(`Worker ${workerId} listening on ${port}`);
    try {
      await post(
        '/internal/workers/register',
        workerInfo({
          status: 'ACTIVE',
          activeActors: 0,
        }),
      );
      ready = true;
    } catch (error) {
      console.error('Worker registration failed:', error.message);
    }
  });
  const workerInfo = (extra = {}) => ({
    workerId,
    podName: process.env.POD_NAME || null,
    namespace: process.env.POD_NAMESPACE || null,
    podIP: process.env.POD_IP || null,
    url: `http://${advertisedHost}:${port}`,
    host: os.hostname(),
    mode: kubernetesMode ? 'kubernetes' : process.env.RUNTIME_MODE || 'local',
    ...extra,
  });
  const timer = setInterval(
    () =>
      heartbeat().catch((error) =>
        console.error('Worker heartbeat failed:', error.message),
      ),
    Number(process.env.HEARTBEAT_INTERVAL_MS || 4000),
  );
  const recoveryTimer = setInterval(
    () => {
      if (ready)
        recoverPendingMessages(workerId).catch((error) =>
          console.error('Message recovery failed:', error.message),
        );
    },
    Number(process.env.MESSAGE_RECOVERY_INTERVAL_MS || 1000),
  );
  const idleTimer = setInterval(
    () => {
      if (ready)
        deactivateIdleActors(workerId).catch((error) =>
          console.error('Actor deactivation failed:', error.message),
        );
    },
    Number(process.env.ACTOR_DEACTIVATION_INTERVAL_MS || 10000),
  );
  const shutdown = async () => {
    ready = false;
    clearInterval(timer);
    clearInterval(recoveryTimer);
    clearInterval(idleTimer);
    const closed = new Promise((resolve) => server.close(resolve));
    await post('/internal/workers/drain', { workerId }).catch(() => {});
    await Promise.allSettled([...tails.values()]);
    await closed;
    if (lease?.token)
      await redis
        .eval(
          `local raw = redis.call('get', KEYS[1]); if not raw then return 0 end; local owner = cjson.decode(raw); if owner.workerId == ARGV[1] and owner.token == ARGV[2] and tonumber(owner.generation) == tonumber(ARGV[3]) then return redis.call('del', KEYS[1]) else return 0 end`,
          {
            keys: ['actorlab:singleton:global-scheduler'],
            arguments: [workerId, lease.token, String(lease.generation)],
          },
        )
        .catch(() => {});
    await redis.quit().catch(() => {});
    process.exit(0);
  };
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);
}
