import crypto from 'node:crypto';
import Actor from '../models/Actor.js';
import Message from '../models/Message.js';
import RuntimeEvent from '../models/RuntimeEvent.js';
import ActorMigration from '../models/ActorMigration.js';
import { getWorkerResourceMetrics } from './kubernetes.js';
import { redis } from '../config/redis.js';

const OWNER_KEY = 'actorlab:owners';
const WORKER_KEY = 'actorlab:workers';
const LEASE_KEY = 'actorlab:singleton:global-scheduler';
const acquireLeaseScript = `
if redis.call('exists', KEYS[1]) == 1 then return 0 end
local currentGeneration = tonumber(redis.call('get', KEYS[2]) or '0')
local durableFloor = tonumber(ARGV[5]) or 0
if currentGeneration < durableFloor then redis.call('set', KEYS[2], durableFloor) end
local generation = redis.call('incr', KEYS[2])
local owner = cjson.encode({workerId=ARGV[1], token=ARGV[2], generation=generation})
redis.call('psetex', KEYS[1], ARGV[3], owner)
redis.call('hdel', KEYS[3], 'renewedAt')
redis.call('hset', KEYS[3], 'workerId', ARGV[1], 'token', ARGV[2], 'generation', generation, 'expiresAt', ARGV[4], 'acquiredAt', ARGV[4])
return generation`;
const renewLeaseScript = `
local raw = redis.call('get', KEYS[1]); if not raw then return 0 end
local owner = cjson.decode(raw)
if owner.workerId ~= ARGV[1] or owner.token ~= ARGV[2] or tonumber(owner.generation) ~= tonumber(ARGV[3]) then return 0 end
owner.expiresAt = ARGV[5]
redis.call('set', KEYS[1], cjson.encode(owner), 'PX', ARGV[4])
redis.call('hset', KEYS[2], 'expiresAt', ARGV[5], 'renewedAt', ARGV[5])
return 1`;
const releaseLeaseScript = `
local raw = redis.call('get', KEYS[1]); if not raw then return 0 end
local owner = cjson.decode(raw)
if owner.workerId == ARGV[1] and owner.token == ARGV[2] and tonumber(owner.generation) == tonumber(ARGV[3]) then
  redis.call('del', KEYS[1]); return 1
end
return 0`;
const actorInstances = new Map();
export const rebalanceMinimumWorkers = Number(
  process.env.REBALANCE_MIN_WORKERS || 2,
);

export const initialState = (type) =>
  type === 'CounterActor'
    ? { count: 0 }
    : type === 'BankAccountActor'
      ? { balance: 0 }
      : { jobs: [], ticks: 0 };
export const recordEvent = (type, message, details = {}) =>
  RuntimeEvent.create({ type, message, details }).catch((error) => {
    console.error('Runtime event could not be saved:', error.message);
    return null;
  });

export async function resetDemoData() {
  const [actors, messages, events, migrations] = await Promise.all([
    Actor.deleteMany({}),
    Message.deleteMany({}),
    RuntimeEvent.deleteMany({}),
    ActorMigration.deleteMany({}),
  ]);
  await redis.del([
    OWNER_KEY,
    LEASE_KEY,
    'actorlab:singleton:generation',
    'actorlab:singleton:status',
  ]);
  actorInstances.clear();
  return {
    actors: actors.deletedCount,
    messages: messages.deletedCount,
    events: events.deletedCount,
    migrations: migrations.deletedCount,
  };
}

export async function listWorkers() {
  const workers = await redis.hGetAll(WORKER_KEY);
  const now = Date.now();
  const live = [];
  for (const [workerId, raw] of Object.entries(workers)) {
    try {
      const worker = JSON.parse(raw);
      if (
        now - new Date(worker.lastHeartbeat).getTime() <=
        Number(process.env.WORKER_TIMEOUT_MS || 15000)
      ) {
        worker.status = worker.status || 'ACTIVE';
        if (worker.status === 'ACTIVE') live.push(worker);
      } else await redis.hDel(WORKER_KEY, workerId);
    } catch {
      await redis.hDel(WORKER_KEY, workerId);
    }
  }
  return live.sort((a, b) => a.workerId.localeCompare(b.workerId));
}

export async function registerWorker(worker) {
  if (
    !worker?.workerId ||
    (worker.mode === 'kubernetes' && worker.workerId !== worker.podName)
  )
    throw Object.assign(
      new Error('Kubernetes workerId must match its Pod name'),
      { status: 400 },
    );
  const existing = await redis.hGet(WORKER_KEY, worker.workerId);
  if (existing) {
    const previous = JSON.parse(existing);
    if (
      previous.podIP &&
      worker.podIP &&
      previous.podIP !== worker.podIP &&
      worker.mode === 'kubernetes'
    )
      throw Object.assign(
        new Error(
          'Worker identity is already registered by a different Pod IP',
        ),
        { status: 409 },
      );
  }
  const value = {
    status: 'ACTIVE',
    ...worker,
    lastHeartbeat: new Date().toISOString(),
  };
  await redis.hSet(WORKER_KEY, worker.workerId, JSON.stringify(value));
  await recordEvent(
    'worker.registered',
    `Worker ${worker.workerId} registered`,
    { workerId: worker.workerId },
  );
  setTimeout(() => {
    rebalance().catch((error) => {
      if (error.status !== 409)
        console.error('Join rebalance failed:', error.message);
    });
  }, 0).unref?.();
  return value;
}

export async function drainWorker(workerId) {
  const raw = await redis.hGet(WORKER_KEY, workerId);
  if (!raw) return { ok: true, status: 'TERMINATED' };
  const current = JSON.parse(raw);
  await redis.hSet(
    WORKER_KEY,
    workerId,
    JSON.stringify({
      ...current,
      status: 'DRAINING',
      lastHeartbeat: new Date().toISOString(),
    }),
  );
  setTimeout(() => {
    rebalance().catch((error) => {
      if (error.status !== 409)
        console.error('Drain rebalance failed:', error.message);
    });
  }, 0).unref?.();
  return { ok: true, status: 'DRAINING' };
}

export async function heartbeat(worker) {
  const raw = await redis.hGet(WORKER_KEY, worker.workerId);
  if (!raw) {
    const error = new Error('Worker registration expired');
    error.status = 404;
    throw error;
  }
  const previous = JSON.parse(raw);
  await redis.hSet(
    WORKER_KEY,
    worker.workerId,
    JSON.stringify({
      ...previous,
      ...worker,
      lastHeartbeat: new Date().toISOString(),
    }),
  );
  return { ok: true };
}

async function chooseWorker() {
  const workers = await listWorkers();
  if (!workers.length) {
    const error = new Error(
      'No live workers are registered. Start a worker process and retry.',
    );
    error.status = 503;
    throw error;
  }
  const counts = await Actor.aggregate([
    { $match: { workerId: { $in: workers.map((w) => w.workerId) } } },
    { $group: { _id: '$workerId', count: { $sum: 1 } } },
  ]);
  const countMap = new Map(counts.map((item) => [item._id, item.count]));
  const metrics = await getWorkerResourceMetrics();
  const pressure = (workerId) => {
    const metric = metrics.workers[workerId];
    if (!metric) return 0;
    return (
      (metric.cpuPercentOfRequest ?? metric.cpuMillicores / 10 ?? 0) +
      (metric.memoryPercentOfRequest ?? metric.memoryBytes / (1024 * 1024) ?? 0)
    );
  };
  return workers.sort(
    (a, b) =>
      (countMap.get(a.workerId) || 0) * 10 +
      pressure(a.workerId) -
      ((countMap.get(b.workerId) || 0) * 10 + pressure(b.workerId)),
  )[0];
}

export async function ensureActorOwner(actor) {
  const deadline =
    Date.now() + Number(process.env.WORKER_REQUEST_TIMEOUT_MS || 5000);
  let currentActor = await Actor.findOne({ actorId: actor.actorId });
  while (currentActor?.status === 'migrating' && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 40));
    currentActor = await Actor.findOne({ actorId: actor.actorId });
  }
  if (!currentActor) {
    const e = new Error('Actor not found');
    e.status = 404;
    throw e;
  }
  if (currentActor.status === 'migrating') {
    const e = new Error('Actor is still migrating; retry shortly');
    e.status = 503;
    throw e;
  }
  const workers = await listWorkers();
  const existingRaw = await redis.hGet(OWNER_KEY, actor.actorId);
  let existing;
  try {
    existing = existingRaw ? JSON.parse(existingRaw) : null;
  } catch {
    existing = null;
  }
  let owner;
  let singletonGeneration;
  if (currentActor.actorType === 'GlobalSchedulerActor') {
    const lease = await singletonStatus();
    owner = workers.find((worker) => worker.workerId === lease.owner);
    if (!owner || !lease.active) {
      const e = new Error('GlobalSchedulerActor has no active singleton lease');
      e.status = 503;
      throw e;
    }
    singletonGeneration = Number(lease.generation);
  } else {
    owner =
      workers.find((worker) => worker.workerId === existing?.workerId) ||
      workers.find((worker) => worker.workerId === currentActor.workerId) ||
      (await chooseWorker());
  }
  const generation =
    existing?.workerId === owner.workerId
      ? existing.generation
      : (existing?.generation || currentActor.ownerGeneration || 0) + 1;
  const assignment =
    existing?.workerId === owner.workerId
      ? existing
      : {
          workerId: owner.workerId,
          generation,
          changedAt: new Date().toISOString(),
        };
  if (
    currentActor.workerId !== assignment.workerId ||
    currentActor.ownerGeneration !== assignment.generation
  ) {
    currentActor.workerId = assignment.workerId;
    currentActor.ownerGeneration = assignment.generation;
    await currentActor.save();
  }
  if (existingRaw !== JSON.stringify(assignment)) {
    const swapped = await redis.eval(
      `local current = redis.call('hget', KEYS[1], ARGV[1]); if (current or '') == ARGV[2] then redis.call('hset', KEYS[1], ARGV[1], ARGV[3]); return 1 else return 0 end`,
      {
        keys: [OWNER_KEY],
        arguments: [
          actor.actorId,
          existingRaw || '',
          JSON.stringify(assignment),
        ],
      },
    );
    if (Number(swapped) !== 1) {
      const latest = await Actor.findOne({ actorId: actor.actorId });
      const latestRaw = await redis.hGet(OWNER_KEY, actor.actorId);
      if (latestRaw) {
        const mapped = JSON.parse(latestRaw);
        await Actor.updateOne(
          { actorId: actor.actorId, status: { $ne: 'migrating' } },
          {
            $set: {
              workerId: mapped.workerId,
              ownerGeneration: mapped.generation,
            },
          },
        );
      }
      return ensureActorOwner(latest || actor);
    }
  }
  return { worker: owner, assignment, singletonGeneration };
}

export async function createActor({ actorId, actorType }) {
  const actor = await Actor.create({
    actorId,
    actorType,
    state: initialState(actorType),
    workerId: null,
    ownerGeneration: 0,
  });
  await recordEvent('actor.created', `${actorType} ${actorId} created`, {
    actorId,
    workerId: null,
  });
  return actor;
}

async function persistMessage(actorId, messageId, operation, payload) {
  const lockKey = `actorlab:actor:${actorId}:enqueue`;
  const token = crypto.randomUUID();
  const deadline =
    Date.now() + Number(process.env.WORKER_REQUEST_TIMEOUT_MS || 5000);
  while (!(await redis.set(lockKey, token, { NX: true, PX: 10000 }))) {
    if (Date.now() >= deadline)
      throw Object.assign(new Error('Actor message queue is busy'), {
        status: 503,
      });
    await new Promise((resolve) => setTimeout(resolve, 15));
  }
  try {
    const existing = await Message.findOne({ messageId });
    if (existing) return existing;
    const actor = await Actor.findOneAndUpdate(
      { actorId },
      { $inc: { messageSequence: 1 } },
      { new: true },
    );
    if (!actor)
      throw Object.assign(new Error('Actor not found'), { status: 404 });
    try {
      return await Message.create({
        messageId,
        actorId,
        operation,
        payload,
        status: 'PENDING',
        sequence: actor.messageSequence,
        attempts: 0,
      });
    } catch (error) {
      if (error.code !== 11000) throw error;
      const duplicate = await Message.findOne({ messageId });
      if (duplicate) return duplicate;
      throw error;
    }
  } finally {
    await redis
      .eval(
        `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
        { keys: [lockKey], arguments: [token] },
      )
      .catch(() => {});
  }
}

export async function sendMessage(actorId, body) {
  if (
    !body ||
    typeof body !== 'object' ||
    typeof body.operation !== 'string' ||
    body.operation.length > 80
  ) {
    const e = new Error('A valid operation is required');
    e.status = 400;
    throw e;
  }
  if (
    body.payload !== undefined &&
    (!body.payload ||
      typeof body.payload !== 'object' ||
      Array.isArray(body.payload))
  ) {
    const e = new Error('payload must be an object');
    e.status = 400;
    throw e;
  }
  if (
    body.messageId !== undefined &&
    (typeof body.messageId !== 'string' || body.messageId.length > 120)
  ) {
    const e = new Error('messageId must be a string of at most 120 characters');
    e.status = 400;
    throw e;
  }
  let actor = await Actor.findOne({ actorId });
  if (!actor) {
    const e = new Error(`Actor ${actorId} was not found`);
    e.status = 404;
    throw e;
  }
  const messageId = String(body.messageId || crypto.randomUUID());
  const durableMessage = await persistMessage(
    actorId,
    messageId,
    body.operation,
    body.payload || {},
  );
  if (durableMessage.status === 'COMPLETED')
    return {
      actor: await Actor.findOne({ actorId }),
      result: durableMessage.result,
      messageId,
      duplicate: true,
    };
  if (durableMessage.status === 'FAILED')
    throw Object.assign(
      new Error(durableMessage.error || 'This message previously failed'),
      { status: 409 },
    );
  if (durableMessage.actorId !== actorId)
    throw Object.assign(
      new Error('messageId is already used by a different actor'),
      { status: 409 },
    );
  if (actor.status === 'migrating') {
    const deadline =
      Date.now() + Number(process.env.WORKER_REQUEST_TIMEOUT_MS || 5000);
    while (actor?.status === 'migrating' && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      actor = await Actor.findOne({ actorId });
    }
    if (actor?.status === 'migrating') {
      const e = new Error(
        'Message is durably queued; actor migration is still in progress',
      );
      e.status = 503;
      throw e;
    }
  }
  const { worker, assignment, singletonGeneration } =
    await ensureActorOwner(actor);
  const timeout = AbortSignal.timeout(
    Number(process.env.WORKER_REQUEST_TIMEOUT_MS || 5000),
  );
  let response;
  try {
    response = await fetch(
      `${worker.url}/internal/actors/${encodeURIComponent(actorId)}/message`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          ...body,
          payload: durableMessage.payload,
          messageId,
          sequence: durableMessage.sequence,
          generation: assignment.generation,
          singletonGeneration,
        }),
        signal: timeout,
      },
    );
  } catch (error) {
    const e = new Error(
      `Worker ${worker.workerId} is unreachable: ${error.message}`,
    );
    e.status = 503;
    throw e;
  }
  const result = await response.json();
  if (!response.ok) {
    const e = new Error(result.error || 'Worker rejected message');
    e.status = response.status;
    throw e;
  }
  return { ...result, messageId, duplicate: Boolean(result.duplicate) };
}

async function workerProcessLocked(workerId, actorId, body) {
  const ownershipRaw = await redis.hGet(OWNER_KEY, actorId);
  if (!ownershipRaw) {
    const e = new Error('Actor ownership is not registered');
    e.status = 409;
    throw e;
  }
  const ownership = JSON.parse(ownershipRaw);
  if (
    ownership.workerId !== workerId ||
    ownership.generation !== Number(body.generation)
  ) {
    const e = new Error('Stale worker ownership generation');
    e.status = 409;
    throw e;
  }
  const message = await Message.findOne({ messageId: body.messageId });
  if (!message || message.actorId !== actorId)
    throw Object.assign(new Error('Durable message is not available'), {
      status: 404,
    });
  if (message.status === 'COMPLETED')
    return {
      actor: await Actor.findOne({ actorId }),
      result: message.result,
      duplicate: true,
    };
  const earlier = await Message.findOne({
    actorId,
    sequence: { $lt: message.sequence },
    status: { $in: ['PENDING', 'PROCESSING'] },
  }).sort({ sequence: 1 });
  if (earlier)
    throw Object.assign(
      new Error('An earlier actor message is still pending'),
      { status: 409 },
    );
  await Message.updateOne(
    { _id: message._id, status: { $in: ['PENDING', 'PROCESSING'] } },
    {
      $set: { status: 'PROCESSING', lockedAt: new Date(), error: null },
      $inc: { attempts: 1 },
    },
  );
  const actor = await Actor.findOne({
    actorId,
    workerId,
    ownerGeneration: Number(body.generation),
  }).select('+processedMessages');
  if (!actor) {
    const e = new Error('Actor is not assigned to this worker');
    e.status = 409;
    throw e;
  }
  let instance = actorInstances.get(actorId);
  if (
    !instance ||
    instance.workerId !== workerId ||
    instance.generation !== Number(body.generation)
  ) {
    await Actor.updateOne(
      { _id: actor._id, ownerGeneration: Number(body.generation) },
      { $set: { status: 'activating' } },
    );
    instance = {
      actorId,
      actorType: actor.actorType,
      workerId,
      generation: Number(body.generation),
      state: structuredClone(actor.state),
      lastActivityAt: Date.now(),
    };
    actorInstances.set(actorId, instance);
    await Actor.updateOne(
      {
        _id: actor._id,
        workerId,
        ownerGeneration: Number(body.generation),
        status: 'activating',
      },
      { $set: { status: 'active' } },
    );
  }
  instance.state = structuredClone(actor.state);
  actor.state = structuredClone(instance.state);
  const processed = actor.processedMessages.find(
    (entry) => entry.messageId === body.messageId,
  );
  if (processed) {
    await Message.updateOne(
      { _id: message._id },
      {
        $set: {
          status: 'COMPLETED',
          result: processed.result,
          processedAt: new Date(),
          lockedAt: null,
        },
      },
    );
    return {
      actor: await Actor.findOne({ actorId }),
      result: processed.result,
      duplicate: true,
    };
  }
  if (actor.actorType === 'GlobalSchedulerActor') {
    const leaseRaw = await redis.get(LEASE_KEY);
    const leaseStatus = await redis.hGet(
      'actorlab:singleton:status',
      'generation',
    );
    let current;
    try {
      current = JSON.parse(leaseRaw);
    } catch {}
    if (
      current?.workerId !== workerId ||
      Number(leaseStatus) !== Number(body.singletonGeneration) ||
      Number(current?.generation) !== Number(body.singletonGeneration)
    ) {
      const e = new Error('Singleton lease fencing check failed');
      e.status = 409;
      throw e;
    }
  }
  const executionKey = `actorlab:actor:${actorId}:execution`;
  if ((await redis.get(executionKey)) !== body.executionLeaseToken)
    throw Object.assign(
      new Error('Actor execution lease expired before commit'),
      { status: 409 },
    );
  const { operation, payload = {} } = body;
  let result;
  if (actor.actorType === 'CounterActor') {
    if (operation === 'increment' || operation === 'decrement') {
      const amount = Number(payload.amount ?? 1);
      if (!Number.isFinite(amount) || amount <= 0) {
        const e = new Error('Counter amount must be greater than zero');
        e.status = 400;
        throw e;
      }
      actor.state.count += operation === 'increment' ? amount : -amount;
    } else if (operation !== 'get') {
      const e = new Error('CounterActor supports increment, decrement, get');
      e.status = 400;
      throw e;
    }
    result = { count: actor.state.count };
  } else if (actor.actorType === 'BankAccountActor') {
    if (operation === 'deposit') {
      const amount = Number(payload.amount);
      if (!Number.isFinite(amount) || amount <= 0) {
        const e = new Error('Deposit amount must be greater than zero');
        e.status = 400;
        throw e;
      }
      actor.state.balance += amount;
    } else if (operation === 'withdraw') {
      const amount = Number(payload.amount);
      if (!Number.isFinite(amount) || amount <= 0) {
        const e = new Error('Withdrawal amount must be greater than zero');
        e.status = 400;
        throw e;
      }
      if (actor.state.balance - amount < 0) {
        const e = new Error('Insufficient funds');
        e.status = 409;
        throw e;
      }
      actor.state.balance -= amount;
    } else if (operation !== 'getBalance') {
      const e = new Error(
        'BankAccountActor supports deposit, withdraw, getBalance',
      );
      e.status = 400;
      throw e;
    }
    result = { balance: actor.state.balance };
  } else {
    if (operation === 'tick') actor.state.ticks += 1;
    else if (operation === 'addJob')
      actor.state.jobs.push({
        id: crypto.randomUUID(),
        name: String(payload.name || 'untitled'),
        createdAt: new Date(),
      });
    else if (operation !== 'get') {
      const e = new Error('GlobalSchedulerActor supports tick, addJob, get');
      e.status = 400;
      throw e;
    }
    result = { jobs: actor.state.jobs, ticks: actor.state.ticks };
  }
  const oldVersion = actor.version;
  actor.version += 1;
  actor.lastActiveAt = new Date();
  actor.status = 'active';
  if (actor.actorType === 'GlobalSchedulerActor') {
    const currentLease = await redis.get(LEASE_KEY);
    let current;
    try {
      current = JSON.parse(currentLease);
    } catch {}
    if (
      current?.workerId !== workerId ||
      Number(current?.generation) !== Number(body.singletonGeneration)
    ) {
      throw Object.assign(
        new Error('Singleton fencing token expired before commit'),
        { status: 409 },
      );
    }
  }
  if ((await redis.get(executionKey)) !== body.executionLeaseToken)
    throw Object.assign(
      new Error('Actor execution lease expired before persistence'),
      { status: 409 },
    );
  const saved = await Actor.updateOne(
    {
      _id: actor._id,
      version: oldVersion,
      workerId,
      ownerGeneration: Number(body.generation),
      ...(actor.actorType === 'GlobalSchedulerActor'
        ? {
            $or: [
              {
                singletonGeneration: { $lte: Number(body.singletonGeneration) },
              },
              { singletonGeneration: { $exists: false } },
            ],
          }
        : {}),
      'processedMessages.messageId': { $ne: body.messageId },
    },
    {
      $set: {
        state: actor.state,
        version: actor.version,
        lastActiveAt: actor.lastActiveAt,
        status: actor.status,
        ...(actor.actorType === 'GlobalSchedulerActor'
          ? { singletonGeneration: Number(body.singletonGeneration) }
          : {}),
      },
      $push: {
        processedMessages: {
          $each: [{ messageId: body.messageId, result }],
          $slice: -256,
        },
      },
    },
  );
  if (!saved.modifiedCount) {
    const current = await Actor.findOne({ actorId }).select(
      '+processedMessages',
    );
    const duplicate = current?.processedMessages.find(
      (entry) => entry.messageId === body.messageId,
    );
    if (duplicate)
      return {
        actor: await Actor.findOne({ actorId }),
        result: duplicate.result,
        duplicate: true,
      };
    const e = new Error(
      'Actor version changed during processing; retry with the same messageId',
    );
    e.status = 409;
    throw e;
  }
  instance.state = structuredClone(actor.state);
  instance.lastActivityAt = Date.now();
  await Message.updateOne(
    { _id: message._id, status: 'PROCESSING' },
    {
      $set: {
        status: 'COMPLETED',
        result,
        processedAt: new Date(),
        lockedAt: null,
        error: null,
      },
    },
  );
  try {
    await redis.hSet(
      WORKER_KEY,
      workerId,
      JSON.stringify({
        ...(await getWorker(workerId)),
        activeActors: await Actor.countDocuments({
          workerId,
          status: 'active',
        }),
        lastHeartbeat: new Date().toISOString(),
      }),
    );
  } catch (error) {
    console.error(
      'Worker metric update failed after actor state commit:',
      error.message,
    );
  }
  return { actor: await Actor.findOne({ actorId }), result };
}

export async function workerProcess(workerId, actorId, body) {
  const key = `actorlab:actor:${actorId}:execution`;
  const token = crypto.randomUUID();
  const deadline =
    Date.now() + Number(process.env.WORKER_REQUEST_TIMEOUT_MS || 5000);
  while (Date.now() < deadline) {
    const acquired = await redis.set(key, token, { NX: true, PX: 30000 });
    if (acquired) {
      const renewal = setInterval(() => {
        redis
          .eval(
            `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end`,
            { keys: [key], arguments: [token, '30000'] },
          )
          .catch(() => {});
      }, 10000);
      renewal.unref?.();
      try {
        return await workerProcessLocked(workerId, actorId, {
          ...body,
          executionLeaseToken: token,
        });
      } catch (error) {
        if (error.status === 400 || error.message === 'Insufficient funds') {
          await Message.updateOne(
            { messageId: body.messageId, status: 'PROCESSING' },
            {
              $set: { status: 'FAILED', error: error.message, lockedAt: null },
            },
          ).catch(() => {});
        } else if (error.status === 409) {
          await Message.updateOne(
            { messageId: body.messageId, status: 'PROCESSING' },
            { $set: { status: 'PENDING', lockedAt: null } },
          ).catch(() => {});
        }
        throw error;
      } finally {
        clearInterval(renewal);
        await redis
          .eval(
            `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
            { keys: [key], arguments: [token] },
          )
          .catch(() => {});
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw Object.assign(
    new Error(
      'Actor is processing another message; retry with the same messageId',
    ),
    { status: 503 },
  );
}

export async function recoverPendingMessages(workerId) {
  const staleBefore = new Date(Date.now() - 30000);
  await Message.updateMany(
    { status: 'PROCESSING', lockedAt: { $lte: staleBefore } },
    { $set: { status: 'PENDING', lockedAt: null } },
  );
  const ownedActors = await Actor.find({
    workerId,
    status: { $ne: 'migrating' },
  })
    .select('actorId ownerGeneration actorType')
    .lean();
  await Promise.all(
    ownedActors.map(async (actor) => {
      const message = await Message.findOne({
        actorId: actor.actorId,
        status: 'PENDING',
      })
        .sort({ sequence: 1 })
        .lean();
      if (!message) return;
      try {
        let singletonGeneration;
        if (actor.actorType === 'GlobalSchedulerActor') {
          const lease = await singletonStatus();
          if (!lease.active || lease.owner !== workerId) return;
          singletonGeneration = Number(lease.generation);
        }
        await workerProcess(workerId, actor.actorId, {
          operation: message.operation,
          payload: message.payload || {},
          messageId: message.messageId,
          sequence: message.sequence,
          generation: actor.ownerGeneration,
          singletonGeneration,
        });
      } catch (error) {
        if (error.status !== 409 && error.status !== 503)
          console.error(
            `Pending message ${message.messageId} recovery failed:`,
            error.message,
          );
      }
    }),
  );
}

export async function deactivateIdleActors(workerId) {
  const idleMs = Number(process.env.ACTOR_IDLE_TIMEOUT_MS || 60000);
  const cutoff = Date.now() - idleMs;
  for (const [actorId, instance] of actorInstances) {
    if (instance.workerId !== workerId) continue;
    if (instance.lastActivityAt > cutoff) {
      const ownerRaw = await redis.hGet(OWNER_KEY, actorId);
      let owner;
      try {
        owner = ownerRaw ? JSON.parse(ownerRaw) : null;
      } catch {}
      if (
        !owner ||
        owner.workerId !== workerId ||
        Number(owner.generation) !== instance.generation
      )
        actorInstances.delete(actorId);
      continue;
    }
    const token = crypto.randomUUID();
    const key = `actorlab:actor:${actorId}:execution`;
    if (!(await redis.set(key, token, { NX: true, PX: 10000 }))) continue;
    try {
      const actor = await Actor.findOne({
        actorId,
        workerId,
        ownerGeneration: instance.generation,
        keepWarm: { $ne: true },
        status: { $in: ['active', 'idle'] },
        lastActiveAt: { $lte: new Date(cutoff) },
      });
      if (!actor) {
        const ownerRaw = await redis.hGet(OWNER_KEY, actorId);
        let owner;
        try {
          owner = ownerRaw ? JSON.parse(ownerRaw) : null;
        } catch {}
        if (
          !owner ||
          owner.workerId !== workerId ||
          Number(owner.generation) !== instance.generation
        )
          actorInstances.delete(actorId);
        continue;
      }
      if (actor.status === 'active') {
        await Actor.updateOne(
          { _id: actor._id, status: 'active' },
          { $set: { status: 'idle' } },
        );
        continue;
      }
      await Actor.updateOne(
        { _id: actor._id, status: 'idle' },
        { $set: { status: 'deactivating' } },
      );
      // State is persisted after every acknowledged message; this final write
      // makes the deactivation boundary explicit before dropping the instance.
      await Actor.updateOne(
        {
          _id: actor._id,
          status: 'deactivating',
          ownerGeneration: instance.generation,
        },
        { $set: { state: instance.state, status: 'uninitialized' } },
      );
      actorInstances.delete(actorId);
    } finally {
      await redis
        .eval(
          `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
          { keys: [key], arguments: [token] },
        )
        .catch(() => {});
    }
  }
}

async function getWorker(workerId) {
  const raw = await redis.hGet(WORKER_KEY, workerId);
  return raw ? JSON.parse(raw) : { workerId };
}

export async function dashboardData() {
  const [
    totalActors,
    activeActors,
    totalMessages,
    workerResult,
    events,
    actorCounts,
  ] = await Promise.all([
    Actor.countDocuments(),
    Actor.countDocuments({ status: 'active' }),
    Message.countDocuments(),
    listWorkers()
      .then((workers) => ({ workers, available: true }))
      .catch(() => ({ workers: [], available: false })),
    RuntimeEvent.find().sort({ createdAt: -1 }).limit(8).lean(),
    Actor.aggregate([{ $group: { _id: '$workerId', count: { $sum: 1 } } }]),
  ]);
  return {
    totalActors,
    activeActors,
    totalMessages,
    workers: workerResult.workers,
    coordinationAvailable: workerResult.available,
    events,
    actorCounts,
  };
}

async function rebalanceUnlocked() {
  const workers = await listWorkers();
  if (workers.length < rebalanceMinimumWorkers) {
    const e = new Error(
      `At least ${rebalanceMinimumWorkers} live workers are required to rebalance`,
    );
    e.status = 409;
    throw e;
  }
  const liveIds = new Set(workers.map((worker) => worker.workerId));
  const resourceMetrics = await getWorkerResourceMetrics();
  const pressure = (workerId) => {
    const metric = resourceMetrics.workers[workerId];
    if (!metric) return 0;
    return (
      (metric.cpuPercentOfRequest ?? metric.cpuMillicores / 10 ?? 0) +
      (metric.memoryPercentOfRequest ?? metric.memoryBytes / (1024 * 1024) ?? 0)
    );
  };
  const orphanedRecords = await ActorMigration.find({ status: 'MIGRATING' });
  for (const migration of orphanedRecords) {
    const current = await Actor.findOne({ actorId: migration.actorId }).select(
      'workerId ownerGeneration status',
    );
    if (
      current &&
      current.status !== 'migrating' &&
      current.ownerGeneration >= migration.toGeneration
    ) {
      await ActorMigration.updateOne(
        { _id: migration._id, status: 'MIGRATING' },
        { $set: { status: 'RECOVERED', completedAt: new Date() } },
      );
    }
  }
  const interrupted = await Actor.find({ status: 'migrating' });
  for (const actor of interrupted) {
    const previousOwnerId = actor.workerId;
    const ownerRaw = await redis.hGet(OWNER_KEY, actor.actorId);
    const owner = ownerRaw ? JSON.parse(ownerRaw) : null;
    let repairedOwner = owner && liveIds.has(owner.workerId) ? owner : null;
    if (!repairedOwner) {
      const destination = await chooseWorker();
      const replacement = {
        workerId: destination.workerId,
        generation:
          Math.max(Number(owner?.generation || 0), actor.ownerGeneration || 0) +
          1,
        changedAt: new Date().toISOString(),
      };
      const cas = await redis.eval(
        `local current = redis.call('hget', KEYS[1], ARGV[1]); if (current or '') == ARGV[2] then redis.call('hset', KEYS[1], ARGV[1], ARGV[3]); return 1 else return 0 end`,
        {
          keys: [OWNER_KEY],
          arguments: [
            actor.actorId,
            ownerRaw || '',
            JSON.stringify(replacement),
          ],
        },
      );
      if (Number(cas) === 1) repairedOwner = replacement;
    }
    if (repairedOwner && liveIds.has(repairedOwner.workerId)) {
      await Actor.updateOne(
        { _id: actor._id, status: 'migrating' },
        {
          $set: {
            workerId: repairedOwner.workerId,
            ownerGeneration: repairedOwner.generation,
            status: 'uninitialized',
          },
        },
      );
      await ActorMigration.updateMany(
        { actorId: actor.actorId, status: 'MIGRATING' },
        { $set: { status: 'RECOVERED', completedAt: new Date() } },
      );
      const sourceWorker = workers.find(
        (worker) => worker.workerId === previousOwnerId,
      );
      if (sourceWorker)
        await fetch(
          `${sourceWorker.url}/internal/actors/${encodeURIComponent(actor.actorId)}/resume`,
          {
            method: 'POST',
            signal: AbortSignal.timeout(2000),
          },
        ).catch(() => {});
    }
  }
  const actors = await Actor.find({
    workerId: { $ne: null },
    status: { $ne: 'migrating' },
    actorType: { $ne: 'GlobalSchedulerActor' },
  }).sort({ createdAt: 1 });
  actors.sort(
    (a, b) => Number(liveIds.has(a.workerId)) - Number(liveIds.has(b.workerId)),
  );
  const counts = new Map(workers.map((w) => [w.workerId, 0]));
  for (const actor of actors)
    if (liveIds.has(actor.workerId))
      counts.set(actor.workerId, counts.get(actor.workerId) + 1);
  let moved = 0;
  for (const actor of actors) {
    // Actor count is the balancing invariant. Resource pressure is only a
    // tie-breaker: adding it to the count score can make a CPU-busy worker
    // with zero actors look like the donor and prevent any actor from moving.
    const donor = [...counts.entries()].sort(
      (a, b) => b[1] - a[1] || pressure(b[0]) - pressure(a[0]),
    )[0];
    const receiver = [...counts.entries()].sort(
      (a, b) => a[1] - b[1] || pressure(a[0]) - pressure(b[0]),
    )[0];
    const failedWorker = !liveIds.has(actor.workerId);
    if (!donor || !receiver || (!failedWorker && donor[0] === receiver[0]))
      break;
    if (
      !failedWorker && donor[1] - receiver[1] <= 1
    )
      break;
    if (!failedWorker && actor.workerId !== donor[0]) continue;
    const current = await redis.hGet(OWNER_KEY, actor.actorId);
    const oldAssignment = current
      ? JSON.parse(current)
      : { generation: actor.ownerGeneration };
    const oldWorker = workers.find((w) => w.workerId === actor.workerId);
    const sourceWorkerId = actor.workerId;
    const fromGeneration = Number(
      oldAssignment.generation || actor.ownerGeneration || 0,
    );
    actor.status = 'migrating';
    await actor.save();
    const migration = await ActorMigration.create({
      actorId: actor.actorId,
      sourceWorkerId,
      destinationWorkerId: receiver[0],
      fromGeneration,
      toGeneration: fromGeneration + 1,
      status: 'MIGRATING',
    });
    if (oldWorker) {
      try {
        const drainResponse = await fetch(
          `${oldWorker.url}/internal/actors/${encodeURIComponent(actor.actorId)}/drain`,
          { method: 'POST', signal: AbortSignal.timeout(3000) },
        );
        if (!drainResponse.ok)
          throw new Error(
            `Source worker drain returned ${drainResponse.status}`,
          );
      } catch (error) {
        actor.status = 'uninitialized';
        await actor.save();
        await ActorMigration.updateOne(
          { _id: migration._id },
          {
            $set: {
              status: 'FAILED',
              error: error.message,
              completedAt: new Date(),
            },
          },
        );
        continue;
      }
    }
    const next = {
      workerId: receiver[0],
      generation: fromGeneration + 1,
      changedAt: new Date().toISOString(),
    };
    let swapped;
    try {
      swapped = await redis.eval(
        `local current = redis.call('hget', KEYS[1], ARGV[1]); if (current or '') == ARGV[2] then redis.call('hset', KEYS[1], ARGV[1], ARGV[3]); return 1 else return 0 end`,
        {
          keys: [OWNER_KEY],
          arguments: [actor.actorId, current || '', JSON.stringify(next)],
        },
      );
    } catch (error) {
      actor.status = 'idle';
      await actor.save();
      await ActorMigration.updateOne(
        { _id: migration._id },
        {
          $set: {
            status: 'FAILED',
            error: error.message,
            completedAt: new Date(),
          },
        },
      );
      if (oldWorker)
        await fetch(
          `${oldWorker.url}/internal/actors/${encodeURIComponent(actor.actorId)}/resume`,
          { method: 'POST', signal: AbortSignal.timeout(2000) },
        ).catch(() => {});
      throw error;
    }
    if (Number(swapped) !== 1) {
      const latestRaw = await redis.hGet(OWNER_KEY, actor.actorId);
      if (latestRaw) {
        const latest = JSON.parse(latestRaw);
        actor.workerId = latest.workerId;
        actor.ownerGeneration = latest.generation;
      }
      actor.status = 'idle';
      await actor.save();
      await ActorMigration.updateOne(
        { _id: migration._id },
        {
          $set: {
            status: 'FAILED',
            error: 'Ownership compare-and-set failed',
            completedAt: new Date(),
          },
        },
      );
      if (oldWorker)
        await fetch(
          `${oldWorker.url}/internal/actors/${encodeURIComponent(actor.actorId)}/resume`,
          { method: 'POST', signal: AbortSignal.timeout(2000) },
        ).catch(() => {});
      continue;
    }
    actor.workerId = receiver[0];
    actor.ownerGeneration = next.generation;
    actor.status = 'uninitialized';
    await actor.save();
    await ActorMigration.updateOne(
      { _id: migration._id },
      { $set: { status: 'COMPLETED', completedAt: new Date() } },
    );
    if (oldWorker)
      await fetch(
        `${oldWorker.url}/internal/actors/${encodeURIComponent(actor.actorId)}/resume`,
        { method: 'POST', signal: AbortSignal.timeout(2000) },
      ).catch(() => {});
    if (!failedWorker) counts.set(donor[0], counts.get(donor[0]) - 1);
    counts.set(receiver[0], counts.get(receiver[0]) + 1);
    moved += 1;
  }
  await recordEvent(
    'actors.rebalanced',
    `Rebalancing moved ${moved} actor(s)`,
    { moved, distribution: Object.fromEntries(counts) },
  );
  return { moved, distribution: Object.fromEntries(counts) };
}

export async function rebalance() {
  const token = crypto.randomUUID();
  const lockKey = 'actorlab:coordinator:rebalance';
  const locked = await redis.set(lockKey, token, { NX: true, PX: 120000 });
  if (!locked)
    throw Object.assign(new Error('A rebalance is already in progress'), {
      status: 409,
    });
  const renewal = setInterval(() => {
    redis
      .eval(
        `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end`,
        { keys: [lockKey], arguments: [token, '120000'] },
      )
      .catch(() => {});
  }, 30000);
  renewal.unref?.();
  try {
    return await rebalanceUnlocked();
  } finally {
    clearInterval(renewal);
    await redis
      .eval(
        `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
        { keys: [lockKey], arguments: [token] },
      )
      .catch(() => {});
  }
}

export async function acquireSingleton(workerId) {
  const ttl = Number(process.env.SINGLETON_LEASE_MS || 12000);
  const token = crypto.randomUUID();
  const expiresAt = new Date(Date.now() + ttl).toISOString();
  const scheduler = await Actor.findOne({ actorType: 'GlobalSchedulerActor' })
    .select('singletonGeneration')
    .lean();
  const generation = Number(
    await redis.eval(acquireLeaseScript, {
      keys: [
        LEASE_KEY,
        'actorlab:singleton:generation',
        'actorlab:singleton:status',
      ],
      arguments: [
        workerId,
        token,
        String(ttl),
        expiresAt,
        String(scheduler?.singletonGeneration || 0),
      ],
    }),
  );
  if (generation > 0) {
    await recordEvent(
      'singleton.acquired',
      `GlobalSchedulerActor lease acquired by ${workerId}`,
      { workerId, generation },
    );
    return {
      workerId,
      token,
      generation,
      expiresAt: new Date(Date.now() + ttl).toISOString(),
    };
  }
  return null;
}

export async function renewSingleton(lease) {
  const ttl = Number(process.env.SINGLETON_LEASE_MS || 12000);
  const expiresAt = new Date(Date.now() + ttl).toISOString();
  const renewed = await redis.eval(renewLeaseScript, {
    keys: [LEASE_KEY, 'actorlab:singleton:status'],
    arguments: [
      lease.workerId,
      lease.token,
      String(lease.generation),
      String(ttl),
      expiresAt,
    ],
  });
  if (Number(renewed)) {
    return { ...lease, expiresAt };
  }
  return null;
}

export async function singletonStatus() {
  try {
    const state = await redis.hGetAll('actorlab:singleton:status');
    const key = await redis.get(LEASE_KEY);
    if (!key)
      return {
        registered: true,
        owner: null,
        active: false,
        expiresAt: state.expiresAt || null,
        generation: state.generation || null,
      };
    let owner = {};
    try {
      owner = JSON.parse(key);
    } catch {}
    return {
      registered: true,
      owner: owner.workerId,
      active: true,
      expiresAt: state.expiresAt,
      generation: owner.generation || state.generation,
      renewedAt: state.renewedAt || null,
      renewalStatus: 'ACTIVE',
    };
  } catch {
    return {
      registered: false,
      owner: null,
      active: false,
      expiresAt: null,
      generation: null,
      unavailable: true,
    };
  }
}

export async function allRuntimeEvents(limit = 30) {
  return RuntimeEvent.find().sort({ createdAt: -1 }).limit(limit).lean();
}
