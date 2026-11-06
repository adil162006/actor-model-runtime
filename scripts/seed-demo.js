import 'dotenv/config';
import mongoose from 'mongoose';
import { connectDatabase } from '../src/config/database.js';
import { connectRedis, redis } from '../src/config/redis.js';
import { resetDemoData, initialState } from '../src/services/runtime.js';
import Actor from '../src/models/Actor.js';

const demoActors = [
  ['counter-orders', 'CounterActor'],
  ['counter-inventory', 'CounterActor'],
  ['counter-payments', 'CounterActor'],
  ['bank-primary', 'BankAccountActor'],
  ['bank-savings', 'BankAccountActor'],
  ['global-scheduler', 'GlobalSchedulerActor'],
];

try {
  await connectDatabase();
  await connectRedis();
  const cleared = await resetDemoData();
  await Actor.insertMany(
    demoActors.map(([actorId, actorType]) => ({
      actorId,
      actorType,
      state: initialState(actorType),
      status: 'uninitialized',
      workerId: null,
      ownerGeneration: 0,
      singletonGeneration: 0,
      messageSequence: 0,
      version: 0,
      lastActiveAt: new Date(),
    })),
  );
  console.log(
    `Demo dataset ready: ${demoActors.length} actors loaded after clearing ${cleared.actors} existing actors.`,
  );
} catch (error) {
  console.error('Demo seeding failed:', error.message);
  process.exitCode = 1;
} finally {
  if (redis.isOpen) await redis.quit();
  await mongoose.disconnect();
}
