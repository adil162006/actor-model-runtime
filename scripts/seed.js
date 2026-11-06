import 'dotenv/config';
import mongoose from 'mongoose';
import { connectDatabase } from '../src/config/database.js';
import Actor from '../src/models/Actor.js';

const actors = Array.from({ length: 200 }, (_, index) => {
  const actorId = `actor-${index + 3}`;

  return {
    updateOne: {
      filter: { actorId },
      update: {
        $setOnInsert: {
          actorId,
          actorType: 'CounterActor',
          state: { count: 0 },
          status: 'uninitialized',
          workerId: null,
          ownerGeneration: 0,
          messageSequence: 0,
          version: 0,
          lastActiveAt: new Date(),
        },
      },
      upsert: true,
    },
  };
});

try {
  await connectDatabase();
  const result = await Actor.bulkWrite(actors);
  console.log(
    `Seeded ${result.upsertedCount} actors; actor-3 through actor-202 are available.`,
  );
} catch (error) {
  console.error('Actor seeding failed:', error.message);
  process.exitCode = 1;
} finally {
  await mongoose.disconnect();
}
