import mongoose from 'mongoose';

const ActorMigrationSchema = new mongoose.Schema(
  {
    actorId: { type: String, required: true, index: true },
    sourceWorkerId: { type: String, default: null },
    destinationWorkerId: { type: String, required: true },
    fromGeneration: { type: Number, required: true },
    toGeneration: { type: Number, required: true },
    status: {
      type: String,
      enum: ['MIGRATING', 'COMPLETED', 'FAILED', 'RECOVERED'],
      default: 'MIGRATING',
      index: true,
    },
    error: String,
    startedAt: { type: Date, default: Date.now },
    completedAt: Date,
  },
  { timestamps: true },
);

export default mongoose.models.ActorMigration ||
  mongoose.model('ActorMigration', ActorMigrationSchema);
