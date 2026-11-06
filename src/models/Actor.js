import mongoose from 'mongoose';
const ActorSchema = new mongoose.Schema(
  {
    actorId: {
      type: String,
      required: true,
      unique: true,
      trim: true,
      maxlength: 120,
    },
    actorType: {
      type: String,
      required: true,
      enum: ['CounterActor', 'BankAccountActor', 'GlobalSchedulerActor'],
    },
    state: { type: mongoose.Schema.Types.Mixed, required: true },
    processedMessages: {
      type: [{ messageId: String, result: mongoose.Schema.Types.Mixed }],
      default: [],
      select: false,
    },
    status: {
      type: String,
      enum: [
        'uninitialized',
        'activating',
        'active',
        'idle',
        'deactivating',
        'migrating',
        'recovering',
      ],
      default: 'uninitialized',
    },
    workerId: { type: String, default: null },
    ownerGeneration: { type: Number, default: 0 },
    singletonGeneration: { type: Number, default: 0 },
    messageSequence: { type: Number, default: 0 },
    version: { type: Number, default: 0 },
    lastActiveAt: { type: Date, default: Date.now },
  },
  { timestamps: true },
);
ActorSchema.index({ workerId: 1, status: 1 });
export default mongoose.models.Actor || mongoose.model('Actor', ActorSchema);
