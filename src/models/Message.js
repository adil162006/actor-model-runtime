import mongoose from 'mongoose';
const MessageSchema = new mongoose.Schema(
  {
    messageId: { type: String, required: true, unique: true },
    actorId: { type: String, required: true, index: true },
    operation: String,
    payload: mongoose.Schema.Types.Mixed,
    result: mongoose.Schema.Types.Mixed,
    status: {
      type: String,
      enum: ['PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'],
      default: 'PENDING',
      index: true,
    },
    sequence: { type: Number, required: true },
    attempts: { type: Number, default: 0 },
    lockedAt: { type: Date, default: null },
    error: String,
    processedAt: { type: Date, default: null },
  },
  { timestamps: true },
);
MessageSchema.index(
  { actorId: 1, sequence: 1 },
  { unique: true, partialFilterExpression: { sequence: { $type: 'number' } } },
);
MessageSchema.index({ actorId: 1, status: 1, sequence: 1 });
export default mongoose.models.Message ||
  mongoose.model('Message', MessageSchema);
