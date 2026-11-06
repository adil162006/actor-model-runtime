import mongoose from 'mongoose';

let errorListenerBound = false;
export async function connectDatabase() {
  mongoose.set('strictQuery', true);
  mongoose.set('bufferCommands', false);
  if (!errorListenerBound) {
    mongoose.connection.on('error', (error) =>
      console.error('MongoDB error:', error.message),
    );
    errorListenerBound = true;
  }
  await mongoose.connect(
    process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/actorlab',
    { serverSelectionTimeoutMS: 5000 },
  );
  console.log('Connected to MongoDB');
}
