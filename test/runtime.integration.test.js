import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import mongoose from 'mongoose';
import Actor from '../src/models/Actor.js';
import Message from '../src/models/Message.js';

const base = (process.env.ACTORLAB_API_URL || '').replace(/\/$/, '');
const enabled = Boolean(base);

async function request(path, options) {
  const response = await fetch(`${base}${path}`, options);
  const payload = await response.json();
  if (!response.ok)
    throw Object.assign(new Error(payload.error || response.statusText), {
      status: response.status,
    });
  return payload;
}

test(
  'durable virtual actor messages activate, sequence, persist, and deduplicate',
  { skip: !enabled },
  async () => {
    const actorId = `integration-${crypto.randomUUID()}`;
    await request('/api/actors', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ actorId, actorType: 'CounterActor' }),
    });

    test(
      'a stale singleton fencing generation cannot commit actor state',
      {
        skip:
          !enabled ||
          !process.env.ACTORLAB_MONGODB_URI ||
          !process.env.ACTORLAB_WORKER_URL,
      },
      async () => {
        const actorId = 'global-scheduler';
        try {
          await request('/api/actors', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              actorId,
              actorType: 'GlobalSchedulerActor',
            }),
          });
        } catch {}
        const existing = await request(`/api/actors/${actorId}`);
        await request(`/api/actors/${actorId}/messages`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            operation: 'get',
            payload: {},
            messageId: `singleton-baseline-${crypto.randomUUID()}`,
          }),
        });
        const baseline = await request(`/api/actors/${actorId}`);
        const singleton = await request('/api/singletons');
        assert.equal(singleton.active, true);
        assert.ok(singleton.generation > 0);
        const competing = await Promise.all([
          request('/internal/singletons/acquire', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              workerId: `competing-${crypto.randomUUID()}`,
            }),
          }),
          request('/internal/singletons/acquire', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              workerId: `competing-${crypto.randomUUID()}`,
            }),
          }),
        ]);
        assert.ok(competing.every((result) => result.lease === null));
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const renewed = await request('/api/singletons');
        assert.equal(renewed.generation, singleton.generation);
        assert.ok(
          Date.parse(renewed.expiresAt) > Date.parse(singleton.expiresAt),
        );
        await mongoose.connect(process.env.ACTORLAB_MONGODB_URI);
        const actor = await Actor.findOneAndUpdate(
          { actorId },
          { $inc: { messageSequence: 1 } },
          { new: true },
        );
        const messageId = `stale-singleton-${crypto.randomUUID()}`;
        await Message.create({
          actorId,
          messageId,
          operation: 'tick',
          payload: {},
          status: 'PROCESSING',
          lockedAt: new Date(),
          sequence: actor.messageSequence,
        });
        const response = await fetch(
          `${process.env.ACTORLAB_WORKER_URL}/internal/actors/${actorId}/message`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              operation: 'tick',
              payload: {},
              messageId,
              sequence: actor.messageSequence,
              generation: existing.ownerGeneration,
              singletonGeneration: renewed.generation - 1,
            }),
          },
        );
        assert.equal(response.status, 409);
        const after = await Actor.findOne({ actorId });
        assert.equal(after.state.ticks, baseline.state.ticks);
        await Message.updateOne(
          { messageId },
          { $set: { status: 'PENDING', lockedAt: null } },
        );
        await mongoose.disconnect();
      },
    );

    const firstId = `${actorId}-first`;
    const first = await request(`/api/actors/${actorId}/messages`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        operation: 'increment',
        payload: { amount: 2 },
        messageId: firstId,
      }),
    });
    assert.equal(first.actor.state.count, 2);
    assert.ok(first.actor.workerId);
    assert.equal(first.actor.status, 'active');

    const duplicate = await request(`/api/actors/${actorId}/messages`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        operation: 'increment',
        payload: { amount: 2 },
        messageId: firstId,
      }),
    });
    assert.equal(duplicate.duplicate, true);
    assert.equal(duplicate.actor.state.count, 2);

    const ids = Array.from(
      { length: 8 },
      () => `${actorId}-${crypto.randomUUID()}`,
    );
    const send = async (messageId) => {
      for (let attempt = 0; attempt < 20; attempt += 1) {
        try {
          return await request(`/api/actors/${actorId}/messages`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              operation: 'increment',
              payload: { amount: 1 },
              messageId,
            }),
          });
        } catch (error) {
          if (![409, 503].includes(error.status)) throw error;
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
      throw new Error(`message ${messageId} did not complete after retries`);
    };
    await Promise.all(ids.map(send));

    const actor = await request(`/api/actors/${actorId}`);
    assert.equal(actor.state.count, 10);
    const messages = await request(`/api/actors/${actorId}/messages`);
    const accepted = messages.filter((message) =>
      [firstId, ...ids].includes(message.messageId),
    );
    assert.equal(accepted.length, 9);
    assert.ok(accepted.every((message) => message.status === 'COMPLETED'));
    assert.equal(new Set(accepted.map((message) => message.sequence)).size, 9);
  },
);

test(
  'idle virtual actors deactivate and reactivate from persisted state',
  { skip: !enabled, timeout: 90000 },
  async () => {
    const actorId = `idle-${crypto.randomUUID()}`;
    await request('/api/actors', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ actorId, actorType: 'CounterActor' }),
    });
    await request(`/api/actors/${actorId}/messages`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        operation: 'increment',
        payload: { amount: 7 },
        messageId: `${actorId}-before-idle`,
      }),
    });
    await new Promise((resolve) =>
      setTimeout(resolve, Number(process.env.ACTOR_IDLE_TEST_WAIT_MS || 85000)),
    );
    const idle = await request(`/api/actors/${actorId}`);
    assert.equal(idle.status, 'uninitialized');
    assert.equal(idle.state.count, 7);
    const reactivated = await request(`/api/actors/${actorId}/messages`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        operation: 'increment',
        payload: { amount: 1 },
        messageId: `${actorId}-after-idle`,
      }),
    });
    assert.equal(reactivated.actor.status, 'active');
    assert.equal(reactivated.actor.state.count, 8);
  },
);
