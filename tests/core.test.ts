import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { StreamCast, type StreamCastMessage } from '../src/core/index.js';
import { startTestRedis, type EphemeralRedis } from './helpers/redis-harness.js';

function waitFor(predicate: () => boolean, timeoutMs = 4_000): Promise<void> {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() - start > timeoutMs) {
        reject(new Error('Timed out waiting for condition'));
        return;
      }
      setTimeout(check, 15);
    };
    check();
  });
}

describe('StreamCast Core multi-node & reliability tests', () => {
  let redisEnv: EphemeralRedis;

  beforeAll(async () => {
    redisEnv = await startTestRedis();
  });

  afterAll(async () => {
    await redisEnv.stop();
  });

  it('delivers broadcasts across 3 independent nodes exactly once and supports except + presence', async () => {
    const prefix = `test:${randomUUID().slice(0, 8)}`;

    const nodeA = new StreamCast({
      redis: redisEnv.url,
      prefix,
      nodeId: 'node-A',
      readBlockMs: 200,
    });
    const nodeB = new StreamCast({
      redis: redisEnv.url,
      prefix,
      nodeId: 'node-B',
      readBlockMs: 200,
    });
    const nodeC = new StreamCast({
      redis: redisEnv.url,
      prefix,
      nodeId: 'node-C',
      readBlockMs: 200,
    });

    await Promise.all([nodeA.ready(), nodeB.ready(), nodeC.ready()]);

    const receivedA: StreamCastMessage[] = [];
    const receivedB: StreamCastMessage[] = [];
    const receivedC: StreamCastMessage[] = [];

    nodeA.registerSession('sess-a', { send: (m) => receivedA.push(m) });
    nodeB.registerSession('sess-b', { send: (m) => receivedB.push(m) });
    nodeC.registerSession('sess-c', { send: (m) => receivedC.push(m) });

    await nodeA.join('sess-a', 'alpha');
    await nodeB.join('sess-b', 'alpha');
    await nodeC.join('sess-c', 'alpha');

    expect(await nodeA.roomSize('alpha')).toBe(3);
    expect((await nodeB.listMembers('alpha')).sort()).toEqual(['sess-a', 'sess-b', 'sess-c']);

    await nodeC.broadcast('alpha', { hello: 'cluster' });

    await waitFor(() => receivedA.length === 1 && receivedB.length === 1 && receivedC.length === 1);

    // Wait briefly to assert NO duplicate delivery happens on nodeC (origin) or A/B
    await new Promise((r) => setTimeout(r, 150));
    expect(receivedA).toHaveLength(1);
    expect(receivedB).toHaveLength(1);
    expect(receivedC).toHaveLength(1);
    expect(receivedA[0]?.payload).toEqual({ hello: 'cluster' });
    expect(receivedB[0]?.payload).toEqual({ hello: 'cluster' });

    await nodeA.broadcast('alpha', { note: 'secret' }, { except: 'sess-b' });

    await waitFor(() => receivedA.length === 2 && receivedC.length === 2);
    await new Promise((r) => setTimeout(r, 120));
    expect(receivedB).toHaveLength(1); // sess-b excluded!

    await Promise.all([nodeA.close(), nodeB.close(), nodeC.close()]);
  });

  it('enforces backpressure skip and close policies based on bufferedAmount', async () => {
    const prefix = `test:${randomUUID().slice(0, 8)}`;

    const nodeSkip = new StreamCast({
      redis: redisEnv.url,
      prefix,
      backpressure: { maxBufferedBytes: 512, strategy: 'skip' },
    });

    const delivered: StreamCastMessage[] = [];
    let simulatedBuffer = 1024; // over limit

    nodeSkip.registerSession('slow-1', {
      send: (m) => delivered.push(m),
      getBufferedAmount: () => simulatedBuffer,
    });
    await nodeSkip.join('slow-1', 'feed');

    await nodeSkip.broadcast('feed', { seq: 1 });
    expect(delivered).toHaveLength(0);
    expect(nodeSkip.getMetrics().backpressureSkipped).toBe(1);

    simulatedBuffer = 100; // recovered below threshold
    await nodeSkip.broadcast('feed', { seq: 2 });
    expect(delivered).toHaveLength(1);
    await nodeSkip.close();

    // Now test 'close' strategy
    const nodeClose = new StreamCast({
      redis: redisEnv.url,
      prefix: `${prefix}:close`,
      backpressure: { maxBufferedBytes: 256, strategy: 'close', closeCode: 1008 },
    });

    let closedWithCode: number | undefined;
    nodeClose.registerSession('slow-2', {
      send: () => undefined,
      getBufferedAmount: () => 2048,
      close: (code) => {
        closedWithCode = code;
      },
    });
    await nodeClose.join('slow-2', 'feed');
    await nodeClose.broadcast('feed', { seq: 1 });

    expect(closedWithCode).toBe(1008);
    expect(nodeClose.getMetrics().backpressureClosed).toBe(1);
    await nodeClose.close();
  });

  it('cleans up dead nodes consumer groups and stale presence after heartbeat TTL expires', async () => {
    const prefix = `test:${randomUUID().slice(0, 8)}`;

    const survivor = new StreamCast({
      redis: redisEnv.url,
      prefix,
      nodeId: 'survivor',
      heartbeatIntervalMs: 100,
      heartbeatTtlMs: 350,
      cleanupIntervalMs: 10_000, // trigger manually for deterministic assertion
      readBlockMs: 150,
    });

    const crashing = new StreamCast({
      redis: redisEnv.url,
      prefix,
      nodeId: 'crashing',
      heartbeatIntervalMs: 10_000, // don't refresh so TTL expires quickly
      heartbeatTtlMs: 250,
      cleanupIntervalMs: 10_000,
      readBlockMs: 150,
    });

    await Promise.all([survivor.ready(), crashing.ready()]);

    survivor.registerSession('s-live', { send: () => undefined });
    crashing.registerSession('s-dead', { send: () => undefined });

    await survivor.join('s-live', 'lobby');
    await crashing.join('s-dead', 'lobby');
    expect(await survivor.roomSize('lobby')).toBe(2);

    // Simulate ungraceful crash by force-disconnecting crashing's sockets without calling close()
    crashing.getBlockingRedis().disconnect();
    crashing.getCommandRedis().disconnect();

    // Wait for crashing node's 250ms heartbeat TTL to expire
    await new Promise((r) => setTimeout(r, 400));

    // Presence ignores the dead node as soon as its heartbeat expires, before the sweep
    expect(await survivor.roomSize('lobby')).toBe(1);
    expect(await survivor.listMembers('lobby')).toEqual(['s-live']);

    const cleaned = await survivor.cleanupDeadNodes();
    expect(cleaned).toContain('crashing');

    const rawGroups = (await survivor
      .getCommandRedis()
      .call('XINFO', 'GROUPS', survivor.streamKey)) as unknown[];
    expect(rawGroups).toHaveLength(1); // only `sc:node:survivor` remains!

    await survivor.close();
  });
});
