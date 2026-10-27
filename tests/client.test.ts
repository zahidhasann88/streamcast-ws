import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { createStreamCastServer } from '../src/adapters/ws/index.js';
import {
  MemorySessionStorage,
  StreamCastClient,
  computeReconnectDelay,
  type ClientDeliveredEvent,
  type WebSocketConstructor,
} from '../src/client/index.js';
import { getFreePort, startTestRedis, type EphemeralRedis } from './helpers/redis-harness.js';

describe('StreamCastClient end-to-end & backoff tests', () => {
  let redisEnv: EphemeralRedis;

  beforeAll(async () => {
    redisEnv = await startTestRedis();
  });

  afterAll(async () => {
    await redisEnv.stop();
  });

  it('computes exponential backoff with bounded jitter', () => {
    // Deterministic rng = 0.5 -> zero jitter offset
    expect(
      computeReconnectDelay(
        0,
        { minDelayMs: 100, maxDelayMs: 5000, factor: 2, jitter: 0.3 },
        () => 0.5,
      ),
    ).toBe(100);
    expect(
      computeReconnectDelay(
        3,
        { minDelayMs: 100, maxDelayMs: 5000, factor: 2, jitter: 0.3 },
        () => 0.5,
      ),
    ).toBe(800);
    expect(
      computeReconnectDelay(
        10,
        { minDelayMs: 100, maxDelayMs: 5000, factor: 2, jitter: 0.3 },
        () => 0.5,
      ),
    ).toBe(5000);

    // Extreme rng bounds stay within [base*(1-j), base*(1+j)] and <= maxDelayMs
    const low = computeReconnectDelay(
      1,
      { minDelayMs: 200, maxDelayMs: 5000, factor: 2, jitter: 0.25 },
      () => 0,
    );
    const high = computeReconnectDelay(
      1,
      { minDelayMs: 200, maxDelayMs: 5000, factor: 2, jitter: 0.25 },
      () => 1,
    );
    expect(low).toBe(300); // 400 * 0.75
    expect(high).toBe(500); // 400 * 1.25
  });

  it('automatically reconnects across nodes with exponential backoff, acks, and replays missed events', async () => {
    const prefix = `cli:${randomUUID().slice(0, 8)}`;
    const [portA, portB] = await Promise.all([getFreePort(), getFreePort()]);

    const wssA = new WebSocketServer({ port: portA, host: '127.0.0.1' });
    const wssB = new WebSocketServer({ port: portB, host: '127.0.0.1' });

    const serverA = createStreamCastServer(wssA, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'cli-srv-A',
      readBlockMs: 150,
    });
    const serverB = createStreamCastServer(wssB, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'cli-srv-B',
      readBlockMs: 150,
    });

    await Promise.all([serverA.ready(), serverB.ready()]);

    let connectCount = 0;
    const storage = new MemorySessionStorage();
    const received: ClientDeliveredEvent[] = [];
    const reconnectAttempts: number[] = [];

    const client = new StreamCastClient({
      // First connection goes to Node A; subsequent reconnects go to Node B
      url: () => {
        connectCount += 1;
        return connectCount === 1 ? `ws://127.0.0.1:${portA}` : `ws://127.0.0.1:${portB}`;
      },
      WebSocket: WebSocket as unknown as WebSocketConstructor,
      storage,
      backoff: { minDelayMs: 60, maxDelayMs: 300, factor: 2, jitter: 0.1 },
    });

    client.on('event', (ev) => received.push(ev));
    client.on('reconnecting', ({ attempt }) => reconnectAttempts.push(attempt));

    await new Promise<void>((resolve) => client.once('welcome', () => resolve()));

    const joinedPromise = new Promise<void>((resolve) => client.once('joined', () => resolve()));
    client.join('deployments');
    await joinedPromise;

    await serverA.broadcast('deployments', { n: 1 });
    await new Promise<void>((resolve) => {
      const check = () => (received.length >= 1 ? resolve() : setTimeout(check, 15));
      check();
    });
    // Allow auto-ack to land on serverA
    await new Promise((r) => setTimeout(r, 50));

    // Abruptly terminate Node A's underlying client socket to trigger an automatic reconnect to Node B
    const reconnectedWelcome = new Promise<{ recovered: boolean }>((resolve) => {
      client.once('welcome', (w) => resolve(w));
    });

    for (const sock of wssA.clients) {
      sock.terminate();
    }

    // While client is in backoff delay before hitting Node B, broadcast 5 messages
    for (let n = 2; n <= 6; n++) {
      await serverB.broadcast('deployments', { n });
    }

    const welcome2 = await reconnectedWelcome;
    expect(welcome2.recovered).toBe(true);
    expect(reconnectAttempts.length).toBeGreaterThanOrEqual(1);

    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 4_000;
      const check = () => {
        if (received.length >= 6) return resolve();
        if (Date.now() > deadline) return reject(new Error(`Got only ${received.length} events`));
        setTimeout(check, 20);
      };
      check();
    });

    expect(received.map((e) => (e.data as { n: number }).n)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(received[0]?.replayed).toBe(false);

    client.close();
    await Promise.all([
      serverA.close({ closeWebSocketServer: true }),
      serverB.close({ closeWebSocketServer: true }),
    ]);
  });
});
