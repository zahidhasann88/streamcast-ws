import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { createStreamCastServer } from '../src/adapters/ws/index.js';
import { StreamCast, type StreamCastMessage } from '../src/core/index.js';
import { decodeServerFrame, encodeFrame, type EventFrame } from '../src/protocol/index.js';
import {
  FaultyTcpProxy,
  getFreePort,
  startTestRedis,
  type EphemeralRedis,
} from './helpers/redis-harness.js';

describe('Chaos resilience tests (Redis connection kill & restore mid-stream)', () => {
  let redisEnv: EphemeralRedis;

  beforeAll(async () => {
    redisEnv = await startTestRedis();
  });

  afterAll(async () => {
    await redisEnv.stop();
  });

  it('loses zero messages and produces zero duplicates when all Redis TCP sockets are severed mid-stream', async () => {
    const prefix = `chaos-proxy:${randomUUID().slice(0, 8)}`;
    const proxy = new FaultyTcpProxy(redisEnv.host, redisEnv.port);
    await proxy.start();

    const wsPort = await getFreePort();
    const wssSub = new WebSocketServer({ port: wsPort, host: '127.0.0.1' });

    const pubNode = new StreamCast({
      redis: proxy.url,
      prefix,
      nodeId: 'chaos-pub',
      readBlockMs: 150,
      publishRetryAttempts: 12,
      publishRetryBaseDelayMs: 30,
    });

    const subServer = createStreamCastServer(wssSub, {
      redis: proxy.url,
      prefix,
      nodeId: 'chaos-sub',
      readBlockMs: 150,
    });

    await Promise.all([pubNode.ready(), subServer.ready()]);

    const clientWs = new WebSocket(`ws://127.0.0.1:${wsPort}`);
    const receivedEvents: EventFrame[] = [];
    let welcomed = false;
    let joined = false;

    clientWs.on('message', (raw) => {
      const res = decodeServerFrame(raw.toString());
      if (!res.ok) return;
      if (res.frame.type === 'welcome') welcomed = true;
      if (res.frame.type === 'joined') joined = true;
      if (res.frame.type === 'event') receivedEvents.push(res.frame);
    });

    await new Promise<void>((resolve) => {
      const check = () => (welcomed ? resolve() : setTimeout(check, 10));
      check();
    });

    clientWs.send(encodeFrame({ v: 1, type: 'join', room: 'firehose' }));
    await new Promise<void>((resolve) => {
      const check = () => (joined ? resolve() : setTimeout(check, 10));
      check();
    });

    const TOTAL_MESSAGES = 100;
    const publishPromises: Promise<string>[] = [];

    // Stream 100 messages, severing all Redis TCP connections (and refusing reconnects for 180ms) at #40
    for (let seq = 0; seq < TOTAL_MESSAGES; seq++) {
      if (seq === 40) {
        proxy.severAll();
        setTimeout(() => {
          proxy.restore();
        }, 180);
      }

      publishPromises.push(pubNode.broadcast('firehose', { seq }));
      await new Promise((r) => setTimeout(r, 8));
    }

    await Promise.all(publishPromises);

    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 8_000;
      const check = () => {
        if (receivedEvents.length >= TOTAL_MESSAGES) {
          resolve();
          return;
        }
        if (Date.now() > deadline) {
          reject(
            new Error(
              `Timed out waiting for ${TOTAL_MESSAGES} messages after Redis outage; got ${receivedEvents.length}`,
            ),
          );
          return;
        }
        setTimeout(check, 25);
      };
      check();
    });

    // Wait an extra 200ms to confirm no late duplicate deliveries arrive
    await new Promise((r) => setTimeout(r, 200));

    const receivedSeqs = receivedEvents.map((e) => (e.data as { seq: number }).seq);
    const uniqueSeqs = new Set(receivedSeqs);

    expect(subServer.core.getMetrics().redisReconnects).toBeGreaterThanOrEqual(1);
    expect(receivedSeqs).toHaveLength(TOTAL_MESSAGES);
    expect(uniqueSeqs.size).toBe(TOTAL_MESSAGES);
    expect(receivedSeqs).toEqual(Array.from({ length: TOTAL_MESSAGES }, (_, i) => i));

    clientWs.close();
    await Promise.all([pubNode.close(), subServer.close({ closeWebSocketServer: true })]);
    await proxy.stop();
  });

  it('recovers cleanly from Redis server-side CLIENT KILL mid-stream across 3 nodes without loss or duplication', async () => {
    const prefix = `chaos-kill:${randomUUID().slice(0, 8)}`;
    const admin = new Redis(redisEnv.url);

    const nodes = [
      new StreamCast({ redis: redisEnv.url, prefix, nodeId: 'k-A', readBlockMs: 150 }),
      new StreamCast({ redis: redisEnv.url, prefix, nodeId: 'k-B', readBlockMs: 150 }),
      new StreamCast({ redis: redisEnv.url, prefix, nodeId: 'k-C', readBlockMs: 150 }),
    ];
    await Promise.all(nodes.map((n) => n.ready()));

    const receivedOnA: StreamCastMessage[] = [];
    const receivedOnB: StreamCastMessage[] = [];

    nodes[0]!.registerSession('sa', { send: (m) => receivedOnA.push(m) });
    nodes[1]!.registerSession('sb', { send: (m) => receivedOnB.push(m) });
    await nodes[0]!.join('sa', 'ops');
    await nodes[1]!.join('sb', 'ops');

    const TOTAL = 80;
    for (let i = 0; i < TOTAL; i++) {
      if (i === 30) {
        // Evict all normal client connections except admin itself (`SKIPME yes`)
        await admin.call('CLIENT', 'KILL', 'TYPE', 'normal', 'SKIPME', 'yes');
      }
      await nodes[2]!.broadcast('ops', { seq: i });
      if (i % 5 === 0) {
        await new Promise((r) => setTimeout(r, 5));
      }
    }

    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 6_000;
      const check = () => {
        if (receivedOnA.length >= TOTAL && receivedOnB.length >= TOTAL) return resolve();
        if (Date.now() > deadline) {
          return reject(
            new Error(
              `Incomplete after CLIENT KILL: A=${receivedOnA.length}, B=${receivedOnB.length}`,
            ),
          );
        }
        setTimeout(check, 20);
      };
      check();
    });

    await new Promise((r) => setTimeout(r, 150));

    const expected = Array.from({ length: TOTAL }, (_, i) => i);
    expect(receivedOnA.map((m) => (m.payload as { seq: number }).seq)).toEqual(expected);
    expect(receivedOnB.map((m) => (m.payload as { seq: number }).seq)).toEqual(expected);

    await Promise.all(nodes.map((n) => n.close()));
    await admin.quit();
  });
});
