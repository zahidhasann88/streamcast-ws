import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { createStreamCastServer } from '../src/adapters/ws/index.js';
import {
  decodeServerFrame,
  encodeFrame,
  type EventFrame,
  type ReplayCompleteFrame,
  type ReplayFrame,
  type ServerFrame,
  type WelcomeFrame,
} from '../src/protocol/index.js';
import { getFreePort, startTestRedis, type EphemeralRedis } from './helpers/redis-harness.js';

function collectFrames(ws: WebSocket) {
  const frames: ServerFrame[] = [];
  ws.on('message', (raw) => {
    const res = decodeServerFrame(raw.toString());
    if (res.ok) {
      frames.push(res.frame);
    }
  });

  const waitFor = <T extends ServerFrame>(
    pred: (f: ServerFrame) => f is T,
    timeoutMs = 5_000,
  ): Promise<T> => {
    const start = Date.now();
    return new Promise((resolve, reject) => {
      const check = () => {
        const hit = frames.find(pred);
        if (hit) {
          resolve(hit);
          return;
        }
        if (Date.now() - start > timeoutMs) {
          reject(new Error(`Timeout waiting for frame. Seen: ${JSON.stringify(frames)}`));
          return;
        }
        setTimeout(check, 15);
      };
      check();
    });
  };

  return { frames, waitFor };
}

describe('Session Recovery & Replay tests', () => {
  let redisEnv: EphemeralRedis;

  beforeAll(async () => {
    redisEnv = await startTestRedis();
  });

  afterAll(async () => {
    await redisEnv.stop();
  });

  it('replays all 50 missed messages in exact order on cross-node reconnect (filtered to session rooms)', async () => {
    const prefix = `rec50:${randomUUID().slice(0, 8)}`;
    const [portA, portB] = await Promise.all([getFreePort(), getFreePort()]);

    const wssA = new WebSocketServer({ port: portA, host: '127.0.0.1' });
    const wssB = new WebSocketServer({ port: portB, host: '127.0.0.1' });

    const serverA = createStreamCastServer(wssA, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'rec-A',
      readBlockMs: 200,
    });
    const serverB = createStreamCastServer(wssB, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'rec-B',
      readBlockMs: 200,
    });

    await Promise.all([serverA.ready(), serverB.ready()]);

    // 1. Connect to Node A using hello handshake
    const ws1 = new WebSocket(`ws://127.0.0.1:${portA}?waitHello=1`);
    const c1 = collectFrames(ws1);
    await new Promise<void>((res, rej) => {
      ws1.once('open', () => res());
      ws1.once('error', rej);
    });
    ws1.send(encodeFrame({ v: 1, type: 'hello' }));

    const welcome1 = await c1.waitFor((f): f is WelcomeFrame => f.type === 'welcome');
    expect(welcome1.recovered).toBe(false);
    expect(welcome1.reason).toBe('new_session');
    const sessionId = welcome1.sessionId;

    // Join multiple rooms concurrently to verify atomic Lua room tracking
    ws1.send(encodeFrame({ v: 1, type: 'join', room: 'orders' }));
    ws1.send(encodeFrame({ v: 1, type: 'join', room: 'audit' }));
    await c1.waitFor(
      (f): f is Extract<ServerFrame, { type: 'joined' }> =>
        f.type === 'joined' && f.room === 'orders',
    );
    await c1.waitFor(
      (f): f is Extract<ServerFrame, { type: 'joined' }> =>
        f.type === 'joined' && f.room === 'audit',
    );

    await serverA.broadcast('orders', { seq: -1, kind: 'baseline' });
    const baselineEvent = await c1.waitFor((f): f is EventFrame => f.type === 'event');
    ws1.send(encodeFrame({ v: 1, type: 'ack', id: baselineEvent.id }));

    // Give ack frame a moment to persist in Redis
    await new Promise((r) => setTimeout(r, 60));

    // 2. Disconnect client from Node A
    await new Promise<void>((resolve) => {
      ws1.once('close', () => resolve());
      ws1.close();
    });

    // 3. Broadcast 50 messages to 'orders' (plus 15 noise messages to 'unsubscribed-room')
    for (let i = 0; i < 50; i++) {
      if (i % 3 === 0) {
        await serverB.broadcast('unsubscribed-room', { noise: i });
      }
      await serverB.broadcast('orders', { seq: i, text: `msg-${i}` });
    }

    // 4. Reconnect to Node B (a different server) with sessionId
    const ws2 = new WebSocket(`ws://127.0.0.1:${portB}?waitHello=1`);
    const c2 = collectFrames(ws2);
    await new Promise<void>((res, rej) => {
      ws2.once('open', () => res());
      ws2.once('error', rej);
    });
    ws2.send(encodeFrame({ v: 1, type: 'hello', sessionId }));

    const welcome2 = await c2.waitFor((f): f is WelcomeFrame => f.type === 'welcome');
    expect(welcome2.sessionId).toBe(sessionId);
    expect(welcome2.recovered).toBe(true);
    expect(welcome2.reason).toBe('recovered');
    expect(welcome2.rooms.sort()).toEqual(['audit', 'orders']);

    const replayDone = await c2.waitFor(
      (f): f is ReplayCompleteFrame => f.type === 'replay_complete',
    );
    expect(replayDone.count).toBe(50);

    const replays = c2.frames.filter((f): f is ReplayFrame => f.type === 'replay');
    expect(replays).toHaveLength(50);
    expect(replays.map((r) => (r.data as { seq: number }).seq)).toEqual(
      Array.from({ length: 50 }, (_, idx) => idx),
    );
    expect(replays.every((r) => r.room === 'orders')).toBe(true);

    // 5. Verify restored room membership immediately receives subsequent live broadcasts on Node B
    await serverA.broadcast('audit', { liveAfterReplay: true });
    const liveEvent = await c2.waitFor((f): f is EventFrame => f.type === 'event');
    expect(liveEvent.room).toBe('audit');
    expect(liveEvent.data).toEqual({ liveAfterReplay: true });

    ws2.close();
    await Promise.all([
      serverA.close({ closeWebSocketServer: true }),
      serverB.close({ closeWebSocketServer: true }),
    ]);
  });

  it('signals recovered: false (reason: stream_trimmed) when lastEventId falls off the trimmed stream', async () => {
    const prefix = `trim:${randomUUID().slice(0, 8)}`;
    const port = await getFreePort();
    const wss = new WebSocketServer({ port, host: '127.0.0.1' });

    // Configure maxStreamLength = 25 with exact trimming so deterministic eviction occurs at >25 entries
    const server = createStreamCastServer(wss, {
      redis: redisEnv.url,
      prefix,
      maxStreamLength: 25,
      approximateTrim: false,
      readBlockMs: 200,
    });
    await server.ready();

    const ws1 = new WebSocket(`ws://127.0.0.1:${port}?waitHello=1`);
    const c1 = collectFrames(ws1);
    await new Promise<void>((res, rej) => {
      ws1.once('open', () => res());
      ws1.once('error', rej);
    });
    ws1.send(encodeFrame({ v: 1, type: 'hello' }));

    const welcome1 = await c1.waitFor((f): f is WelcomeFrame => f.type === 'welcome');
    ws1.send(encodeFrame({ v: 1, type: 'join', room: 'ticker' }));
    await c1.waitFor((f): f is Extract<ServerFrame, { type: 'joined' }> => f.type === 'joined');

    await server.broadcast('ticker', { price: 100 });
    const ev0 = await c1.waitFor((f): f is EventFrame => f.type === 'event');
    ws1.send(encodeFrame({ v: 1, type: 'ack', id: ev0.id }));
    await new Promise((r) => setTimeout(r, 50));

    await new Promise<void>((resolve) => {
      ws1.once('close', () => resolve());
      ws1.close();
    });

    // Publish 40 more events (> maxStreamLength 25), evicting ev0.id from the stream
    for (let i = 1; i <= 40; i++) {
      await server.broadcast('ticker', { price: 100 + i });
    }

    // Reconnect with sessionId -> should signal recovered: false, reason: 'stream_trimmed'
    const ws2 = new WebSocket(`ws://127.0.0.1:${port}?waitHello=1`);
    const c2 = collectFrames(ws2);
    await new Promise<void>((res, rej) => {
      ws2.once('open', () => res());
      ws2.once('error', rej);
    });
    ws2.send(encodeFrame({ v: 1, type: 'hello', sessionId: welcome1.sessionId }));

    const welcome2 = await c2.waitFor((f): f is WelcomeFrame => f.type === 'welcome');
    expect(welcome2.sessionId).toBe(welcome1.sessionId);
    expect(welcome2.recovered).toBe(false);
    expect(welcome2.reason).toBe('stream_trimmed');
    expect(welcome2.rooms).toEqual(['ticker']);

    const replays = c2.frames.filter((f) => f.type === 'replay');
    expect(replays).toHaveLength(0);

    // Verify live traffic still flows after client initiates full resync
    await server.broadcast('ticker', { price: 999 });
    const postResyncLive = await c2.waitFor((f): f is EventFrame => f.type === 'event');
    expect(postResyncLive.data).toEqual({ price: 999 });

    ws2.close();
    await server.close({ closeWebSocketServer: true });
  });

  it('issues a new session with recovered: false (session_not_found) when session TTL has expired', async () => {
    const prefix = `ttl:${randomUUID().slice(0, 8)}`;
    const port = await getFreePort();
    const wss = new WebSocketServer({ port, host: '127.0.0.1' });

    const server = createStreamCastServer(wss, {
      redis: redisEnv.url,
      prefix,
      sessionTtlMs: 180,
      readBlockMs: 150,
    });
    await server.ready();

    const ws1 = new WebSocket(`ws://127.0.0.1:${port}`);
    const c1 = collectFrames(ws1);
    const welcome1 = await c1.waitFor((f): f is WelcomeFrame => f.type === 'welcome');
    await new Promise<void>((resolve) => {
      ws1.once('close', () => resolve());
      ws1.close();
    });

    // Wait for session TTL (180ms) to expire
    await new Promise((r) => setTimeout(r, 280));

    const ws2 = new WebSocket(`ws://127.0.0.1:${port}?sessionId=${welcome1.sessionId}`);
    const c2 = collectFrames(ws2);
    const welcome2 = await c2.waitFor((f): f is WelcomeFrame => f.type === 'welcome');

    expect(welcome2.recovered).toBe(false);
    expect(welcome2.reason).toBe('session_not_found');
    expect(welcome2.sessionId).not.toBe(welcome1.sessionId);

    ws2.close();
    await server.close({ closeWebSocketServer: true });
  });
});
