import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WebSocket, WebSocketServer } from 'ws';
import { createStreamCastServer, type StreamCastWsServer } from '../src/adapters/ws/index.js';
import {
  decodeServerFrame,
  encodeFrame,
  type EventFrame,
  type ServerFrame,
  type WelcomeFrame,
} from '../src/protocol/index.js';
import { getFreePort, startTestRedis, type EphemeralRedis } from './helpers/redis-harness.js';

interface TestWsClient {
  ws: WebSocket;
  frames: ServerFrame[];
  events: EventFrame[];
  waitWelcome: () => Promise<WelcomeFrame>;
  waitJoined: (room: string) => Promise<void>;
  waitEvents: (count: number) => Promise<EventFrame[]>;
  close: () => Promise<void>;
}

async function connectTestClient(url: string): Promise<TestWsClient> {
  const ws = new WebSocket(url);
  const frames: ServerFrame[] = [];
  const events: EventFrame[] = [];

  ws.on('message', (raw) => {
    const res = decodeServerFrame(raw.toString());
    if (res.ok) {
      frames.push(res.frame);
      if (res.frame.type === 'event') {
        events.push(res.frame);
      }
    }
  });

  await new Promise<void>((resolve, reject) => {
    ws.once('open', () => resolve());
    ws.once('error', reject);
  });

  const waitForFrame = <T extends ServerFrame>(
    pred: (f: ServerFrame) => f is T,
    timeoutMs = 4_000,
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
          reject(new Error(`Timed out waiting for frame. Received: ${JSON.stringify(frames)}`));
          return;
        }
        setTimeout(check, 15);
      };
      check();
    });
  };

  return {
    ws,
    frames,
    events,
    waitWelcome: () => waitForFrame((f): f is WelcomeFrame => f.type === 'welcome'),
    waitJoined: async (room: string) => {
      await waitForFrame(
        (f): f is Extract<ServerFrame, { type: 'joined' }> =>
          f.type === 'joined' && f.room === room,
      );
    },
    waitEvents: (count: number, timeoutMs = 4_000) => {
      const start = Date.now();
      return new Promise((resolve, reject) => {
        const check = () => {
          if (events.length >= count) {
            resolve(events.slice(0, count));
            return;
          }
          if (Date.now() - start > timeoutMs) {
            reject(new Error(`Timed out waiting for ${count} events (got ${events.length})`));
            return;
          }
          setTimeout(check, 15);
        };
        check();
      });
    },
    close: async () => {
      if (ws.readyState === WebSocket.CLOSED) return;
      await new Promise<void>((resolve) => {
        ws.once('close', () => resolve());
        ws.close();
      });
    },
  };
}

describe('ws Adapter 3-node integration tests', () => {
  let redisEnv: EphemeralRedis;

  beforeAll(async () => {
    redisEnv = await startTestRedis();
  });

  afterAll(async () => {
    await redisEnv.stop();
  });

  it('fans out a broadcast from node C to clients on node A and node B in the same room exactly once', async () => {
    const prefix = `ws3:${randomUUID().slice(0, 8)}`;
    const [portA, portB, portC] = await Promise.all([getFreePort(), getFreePort(), getFreePort()]);

    const wssA = new WebSocketServer({ port: portA, host: '127.0.0.1' });
    const wssB = new WebSocketServer({ port: portB, host: '127.0.0.1' });
    const wssC = new WebSocketServer({ port: portC, host: '127.0.0.1' });

    const serverA: StreamCastWsServer = createStreamCastServer(wssA, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'ws-node-A',
      readBlockMs: 200,
    });
    const serverB: StreamCastWsServer = createStreamCastServer(wssB, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'ws-node-B',
      readBlockMs: 200,
    });
    const serverC: StreamCastWsServer = createStreamCastServer(wssC, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'ws-node-C',
      readBlockMs: 200,
    });

    await Promise.all([serverA.ready(), serverB.ready(), serverC.ready()]);

    const clientA = await connectTestClient(`ws://127.0.0.1:${portA}`);
    const clientB = await connectTestClient(`ws://127.0.0.1:${portB}`);
    const clientUnrelated = await connectTestClient(`ws://127.0.0.1:${portA}`);

    const welcomeA = await clientA.waitWelcome();
    const welcomeB = await clientB.waitWelcome();
    await clientUnrelated.waitWelcome();

    clientA.ws.send(encodeFrame({ v: 1, type: 'join', room: 'announcements' }));
    clientB.ws.send(encodeFrame({ v: 1, type: 'join', room: 'announcements' }));
    clientUnrelated.ws.send(encodeFrame({ v: 1, type: 'join', room: 'other-room' }));

    await Promise.all([
      clientA.waitJoined('announcements'),
      clientB.waitJoined('announcements'),
      clientUnrelated.waitJoined('other-room'),
    ]);

    expect(await serverC.roomSize('announcements')).toBe(2);
    expect((await serverC.listMembers('announcements')).sort()).toEqual(
      [welcomeA.sessionId, welcomeB.sessionId].sort(),
    );

    const publishedId = await serverC.broadcast('announcements', {
      title: 'Deploy v2',
      author: 'node-C',
    });

    await Promise.all([clientA.waitEvents(1), clientB.waitEvents(1)]);

    // Give extra time to ensure no duplicate delivery ever arrives
    await new Promise((r) => setTimeout(r, 150));

    expect(clientA.events).toHaveLength(1);
    expect(clientB.events).toHaveLength(1);
    expect(clientUnrelated.events).toHaveLength(0);

    expect(clientA.events[0]?.id).toBe(publishedId);
    expect(clientA.events[0]?.data).toEqual({ title: 'Deploy v2', author: 'node-C' });
    expect(clientB.events[0]?.id).toBe(publishedId);
    expect(clientB.events[0]?.data).toEqual({ title: 'Deploy v2', author: 'node-C' });

    await Promise.all([clientA.close(), clientB.close(), clientUnrelated.close()]);
    await Promise.all([
      serverA.close({ closeWebSocketServer: true }),
      serverB.close({ closeWebSocketServer: true }),
      serverC.close({ closeWebSocketServer: true }),
    ]);
  });
});
