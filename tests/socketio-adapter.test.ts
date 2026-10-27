import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { Server as SocketIoServer } from 'socket.io';
import { io as createSocketIoClient } from 'socket.io-client';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createSocketIoAdapter } from '../src/adapters/socketio/index.js';
import { getFreePort, startTestRedis, type EphemeralRedis } from './helpers/redis-harness.js';

describe('Experimental Socket.io Adapter 3-node integration tests', () => {
  let redisEnv: EphemeralRedis;

  beforeAll(async () => {
    redisEnv = await startTestRedis();
  });

  afterAll(async () => {
    await redisEnv.stop();
  });

  it('scales room broadcasts, except exclusions, sockets(), and serverCount() across 3 Socket.io servers', async () => {
    const prefix = `sio3:${randomUUID().slice(0, 8)}`;
    const ports = await Promise.all([getFreePort(), getFreePort(), getFreePort()]);

    const httpServers = ports.map(() => createServer());
    const ioServers = httpServers.map(
      (httpSrv, idx) =>
        new SocketIoServer(httpSrv, {
          adapter: createSocketIoAdapter({
            redis: redisEnv.url,
            prefix,
            nodeId: `sio-node-${idx}`,
            readBlockMs: 150,
          }),
        }),
    );

    await Promise.all(
      httpServers.map(
        (srv, idx) =>
          new Promise<void>((resolve) => srv.listen(ports[idx]!, '127.0.0.1', () => resolve())),
      ),
    );

    // Wait briefly for adapter init() promises to register listeners
    await new Promise((r) => setTimeout(r, 120));

    expect(await ioServers[0]!.of('/').adapter.serverCount()).toBe(3);

    const clientA = createSocketIoClient(`http://127.0.0.1:${ports[0]}`, {
      transports: ['websocket'],
    });
    const clientB = createSocketIoClient(`http://127.0.0.1:${ports[1]}`, {
      transports: ['websocket'],
    });

    await Promise.all([
      new Promise<void>((res) => clientA.once('connect', () => res())),
      new Promise<void>((res) => clientB.once('connect', () => res())),
    ]);

    const socketOnA = ioServers[0]!.sockets.sockets.get(clientA.id!);
    const socketOnB = ioServers[1]!.sockets.sockets.get(clientB.id!);
    expect(socketOnA).toBeDefined();
    expect(socketOnB).toBeDefined();

    await Promise.all([socketOnA!.join('guild-7'), socketOnB!.join('guild-7')]);
    await new Promise((r) => setTimeout(r, 60));

    const globalMembers = await ioServers[2]!.in('guild-7').allSockets();
    expect(globalMembers.size).toBe(2);
    expect(globalMembers.has(clientA.id!)).toBe(true);
    expect(globalMembers.has(clientB.id!)).toBe(true);

    const receivedA: unknown[] = [];
    const receivedB: unknown[] = [];
    clientA.on('quest', (payload) => receivedA.push(payload));
    clientB.on('quest', (payload) => receivedB.push(payload));

    // Emit from server 2, which has no connected clients
    ioServers[2]!.to('guild-7').emit('quest', { boss: 'dragon' });

    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 4_000;
      const poll = () => {
        if (receivedA.length >= 1 && receivedB.length >= 1) return resolve();
        if (Date.now() > deadline) return reject(new Error('Timed out waiting for quest event'));
        setTimeout(poll, 15);
      };
      poll();
    });

    // Verify no duplicates arrive
    await new Promise((r) => setTimeout(r, 120));
    expect(receivedA).toEqual([{ boss: 'dragon' }]);
    expect(receivedB).toEqual([{ boss: 'dragon' }]);

    ioServers[2]!.to('guild-7').except(clientB.id!).emit('quest', { loot: 'gold' });
    await new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + 4_000;
      const poll = () => {
        if (receivedA.length >= 2) return resolve();
        if (Date.now() > deadline) return reject(new Error('Timed out waiting for loot event'));
        setTimeout(poll, 15);
      };
      poll();
    });
    await new Promise((r) => setTimeout(r, 120));
    expect(receivedA).toHaveLength(2);
    expect(receivedB).toHaveLength(1); // clientB excluded!

    clientA.disconnect();
    clientB.disconnect();
    await Promise.all(
      ioServers.map(
        (io) =>
          new Promise<void>((resolve) => {
            void io.of('/').adapter.close();
            io.close(() => resolve());
          }),
      ),
    );
  });
});
