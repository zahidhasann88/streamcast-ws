import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import net from 'node:net';
import os from 'node:os';
import { performance } from 'node:perf_hooks';
import Redis from 'ioredis';
import { WebSocket, WebSocketServer } from 'ws';
import { createStreamCastServer } from '../src/adapters/ws/index.js';
import { decodeServerFrame, encodeFrame } from '../src/protocol/index.js';

async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address();
      if (addr && typeof addr === 'object') {
        const p = addr.port;
        srv.close(() => resolve(p));
      } else {
        srv.close(() => reject(new Error('Failed to allocate port')));
      }
    });
  });
}

async function ensureRedis(): Promise<{
  url: string;
  version: string;
  cleanup: () => Promise<void>;
}> {
  if (process.env['REDIS_URL']) {
    const url = process.env['REDIS_URL'];
    const client = new Redis(url);
    const info = await client.info('server');
    await client.quit();
    const ver = /redis_version:([^\r\n]+)/.exec(info)?.[1] ?? 'unknown';
    return { url, version: ver, cleanup: async () => undefined };
  }

  const port = await getFreePort();
  const url = `redis://127.0.0.1:${port}`;
  const proc: ChildProcess = spawn(
    'redis-server',
    [
      '--port',
      String(port),
      '--bind',
      '127.0.0.1',
      '--save',
      '',
      '--appendonly',
      'no',
      '--loglevel',
      'warning',
    ],
    { stdio: 'ignore' },
  );

  const client = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 1 });
  client.on('error', () => undefined);
  for (let i = 0; i < 100; i++) {
    try {
      await client.connect();
      if ((await client.ping()) === 'PONG') break;
    } catch {
      client.disconnect();
      await new Promise((r) => setTimeout(r, 30));
    }
  }
  const info = await client.info('server');
  const ver = /redis_version:([^\r\n]+)/.exec(info)?.[1] ?? 'unknown';
  await client.quit();

  return {
    url,
    version: ver,
    cleanup: async () => {
      proc.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 80));
    },
  };
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)]!;
}

async function connectSubscriber(port: number, room: string): Promise<WebSocket> {
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  let welcomed = false;
  let joined = false;

  const onMsg = (raw: WebSocket.RawData) => {
    const res = decodeServerFrame(raw.toString());
    if (!res.ok) return;
    if (res.frame.type === 'welcome') welcomed = true;
    if (res.frame.type === 'joined') joined = true;
  };
  ws.on('message', onMsg);

  await new Promise<void>((resolve) => {
    const poll = () => (welcomed ? resolve() : setTimeout(poll, 5));
    poll();
  });
  ws.send(encodeFrame({ v: 1, type: 'join', room }));
  await new Promise<void>((resolve) => {
    const poll = () => (joined ? resolve() : setTimeout(poll, 5));
    poll();
  });

  ws.off('message', onMsg);
  return ws;
}

async function main(): Promise<void> {
  const redisEnv = await ensureRedis();
  const prefix = `bench:${randomUUID().slice(0, 8)}`;

  try {
    const [portA, portB, portC] = await Promise.all([getFreePort(), getFreePort(), getFreePort()]);
    const wssA = new WebSocketServer({ port: portA, host: '127.0.0.1' });
    const wssB = new WebSocketServer({ port: portB, host: '127.0.0.1' });
    const wssC = new WebSocketServer({ port: portC, host: '127.0.0.1' });

    const serverA = createStreamCastServer(wssA, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'bench-A',
      maxStreamLength: 20_000,
    });
    const serverB = createStreamCastServer(wssB, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'bench-B',
      maxStreamLength: 20_000,
    });
    const serverC = createStreamCastServer(wssC, {
      redis: redisEnv.url,
      prefix,
      nodeId: 'bench-C',
      maxStreamLength: 20_000,
    });

    await Promise.all([serverA.ready(), serverB.ready(), serverC.ready()]);

    // 1. Sequential latency: Node A -> Redis -> Node B -> WS client
    const latencyWs = await connectSubscriber(portB, 'latency-room');
    const WARMUP = 100;
    const SAMPLES = 1_000;
    const latenciesMs: number[] = [];

    let pendingResolve: ((tRecv: number) => void) | null = null;
    latencyWs.on('message', (raw) => {
      const tRecv = performance.now();
      const decoded = decodeServerFrame(raw.toString());
      if (decoded.ok && decoded.frame.type === 'event' && pendingResolve) {
        const fn = pendingResolve;
        pendingResolve = null;
        fn(tRecv);
      }
    });

    for (let i = 0; i < WARMUP + SAMPLES; i++) {
      const waitRecv = new Promise<number>((resolve) => {
        pendingResolve = resolve;
      });
      const tStart = performance.now();
      await serverA.broadcast('latency-room', {
        seq: i,
        text: 'benchmark-payload-message-cross-node-propagation',
      });
      const tRecv = await waitRecv;
      if (i >= WARMUP) {
        latenciesMs.push(tRecv - tStart);
      }
    }

    latenciesMs.sort((a, b) => a - b);
    const meanMs = latenciesMs.reduce((acc, v) => acc + v, 0) / latenciesMs.length;
    const p50Ms = percentile(latenciesMs, 50);
    const p95Ms = percentile(latenciesMs, 95);
    const p99Ms = percentile(latenciesMs, 99);
    const minMs = latenciesMs[0]!;
    const maxMs = latenciesMs[latenciesMs.length - 1]!;

    latencyWs.close();

    // 2. Throughput: Node A publishes to 10 WS clients (5 on Node B, 5 on Node C)
    const SUBSCRIBER_COUNT = 10;
    const THROUGHPUT_MSGS = 5_000;
    const CONCURRENCY = 64;

    const subSockets: WebSocket[] = [];
    for (let s = 0; s < SUBSCRIBER_COUNT; s++) {
      const targetPort = s % 2 === 0 ? portB : portC;
      subSockets.push(await connectSubscriber(targetPort, 'throughput-room'));
    }

    const expectedTotalDeliveries = THROUGHPUT_MSGS * SUBSCRIBER_COUNT;
    let totalDelivered = 0;
    let allDeliveredResolve: (() => void) | null = null;
    const allDeliveredPromise = new Promise<void>((resolve) => {
      allDeliveredResolve = resolve;
    });

    for (const sock of subSockets) {
      sock.on('message', (raw) => {
        const str = raw.toString();
        if (str.includes('"type":"event"')) {
          totalDelivered += 1;
          if (totalDelivered >= expectedTotalDeliveries && allDeliveredResolve) {
            const fn = allDeliveredResolve;
            allDeliveredResolve = null;
            fn();
          }
        }
      });
    }

    const payloadTemplate = {
      room: 'throughput-room',
      body: 'streamcast-ws sustained cross-node throughput test payload (~120B)',
    };

    const tThroughputStart = performance.now();
    let published = 0;

    async function publisherWorker(): Promise<void> {
      while (true) {
        const idx = published;
        if (idx >= THROUGHPUT_MSGS) break;
        published += 1;
        await serverA.broadcast('throughput-room', { seq: idx, ...payloadTemplate });
      }
    }

    await Promise.all(Array.from({ length: CONCURRENCY }, () => publisherWorker()));
    await allDeliveredPromise;
    const elapsedSec = (performance.now() - tThroughputStart) / 1_000;

    const publishRate = Math.round(THROUGHPUT_MSGS / elapsedSec);
    const deliveryRate = Math.round(expectedTotalDeliveries / elapsedSec);

    const cpuModel = os.cpus()[0]?.model ?? 'unknown CPU';
    const cpuCount = os.cpus().length;

    console.log('============================================================');
    console.log(' streamcast-ws Cross-Node Propagation Benchmark');
    console.log('============================================================');
    console.log(`Environment:`);
    console.log(`  OS        : ${os.type()} ${os.release()} (${os.arch()})`);
    console.log(`  CPU       : ${cpuCount} vCPU (${cpuModel})`);
    console.log(`  Node.js   : ${process.version}`);
    console.log(`  Redis     : v${redisEnv.version} (local loopback 127.0.0.1)`);
    console.log('------------------------------------------------------------');
    console.log(`1) Sequential Cross-Node Latency (${SAMPLES} samples, ${WARMUP} warmup)`);
    console.log(`   Path: Node A broadcast() -> Redis XADD -> Node B XREADGROUP -> WS client`);
    console.log(`   min  : ${minMs.toFixed(2)} ms`);
    console.log(`   mean : ${meanMs.toFixed(2)} ms`);
    console.log(`   p50  : ${p50Ms.toFixed(2)} ms`);
    console.log(`   p95  : ${p95Ms.toFixed(2)} ms`);
    console.log(`   p99  : ${p99Ms.toFixed(2)} ms`);
    console.log(`   max  : ${maxMs.toFixed(2)} ms`);
    console.log('------------------------------------------------------------');
    console.log(
      `2) Cross-Node Throughput (${THROUGHPUT_MSGS} msgs x ${SUBSCRIBER_COUNT} WS clients across 2 remote nodes, concurrency=${CONCURRENCY})`,
    );
    console.log(`   Elapsed           : ${elapsedSec.toFixed(2)} s`);
    console.log(`   Published rate    : ${publishRate.toLocaleString()} msgs/sec`);
    console.log(`   WS delivery rate  : ${deliveryRate.toLocaleString()} deliveries/sec`);
    console.log('============================================================');

    for (const sock of subSockets) {
      sock.close();
    }
    await Promise.all([
      serverA.close({ closeWebSocketServer: true }),
      serverB.close({ closeWebSocketServer: true }),
      serverC.close({ closeWebSocketServer: true }),
    ]);
  } finally {
    await redisEnv.cleanup();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
