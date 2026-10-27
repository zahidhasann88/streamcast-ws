import { spawn, type ChildProcess } from 'node:child_process';
import net from 'node:net';
import Redis from 'ioredis';

export interface EphemeralRedis {
  url: string;
  host: string;
  port: number;
  stop: () => Promise<void>;
}

export async function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address();
      if (addr && typeof addr === 'object') {
        const port = addr.port;
        srv.close(() => resolve(port));
      } else {
        srv.close(() => reject(new Error('Could not determine ephemeral port')));
      }
    });
  });
}

/**
 * Returns a Redis connection for tests:
 * - If `process.env.REDIS_URL` is set (e.g. GitHub Actions service container), connects to it.
 * - Otherwise spawns an isolated `redis-server` child process on an ephemeral port.
 */
export async function startTestRedis(): Promise<EphemeralRedis> {
  const envUrl = process.env['REDIS_URL'];
  if (envUrl) {
    const parsed = new URL(envUrl);
    const host = parsed.hostname || '127.0.0.1';
    const port = parsed.port ? Number(parsed.port) : 6379;
    return {
      url: envUrl,
      host,
      port,
      stop: async () => {
        // External Redis service container is managed by CI
      },
    };
  }

  const port = await getFreePort();
  const host = '127.0.0.1';
  const url = `redis://${host}:${port}`;

  const child: ChildProcess = spawn(
    'redis-server',
    [
      '--port',
      String(port),
      '--bind',
      host,
      '--save',
      '',
      '--appendonly',
      'no',
      '--loglevel',
      'warning',
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );

  let stderrBuf = '';
  let spawnError: Error | null = null;
  child.stderr?.on('data', (chunk: Buffer) => {
    stderrBuf += chunk.toString();
  });
  child.on('error', (err) => {
    spawnError = err;
  });

  const probe = new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 1 });
  probe.on('error', () => {
    // Expected briefly while redis-server binds its listening socket
  });
  const deadline = Date.now() + 8_000;
  let ready = false;

  while (Date.now() < deadline) {
    if (spawnError) {
      probe.disconnect();
      throw new Error(
        `Could not start redis-server (${(spawnError as Error).message}). Install Redis or set ` +
          'REDIS_URL, e.g. `docker run -d -p 127.0.0.1:6379:6379 redis:7-alpine` + REDIS_URL=redis://127.0.0.1:6379',
      );
    }
    if (child.exitCode !== null) {
      probe.disconnect();
      throw new Error(`redis-server exited early (code ${child.exitCode}): ${stderrBuf}`);
    }
    try {
      await probe.connect();
      const pong = await probe.ping();
      if (pong === 'PONG') {
        ready = true;
        break;
      }
    } catch {
      probe.disconnect();
      await new Promise((r) => setTimeout(r, 40));
    }
  }

  await probe.quit().catch(() => probe.disconnect());

  if (!ready) {
    child.kill('SIGKILL');
    throw new Error(`Timed out waiting for redis-server on ${url}: ${stderrBuf}`);
  }

  return {
    url,
    host,
    port,
    stop: async () => {
      if (child.exitCode !== null) return;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          child.kill('SIGKILL');
          resolve();
        }, 2_000);
        child.once('exit', () => {
          clearTimeout(timer);
          resolve();
        });
        child.kill('SIGTERM');
      });
    },
  };
}

/**
 * Controllable TCP proxy sitting in front of Redis so chaos tests can violently sever
 * active TCP connections (`ECONNRESET`), reject reconnects during an outage window,
 * and restore connectivity cleanly. Works against a local `redis-server` or an external Redis.
 */
export class FaultyTcpProxy {
  public readonly listenHost = '127.0.0.1';
  public listenPort = 0;

  private readonly targetHost: string;
  private readonly targetPort: number;
  private server: net.Server | null = null;
  private readonly sockets = new Set<net.Socket>();
  private paused = false;

  constructor(targetHost: string, targetPort: number) {
    this.targetHost = targetHost;
    this.targetPort = targetPort;
  }

  get url(): string {
    return `redis://${this.listenHost}:${this.listenPort}`;
  }

  async start(): Promise<void> {
    this.listenPort = await getFreePort();
    this.server = net.createServer((clientSocket) => {
      if (this.paused) {
        clientSocket.destroy();
        return;
      }

      const upstream = net.connect(this.targetPort, this.targetHost);
      this.sockets.add(clientSocket);
      this.sockets.add(upstream);

      clientSocket.pipe(upstream);
      upstream.pipe(clientSocket);

      const cleanup = () => {
        this.sockets.delete(clientSocket);
        this.sockets.delete(upstream);
        if (!clientSocket.destroyed) clientSocket.destroy();
        if (!upstream.destroyed) upstream.destroy();
      };

      clientSocket.on('error', cleanup);
      upstream.on('error', cleanup);
      clientSocket.on('close', cleanup);
      upstream.on('close', cleanup);
    });

    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(this.listenPort, this.listenHost, () => resolve());
    });
  }

  /**
   * Immediately destroys all active proxied TCP connections and rejects new connections
   * until `restore()` is called.
   */
  severAll(): void {
    this.paused = true;
    for (const sock of Array.from(this.sockets)) {
      sock.destroy();
    }
    this.sockets.clear();
  }

  /**
   * Re-allows incoming TCP connections to forward to Redis.
   */
  restore(): void {
    this.paused = false;
  }

  async stop(): Promise<void> {
    this.severAll();
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve()));
      this.server = null;
    }
  }
}
