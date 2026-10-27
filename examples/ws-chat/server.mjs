import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { WebSocketServer } from 'ws';
import { createStreamCastServer } from '../../dist/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 4001);
const NODE_ID = process.env.NODE_ID ?? `chat-node-${PORT}`;
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

const httpServer = createServer(async (req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, nodeId: NODE_ID, port: PORT }));
    return;
  }

  if (req.url === '/' || req.url?.startsWith('/?')) {
    const html = await readFile(path.join(__dirname, 'public', 'index.html'), 'utf8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

const wss = new WebSocketServer({ server: httpServer });

const streamCast = createStreamCastServer(wss, {
  redis: REDIS_URL,
  nodeId: NODE_ID,
  prefix: 'demo-chat',
  maxStreamLength: 500,
  sessionTtlMs: 10 * 60 * 1000,
});

streamCast.on('message', async (_session, data, room) => {
  const targetRoom = room || 'general';
  const payload = typeof data === 'object' && data !== null ? data : { text: String(data) };
  await streamCast.broadcast(targetRoom, {
    ...payload,
    originNode: NODE_ID,
    sentAt: new Date().toISOString(),
  });
});

await streamCast.ready();

// Never log credentials embedded in REDIS_URL (redis://user:password@host).
const redisUrlForLogs = REDIS_URL.replace(/\/\/[^@/]*@/, '//***@');

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[${NODE_ID}] listening on http://0.0.0.0:${PORT} (Redis: ${redisUrlForLogs})`);
});

async function shutdown() {
  console.log(`[${NODE_ID}] shutting down gracefully...`);
  await streamCast.close({ closeWebSocketServer: true });
  httpServer.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
