# streamcast-ws

Room-based WebSocket scaling across multiple Node.js processes using **Redis Streams**, with session recovery for clients that reconnect. No sticky sessions, no Kafka, no RabbitMQ.

> **Status:** `0.x`: APIs may change between minor versions until `1.0`.

## Why

Behind a load balancer, a client on node A and a client on node B can join the same room, and a broadcast from node C has to reach both. Redis Pub/Sub adapters do this, but fire-and-forget: anything published while a client is reconnecting (network switch, rolling deploy, Redis blip) is lost.

`streamcast-ws` appends broadcasts to a bounded Redis Stream instead. Every node reads it through its own consumer group. Reconnecting clients, on any node, get their rooms restored and missed messages replayed in order. If their cursor has aged out of the retention window, they get an explicit `recovered: false` so they can do a full resync.

**Compared to `@socket.io/redis-adapter`:** messages are durable for a retention window and replayable, and the core is not tied to Socket.io. The costs: Redis memory for retained entries (measured 6.3 MiB per 10,000 ~500 B messages), more Redis work per message than `PUBLISH`, and no guaranteed ordering between local and remote publishers unless `immediateLocalDelivery: false`. Full comparison and trade-offs: [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md).

## Install

Requires Node.js >= 22 and Redis >= 7.0 (tested with 7.4). Ships ESM + CommonJS builds with TypeScript types.

```bash
npm install streamcast-ws ws
```

## Quickstart

**Server** (run as many instances as you like against the same Redis):

```ts
import { WebSocketServer } from 'ws';
import { createStreamCastServer } from 'streamcast-ws';

const wss = new WebSocketServer({ port: 3000 });
const server = createStreamCastServer(wss, {
  redis: process.env.REDIS_URL ?? 'redis://127.0.0.1:6379',
  sessionTtlMs: 5 * 60_000, // how long a disconnected client can recover
});

server.on('connection', async (session, _req, info) => {
  if (!info.recovered) await session.join('lobby');
});

server.on('message', async (session, data, room) => {
  // Reaches room members on every node, except the sender
  await server.broadcast(room ?? 'lobby', data, { except: session });
});
```

**Client** (browser; on Node.js without a global `WebSocket`, pass `WebSocket` from `ws`):

```ts
import { StreamCastClient } from 'streamcast-ws/client';

const client = new StreamCastClient({ url: 'wss://realtime.example.com', storage: localStorage });

client.on('event', ({ room, data, replayed }) =>
  console.log(room, data, replayed ? '(replayed)' : ''),
);
client.on('resync', ({ reason }) => {
  // Missed events are gone (stream_trimmed or session_not_found): reload state from your API
});
client.on('welcome', () => client.send({ text: 'hi' }, 'lobby')); // send once the handshake is done
```

The client handles reconnects (exponential backoff with jitter), stores `sessionId`/`lastEventId`, and sends acks.

## Session recovery

1. On connect, the server issues `{ sessionId, lastEventId }` and stores the session's rooms and cursor in a Redis hash with a TTL.
2. The client acks events by stream id. The server advances the stored cursor.
3. On reconnect with `sessionId` (any node), the server restores rooms and replays missed events (`XRANGE`, filtered to the session's rooms), then sends `replay_complete`.
4. If the cursor was trimmed from the stream, the server sends `recovered: false, reason: "stream_trimmed"`. If the session expired, it sends `reason: "session_not_found"` with a new session.

## Other entry points

- **`StreamCast`** (`streamcast-ws`): the transport-agnostic core (`join`, `leave`, `broadcast`, `roomSize`, `listMembers`). Plug in any transport via `registerSession(id, { send, getBufferedAmount, close })`; [example](./docs/ARCHITECTURE.md#using-the-core-directly).
- **Socket.io adapter** (`streamcast-ws/socketio`, experimental; needs `socket.io-adapter`, which `socket.io` already installs): `new Server({ adapter: createSocketIoAdapter({ redis }) })`. Supports room broadcasts, `except`, `sockets()`, and `serverCount()`; no `fetchSockets`/`serverSideEmit` yet.

## Documentation

- [Configuration reference](./docs/CONFIGURATION.md): server and client options
- [Architecture & trade-offs](./docs/ARCHITECTURE.md): design, comparison, known limitations
- [Wire protocol](./docs/PROTOCOL.md)
- [Benchmarks](./docs/BENCHMARKS.md): measured cross-node latency (p50 ≈ 3 ms on a laptop with Redis in Docker) and throughput
- [2-node chat demo](./examples/ws-chat/README.md): `docker compose up --build` in `examples/ws-chat`

## Development

Integration tests need Redis 7+: a local `redis-server`, or set `REDIS_URL` (e.g. Docker):

```bash
docker run -d -p 127.0.0.1:6379:6379 redis:7-alpine
REDIS_URL=redis://127.0.0.1:6379 npm test   # also: npm run bench | lint | typecheck | build
```

## License

[MIT](./LICENSE)
