# Architecture & Trade-Offs

## How it works

- **One stream per prefix.** Every `broadcast()` appends an entry to `{prefix}:events` with `XADD ... MAXLEN ~ N`. Fields: room, origin node id, payload, publisher `uid`, timestamp, and an optional `except` list.
- **Per-node consumer groups (`sc:node:<nodeId>`).** Every node must receive _every_ broadcast to fan out to its local sockets, so a single shared consumer group cannot be used (that load-balances messages across consumers). Each node gets its own group instead. Compared to bare `XREAD`:
  1. Pending Entries List (`0`) recovery drains entries that Redis wrote into a dying TCP socket right before an `ECONNRESET` before resuming the live tail (`>`).
  2. Dead nodes are self-describing via `XINFO GROUPS {prefix}:events`: live nodes periodically inspect consumer groups and `SMEMBERS {prefix}:nodes`, destroying groups and stale presence sets (`{prefix}:node:<id>:room:*`) whose `{prefix}:node:<id>:hb` TTL key has expired.
  3. If a group is ever missing (`NOGROUP`), the node recreates it at `lastSeenStreamId` rather than `$`, preventing event gaps.
- **Deduplication.** Nodes deduplicate by stream ID and publisher `uid` (bounded by `dedupeWindowSize`), so `XADD` retries and PEL redelivery after a Redis blip do not produce duplicates.
- **Dedicated connections.** Each node opens two `ioredis` connections: one used only for blocking `XREADGROUP ... BLOCK` calls (`CLIENT UNBLOCK`ed on shutdown for instant teardown), and one for `XADD`, presence pipelines, and Lua session scripts.
- **Membership & presence.** Room membership is kept per node in memory. Global presence (`roomSize`, `listMembers`) lives in Redis as per-node sets with TTL heartbeats.
- **Session recovery.** Session state (rooms, last acknowledged stream id) is a Redis hash with a TTL. On reconnect the server replays missed entries with `XRANGE`, filtered to the session's rooms. See [PROTOCOL.md](./PROTOCOL.md).

## Using the core directly

The `ws` adapter is a thin layer over `StreamCast`. To use another transport, register each connection with a `send` / `getBufferedAmount` / `close` implementation (only the `ws` adapter is tested):

```ts
import { StreamCast } from 'streamcast-ws';

const bus = new StreamCast({ redis: 'redis://127.0.0.1:6379', prefix: 'app' });
await bus.ready();

bus.registerSession('sess-1', {
  send: (msg) => mySocket.write(JSON.stringify(msg)),
  getBufferedAmount: () => mySocket.getBufferedAmount(),
  close: (code, reason) => mySocket.close(code, reason),
});

await bus.join('sess-1', 'room:alpha');
await bus.broadcast('room:alpha', { hello: 'world' });
console.log(await bus.roomSize('room:alpha')); // global count across live nodes
```

The core does not do session recovery on its own; use `SessionRecovery` (exported from `streamcast-ws`) the way the `ws` adapter does.

## Comparison with the Socket.io Redis adapters

| Dimension                 | `@socket.io/redis-adapter` (Pub/Sub)         | `@socket.io/redis-streams-adapter` | `streamcast-ws`                                                                                   |
| :------------------------ | :------------------------------------------- | :--------------------------------- | :------------------------------------------------------------------------------------------------ |
| **Transport coupling**    | Coupled to Socket.io                         | Coupled to Socket.io               | **Transport-agnostic core** + first-class `ws` server/client + experimental Socket.io adapter     |
| **Redis primitive**       | `PUBLISH` / `SUBSCRIBE`                      | `XADD` / `XREAD`                   | `XADD` + per-node `XREADGROUP` + session hashes                                                   |
| **Message durability**    | None (fire-and-forget)                       | Bounded (`MAXLEN`)                 | Bounded (`MAXLEN ~ N`)                                                                            |
| **Offline client replay** | Not supported (no Connection State Recovery) | Via Socket.io CSR                  | **Built-in** (`sessionId` + `ack` + room-filtered `XRANGE` replay + `recovered: false` detection) |
| **Sticky sessions**       | Needed for HTTP long-polling (Engine.IO)     | Needed for HTTP long-polling       | Not needed with the `ws` adapter (WebSocket-only)                                                 |
| **Redis blip safety**     | Messages published during TCP drop are lost  | Resumes from offset                | **PEL (`0`) drain + `lastSeenStreamId` fallback + publisher `uid` deduplication**                 |

## Trade-offs

Using Redis Streams instead of Redis Pub/Sub is not free:

1. **Memory cost of retention.** Pub/Sub retains nothing once delivered. `streamcast-ws` retains up to `maxStreamLength` entries in `{prefix}:events`, plus per-session hashes (`{prefix}:session:<id>`) for `sessionTtlMs`. Measured: `10,000` entries with a ~500 B JSON payload used **6.3 MiB** (`MEMORY USAGE`, Redis 7.4), i.e. roughly payload size plus ~100 B of fields and overhead per entry. Memory grows linearly with `maxStreamLength` × payload size, so size it to cover realistic reconnect windows (e.g. 1–10 minutes of traffic), not archival storage.
2. **More Redis work per message.** Every broadcast appends to a stream, writes consumer-group `XACK`s, and serializes JSON fields; every client `ack` updates a session hash (coalesce with `ackIntervalMs`). Expect more Redis work per message than raw `PUBLISH` (not benchmarked against Pub/Sub here).
3. **Ordering.**
   - All broadcasts across all rooms are appended to one stream and assigned strictly increasing stream IDs (`<ms>-<seq>`). Remote nodes consume entries in stream order.
   - With `immediateLocalDelivery: true` (default), the publishing node delivers to its own sockets right after `XADD` returns and skips that entry when its consumer loop reads it back. Under concurrent publishing from Node A and Node B, a socket on Node A can see Node A's event before Node B's slightly earlier stream entry. For strict ordering across rooms and publishers, set `immediateLocalDelivery: false` so local sockets also receive only through the `XREADGROUP` loop (one extra Redis round-trip for local deliveries).
4. **Replay scan cost.** All rooms share `{prefix}:events` (avoiding many idle per-room streams and consumer groups), so replay scans every entry since the client's cursor and filters by room. Cost grows with the number of entries published since the cursor (read in pages of `replayBatchSize`), so a very large `maxStreamLength` on a busy stream makes reconnects slower.

## Known limitations

- **Trim detection is conservative.** A session gets `recovered: false, reason: "stream_trimmed"` whenever its last acknowledged entry is older than the oldest entry still in the stream. If only the acknowledged entry itself was trimmed (e.g. it was the last entry of an evicted `MAXLEN ~` node), nothing was actually missed, but the client is still asked to resync. Redis does not track trims precisely enough (`max-deleted-entry-id` covers `XDEL` only) to tell these cases apart without extra state.
- **`skip` backpressure drops live frames.** A skipped socket keeps receiving later events and only recovers the gap if it reconnects. Use `strategy: 'close'` if gaps are unacceptable.
- **Late `hello` session migration** (a client that auto-handshakes, then sends `hello` with a different `sessionId`) does not run the second catch-up `XRANGE` that the initial handshake does. `StreamCastClient` always uses `waitHello=1`, so it never takes this path.
- **Single stream per prefix.** One very noisy room shortens the effective retention window for every other room.
- **Socket.io adapter is experimental.** Room broadcasts (`io.to(room).emit(...)`), `except`, `sockets(rooms)`, and `serverCount()` are implemented and tested. `fetchSockets`, `serverSideEmit`, `broadcastWithAck`, and binary attachments are not implemented.
