# Configuration Reference

## Server: `StreamCastOptions` / `WsStreamCastOptions`

`StreamCast` accepts the core options. `createStreamCastServer(wss, options)` accepts all of them plus the options marked (`ws`).

| Option                          | Type                              | Default                         | Description                                                                                 |
| :------------------------------ | :-------------------------------- | :------------------------------ | :------------------------------------------------------------------------------------------ |
| `redis`                         | `Redis \| RedisOptions \| string` | _required_                      | Redis URL, `ioredis` options, or an `ioredis` instance to duplicate.                        |
| `nodeId`                        | `string`                          | `'node-<random>'`               | Unique identifier for this process (also namespaces its consumer group).                    |
| `prefix`                        | `string`                          | `'streamcast'`                  | Key prefix (`{prefix}:events`, `{prefix}:session:*`, `{prefix}:node:*`).                    |
| `maxStreamLength`               | `number`                          | `10_000`                        | Retention target passed to `XADD MAXLEN`.                                                   |
| `approximateTrim`               | `boolean`                         | `true`                          | Uses `MAXLEN ~ N` for amortized O(1) radix-tree trimming (`false` forces exact `MAXLEN N`). |
| `readBlockMs`                   | `number`                          | `2_000`                         | `XREADGROUP BLOCK` timeout in milliseconds.                                                 |
| `readCount`                     | `number`                          | `128`                           | Max stream entries read per `XREADGROUP` iteration.                                         |
| `heartbeatIntervalMs`           | `number`                          | `5_000`                         | Interval between node heartbeat and presence TTL refreshes.                                 |
| `heartbeatTtlMs`                | `number`                          | `15_000`                        | Expiration TTL for node heartbeat and per-node room sets.                                   |
| `cleanupIntervalMs`             | `number`                          | `15_000`                        | Sweep interval for destroying dead nodes' consumer groups and presence sets.                |
| `dedupeWindowSize`              | `number`                          | `20_000`                        | Bounded set capacity for stream-ID and publisher-UID deduplication.                         |
| `immediateLocalDelivery`        | `boolean`                         | `true`                          | Deliver local-origin broadcasts right after `XADD` and skip in consumer loop.               |
| `publishRetryAttempts`          | `number`                          | `5`                             | Max `XADD` retries on transient socket errors (duplicates deduplicated by `uid`).           |
| `publishRetryBaseDelayMs`       | `number`                          | `40`                            | Base delay for exponential `XADD` retry backoff (capped at 1 s).                            |
| `backpressure.maxBufferedBytes` | `number`                          | `1_048_576`                     | Outgoing `bufferedAmount` threshold in bytes (1 MiB).                                       |
| `backpressure.strategy`         | `'skip' \| 'close'`               | `'skip'`                        | Drop the frame for a slow socket (`'skip'`) or close it (`'close'`).                        |
| `backpressure.closeCode`        | `number`                          | `1008`                          | Close code used when `strategy: 'close'`.                                                   |
| `backpressure.closeReason`      | `string`                          | `'Backpressure limit exceeded'` | Close reason used when `strategy: 'close'`.                                                 |
| `onError`                       | `(err, ctx) => void`              | `undefined`                     | Hook for background Redis, consumer, or delivery errors.                                    |
| `metrics`                       | `StreamCastMetricsHooks`          | `undefined`                     | Callbacks for `onPublish`, `onDeliver`, `onReplay`, `onLag`, `onBackpressure`.              |
| `sessionTtlMs`                  | `number`                          | `600_000`                       | (`ws`) Offline session retention TTL in ms (10 min).                                        |
| `handshake`                     | `'auto' \| 'hello'`               | `'auto'`                        | (`ws`) Auto-welcome vs. mandatory client `hello` frame.                                     |
| `handshakeTimeoutMs`            | `number`                          | `5_000`                         | (`ws`) Time to wait for a `hello` frame before closing with `1008`.                         |
| `allowClientRooms`              | `boolean \| fn`                   | `true`                          | (`ws`) Permit, or authorize per room, client `join`/`leave` frames.                         |

`SessionRecovery` (used internally by the `ws` adapter, also exported) additionally takes `replayBatchSize` (default `500`), the `XRANGE` page size during replay.

## Client: `StreamCastClientOptions`

| Option          | Type                     | Default                | Description                                                                           |
| :-------------- | :----------------------- | :--------------------- | :------------------------------------------------------------------------------------ |
| `url`           | `string \| () => string` | _required_             | Server URL, or a function called on every connect/reconnect.                          |
| `WebSocket`     | constructor              | global `WebSocket`     | WebSocket implementation. Pass `WebSocket` from `ws` on Node.js versions without one. |
| `storage`       | `SessionStorageLike`     | in-memory              | Where `sessionId`/`lastEventId` persist (e.g. `localStorage`).                        |
| `storageKey`    | `string`                 | `'streamcast:session'` | Storage key.                                                                          |
| `autoConnect`   | `boolean`                | `true`                 | Connect in the constructor.                                                           |
| `reconnect`     | `boolean`                | `true`                 | Reconnect after unexpected disconnects.                                               |
| `backoff`       | `BackoffOptions`         | see below              | Exponential backoff with jitter.                                                      |
| `autoAck`       | `boolean`                | `true`                 | Send `ack` frames automatically for received events.                                  |
| `ackIntervalMs` | `number`                 | `0`                    | `0` acks every event; `> 0` coalesces acks and sends the highest id per interval.     |

`BackoffOptions`: `minDelayMs` (`250`), `maxDelayMs` (`15_000`), `factor` (`2`), `jitter` (`0.3`), `maxAttempts` (`Infinity`).

Client events: `open`, `welcome`, `event`, `replay`, `replayComplete`, `resync`, `joined`, `left`, `reconnecting`, `close`, `error`.
