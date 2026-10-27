# `streamcast-ws` Wire Protocol Specification (v1)

`streamcast-ws` defines a lightweight, versioned JSON framing protocol over WebSockets for session issuance, acknowledgements (`ack`), room management, live fan-out, and missed-message replay on reconnect.

Every protocol frame is a UTF-8 JSON object containing:

- `v`: integer protocol version (currently `1`)
- `type`: string discriminator identifying the frame kind

Plain JSON objects sent by applications that do **not** include both `v` and `type` are treated as pass-through application payloads by the `ws` adapter (`server.on('message', ...)`).

---

## Stream IDs

Event cursors (`id` and `lastEventId`) are canonical Redis Stream entry identifiers formatted as:

```
<millisecondsTime>-<sequenceNumber>
```

For example: `"1761546000123-0"`. An empty/uninitialized cursor is `"0-0"`. Stream IDs MUST be compared numerically by `(millisecondsTime, sequenceNumber)` as 64-bit unsigned integers, never lexicographically.

---

## Handshake & Session Recovery Lifecycle

### 1. Initial Connection

A client may connect in either of two ways:

1. **Frame-based `hello` handshake** (used by `StreamCastClient`): connect with `?waitHello=1` (or server `handshake: 'hello'`) and send a `hello` frame immediately upon socket open:
   ```json
   { "v": 1, "type": "hello" }
   ```
2. **Auto handshake** (convenient for raw `WebSocket` clients): connect directly to `ws://host/path` (or `ws://host/path?sessionId=<id>&lastEventId=<streamId>`).

The server creates `{prefix}:session:<sessionId>` in Redis with a configurable TTL (`sessionTtlMs`) and responds with `welcome`:

```json
{
  "v": 1,
  "type": "welcome",
  "sessionId": "sc_8f42b6a91c0d4e5a92a17f01c8b3e112",
  "lastEventId": "1761546000100-0",
  "recovered": false,
  "rooms": [],
  "reason": "new_session"
}
```

### 2. Live Delivery & Acknowledgement

When any node in the cluster calls `broadcast(room, payload)`, members of `room` receive an `event` frame:

```json
{
  "v": 1,
  "type": "event",
  "id": "1761546000450-0",
  "room": "chat:general",
  "data": { "user": "ada", "text": "Hello cluster!" },
  "ts": 1761546000450
}
```

The client records `lastEventId = frame.id` and acknowledges receipt by sending an `ack` frame:

```json
{
  "v": 1,
  "type": "ack",
  "id": "1761546000450-0"
}
```

On receiving `ack`, the server atomically advances `lastEventId` in `{prefix}:session:<sessionId>` (ignoring out-of-order older IDs) and refreshes the session hash TTL.

### 3. Reconnect & Gapless Replay (`recovered: true`)

After a network drop or server restart, the client reconnects to **any** node in the cluster (no sticky sessions required) and presents its `sessionId` (and optionally its latest locally seen `lastEventId`):

```json
{
  "v": 1,
  "type": "hello",
  "sessionId": "sc_8f42b6a91c0d4e5a92a17f01c8b3e112",
  "lastEventId": "1761546000450-0"
}
```

If the session exists in Redis and `lastEventId` has **not** been trimmed from `{prefix}:events`:

1. The server restores all saved room memberships on the new node while buffering concurrent live deliveries.
2. The server queries `{prefix}:events` with `XRANGE {prefix}:events (<anchorId> +`, filtering entries to the session's rooms and excluding entries whose `except` list contains `sessionId`.
3. The server sends `welcome` (`recovered: true`, `reason: "recovered"`), followed by zero or more `replay` frames in strict Redis Stream ID order, followed by `replay_complete`:

```json
{
  "v": 1,
  "type": "welcome",
  "sessionId": "sc_8f42b6a91c0d4e5a92a17f01c8b3e112",
  "lastEventId": "1761546000990-0",
  "recovered": true,
  "rooms": ["chat:general"],
  "reason": "recovered"
}
```

```json
{
  "v": 1,
  "type": "replay",
  "id": "1761546000700-0",
  "room": "chat:general",
  "data": { "user": "grace", "text": "Missed while offline #1" },
  "ts": 1761546000700
}
```

```json
{
  "v": 1,
  "type": "replay_complete",
  "count": 1,
  "lastEventId": "1761546000990-0"
}
```

4. Any live broadcasts that arrived while `XRANGE` was running (with `id > lastReplayedId`) are flushed immediately after `replay_complete`, guaranteeing zero gaps and zero duplicates.

### 4. Trimmed Stream / Expired Session Fallback (`recovered: false`)

If the client was offline longer than the stream retention window (`MAXLEN`) so `lastEventId` is older than the oldest surviving entry in `{prefix}:events`, the server cannot guarantee lossless replay and signals `recovered: false` with `reason: "stream_trimmed"`:

```json
{
  "v": 1,
  "type": "welcome",
  "sessionId": "sc_8f42b6a91c0d4e5a92a17f01c8b3e112",
  "lastEventId": "1761546099000-0",
  "recovered": false,
  "rooms": ["chat:general"],
  "reason": "stream_trimmed"
}
```

Room memberships are still restored so live events continue flowing while the client performs a full application-level snapshot resync (e.g., via REST/RPC).

If the Redis session hash itself expired (`sessionTtlMs`), the server issues a fresh `sessionId` and sends `welcome` with `recovered: false` and `reason: "session_not_found"`.

---

## Complete Frame Reference

### Client -> Server

| `type`    | Fields                                       | Description                                                  |
| :-------- | :------------------------------------------- | :----------------------------------------------------------- |
| `hello`   | `sessionId?: string`, `lastEventId?: string` | Initiates session creation or reconnect recovery.            |
| `ack`     | `id: string`                                 | Acknowledges receipt of events up through stream ID `id`.    |
| `join`    | `room: string`                               | Requests joining `room` (subject to `allowClientRooms`).     |
| `leave`   | `room: string`                               | Requests leaving `room`.                                     |
| `message` | `data: unknown`, `room?: string`             | Application payload delivered to server `message` listeners. |
| `ping`    | `ts?: number`                                | Application-level keepalive probe.                           |

### Server -> Client

| `type`            | Fields                                                                                                       | Description                                                      |
| :---------------- | :----------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------- |
| `welcome`         | `sessionId: string`, `lastEventId: string`, `recovered: boolean`, `rooms: string[]`, `reason: WelcomeReason` | Handshake response detailing session state and recovery outcome. |
| `event`           | `id: string`, `room: string`, `data: unknown`, `ts: number`                                                  | Live broadcast message delivered to a joined room.               |
| `replay`          | `id: string`, `room: string`, `data: unknown`, `ts: number`                                                  | Missed room message replayed during session recovery.            |
| `replay_complete` | `count: number`, `lastEventId: string`                                                                       | Marks completion of the `replay` burst when `recovered: true`.   |
| `joined`          | `room: string`                                                                                               | Confirms client joined `room`.                                   |
| `left`            | `room: string`                                                                                               | Confirms client left `room`.                                     |
| `pong`            | `ts: number`                                                                                                 | Response to client `ping`.                                       |
| `error`           | `code: string`, `message: string`                                                                            | Protocol validation or authorization error.                      |
