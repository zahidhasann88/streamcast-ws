# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-28

### Added

- **Core (`StreamCast`)**: Transport-agnostic room fan-out over Redis Streams (`XADD` with `MAXLEN ~ N`, per-node `XREADGROUP` consumer groups, dedicated blocking and command connections, idempotent publisher UIDs, dead-node consumer group & presence sweep via TTL heartbeats, global `roomSize` & `listMembers`).
- **Session Recovery (`SessionRecovery`)**: Durable session state in Redis hashes with configurable TTL, atomic Lua updates for room membership and monotonic `ack` cursors, gapless `XRANGE` missed-message replay filtered by room, and `recovered: false` detection when cursors fall off trimmed streams.
- **Wire Protocol v1 (`docs/PROTOCOL.md`)**: Versioned JSON framing for `hello`, `welcome`, `event`, `ack`, `replay`, `replay_complete`, `join`, `leave`, `ping`, `pong`, and `error`.
- **`ws` Adapter (`createStreamCastServer`)**: Drop-in wrapper around `ws.WebSocketServer` with `bufferedAmount` backpressure policies (`skip` / `close`), room authorization hooks, and cross-node session recovery.
- **Client Helper (`StreamCastClient`)**: Dependency-free browser & Node.js client with exponential backoff + jitter, `sessionId`/`lastEventId` storage, automatic `ack` coalescing, and `resync` event signaling.
- **Socket.io Adapter (`createSocketIoAdapter`, experimental)**: Custom `socket.io-adapter` implementation backed by Redis Streams, imported from `streamcast-ws/socketio` (`socket.io-adapter` is an optional peer dependency).
- **Requirements**: Node.js >= 22, Redis >= 7.0.
- **Benchmarks & Demo**: Cross-node latency/throughput benchmark (`bench/propagation.ts`) and runnable 2-node chat demo (`examples/ws-chat`).
