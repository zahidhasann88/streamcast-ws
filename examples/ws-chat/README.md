# 2-Node Chat & Session Recovery Demo (`examples/ws-chat`)

Demonstrates cross-process WebSocket room broadcasting and reconnect recovery (without sticky sessions) across two independent Node.js `ws` servers backed by Redis Streams.

> This is a local demo, not a production setup: any client can join any room and broadcast, and there is no authentication. Ports are bound to `127.0.0.1` only.

## Run with Docker Compose

From `examples/ws-chat`:

```bash
docker compose up --build
```

Then open two browser tabs:

- Tab A on Node 1: <http://localhost:4001>
- Tab B on Node 2: <http://localhost:4002>

### Try Cross-Node Fan-Out & Offline Replay

1. Send messages between Tab A (connected to `chat-node-1`) and Tab B (connected to `chat-node-2`).
2. Click **Simulate Offline** in Tab A.
3. Send several messages from Tab B.
4. In Tab A, switch the **Server Node** dropdown to `Node 2 (ws://localhost:4002)` and click **Connect / Switch Node** — even though Tab A reconnects to a completely different server process, its room membership is restored and all missed messages replay in order (`REPLAY` badge).

## Run Locally Without Docker

Assuming `redis-server` is running on `127.0.0.1:6379` and the library has been built (`npm run build` at repo root):

```bash
PORT=4001 NODE_ID=chat-node-1 node examples/ws-chat/server.mjs &
PORT=4002 NODE_ID=chat-node-2 node examples/ws-chat/server.mjs &
```
