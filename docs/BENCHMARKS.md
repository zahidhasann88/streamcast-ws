# Benchmarks

Reproduce with `REDIS_URL=redis://127.0.0.1:6379 npm run bench` against a Redis 7+ instance (see [Development](../README.md#development)).

All server nodes, Redis, and WebSocket clients ran on a single machine, so these numbers include no physical network hop. Results depend heavily on hardware and on how Redis is reached; measure on infrastructure that matches your deployment.

## Workloads

1. **Latency**: full path `Node A server.broadcast()` -> Redis `XADD` -> `Node B XREADGROUP` -> `ws` client receipt; `1,000` sequential samples after `100` warmup, ~95 B JSON payload.
2. **Throughput**: 1 publisher node (concurrency 64) broadcasting `5,000` ~120 B messages to `10` WebSocket clients split across 2 remote subscriber nodes (`50,000` deliveries).

## Windows laptop, Redis in Docker Desktop (measured 2026-10-27)

- **Machine**: Windows 10 (x64), Intel Core i7-8665U @ 1.90 GHz (4C/8T)
- **Runtime**: Node.js `v24.15.0`
- **Redis**: `redis:7-alpine` (`v7.4.11`) in Docker Desktop, reached via `127.0.0.1` port-forwarding through the WSL2 VM, so each Redis call crosses a VM boundary. This is noticeably slower than true loopback.

Three consecutive runs:

| Metric            |    Run 1 |    Run 2 |     Run 3 |
| :---------------- | -------: | -------: | --------: |
| **p50**           |  3.22 ms |  2.85 ms |   2.80 ms |
| **mean**          |  4.21 ms |  3.32 ms |   3.23 ms |
| **p95**           |  7.37 ms |  4.49 ms |   4.90 ms |
| **p99**           | 21.87 ms |  9.12 ms |   7.22 ms |
| **min**           |  1.82 ms |  1.91 ms |   1.82 ms |
| **max**           | 88.57 ms | 91.32 ms | 110.63 ms |
| Broadcasts/sec    |    1,410 |    1,472 |     1,385 |
| WS deliveries/sec |   14,104 |   14,719 |    13,850 |

No native-Linux or cross-host numbers have been measured yet.
