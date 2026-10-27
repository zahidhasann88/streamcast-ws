import type { RedisInput } from './redis-utils.js';

export interface StreamCastMessage {
  /** Redis Stream entry ID (`<ms>-<seq>`). */
  id: string;
  /** Unique publisher event token (`<originNodeId>:<seq>`) used for idempotency. */
  uid: string;
  /** Target room name. */
  room: string;
  /** Deserialized application payload. */
  payload: unknown;
  /** Node ID that published this broadcast. */
  originNodeId: string;
  /** Epoch milliseconds when published. */
  ts: number;
  /** Excluded session IDs, if any. */
  except?: string[];
}

export interface SessionTransport {
  /** Deliver a live broadcast event to this local session. */
  send(message: StreamCastMessage): void;
  /** Current outgoing socket buffer size in bytes for backpressure checks. */
  getBufferedAmount?(): number;
  /** Close the underlying socket when backpressure strategy is `'close'`. */
  close?(code?: number, reason?: string): void;
}

export type BackpressureStrategy = 'skip' | 'close';

export interface BackpressureOptions {
  /**
   * Maximum buffered outgoing bytes allowed on a socket before backpressure
   * mitigation triggers. Default: `1_048_576` (1 MiB).
   */
  maxBufferedBytes?: number;
  /**
   * Action taken when `getBufferedAmount() > maxBufferedBytes`:
   * - `'skip'` (default): skip delivering this event to the slow socket; the gap is only recovered if the client reconnects.
   * - `'close'`: close the slow socket (`closeCode`, default `1008`) so it reconnects cleanly.
   */
  strategy?: BackpressureStrategy;
  /** WebSocket close status code used when `strategy === 'close'`. Default: `1008`. */
  closeCode?: number;
  /** Close reason string used when `strategy === 'close'`. */
  closeReason?: string;
}

export interface BroadcastOptions {
  /** Exclude one or more session IDs from receiving this broadcast. */
  except?: string | string[];
}

export type StreamCastErrorPhase =
  | 'redis_command'
  | 'redis_blocking'
  | 'consumer_loop'
  | 'publish'
  | 'local_delivery'
  | 'heartbeat'
  | 'cleanup'
  | 'recovery'
  | 'protocol';

export interface StreamCastErrorContext {
  phase: StreamCastErrorPhase;
  nodeId: string;
  detail?: string;
}

export interface StreamCastMetricsSnapshot {
  eventsPublished: number;
  eventsDelivered: number;
  eventsReplayed: number;
  eventsDeduplicated: number;
  backpressureSkipped: number;
  backpressureClosed: number;
  redisReconnects: number;
  deadNodesCleaned: number;
  lastConsumerLagMs: number;
}

export interface StreamCastMetricsHooks {
  onPublish?: (info: { room: string; streamId: string; nodeId: string }) => void;
  onDeliver?: (info: {
    room: string;
    streamId: string;
    recipientCount: number;
    lagMs: number;
  }) => void;
  onReplay?: (info: { sessionId: string; count: number }) => void;
  onLag?: (lagMs: number, streamId: string) => void;
  onBackpressure?: (info: {
    sessionId: string;
    bufferedBytes: number;
    strategy: BackpressureStrategy;
  }) => void;
}

export interface StreamCastOptions {
  /** Redis URL, ioredis options, or an existing ioredis client to duplicate from. */
  redis: RedisInput;
  /** Unique identifier for this server node. Auto-generated if omitted. */
  nodeId?: string;
  /** Redis key namespace prefix. Default: `'streamcast'`. */
  prefix?: string;
  /** Approximate max entries retained in `{prefix}:events` (`XADD MAXLEN ~ N`). Default: `10_000`. */
  maxStreamLength?: number;
  /**
   * Use Redis approximate trimming (`MAXLEN ~ N`) for O(1) amortized stream trimming.
   * Set `false` for exact `MAXLEN N` trimming (useful in deterministic trim tests).
   * Default: `true`.
   */
  approximateTrim?: boolean;
  /** Blocking timeout in milliseconds per `XREADGROUP` iteration. Default: `2_000`. */
  readBlockMs?: number;
  /** Max entries fetched per `XREADGROUP` batch. Default: `128`. */
  readCount?: number;
  /** Interval in ms between node heartbeat & presence TTL refreshes. Default: `5_000`. */
  heartbeatIntervalMs?: number;
  /** TTL in ms for node heartbeat and per-node presence keys. Default: `15_000`. */
  heartbeatTtlMs?: number;
  /** Interval in ms between sweeps for dead nodes' consumer groups & sets. Default: `15_000`. */
  cleanupIntervalMs?: number;
  /** Number of recent stream IDs / UIDs retained in memory for deduplication. Default: `20_000`. */
  dedupeWindowSize?: number;
  /**
   * Deliver broadcasts originating on this node to local sockets immediately after `XADD`
   * returns (and skip when consumed back on the stream loop). Set `false` to route local
   * deliveries strictly through the consumer loop. Default: `true`.
   */
  immediateLocalDelivery?: boolean;
  /** Max `XADD` retry attempts on transient TCP disconnects. Default: `5`. */
  publishRetryAttempts?: number;
  /** Base backoff in ms for `XADD` retries. Default: `40`. */
  publishRetryBaseDelayMs?: number;
  /** Outgoing socket backpressure policy. */
  backpressure?: BackpressureOptions;
  /** Error callback invoked on recoverable background errors. */
  onError?: (error: Error, context: StreamCastErrorContext) => void;
  /** Optional observability callbacks. */
  metrics?: StreamCastMetricsHooks;
}
