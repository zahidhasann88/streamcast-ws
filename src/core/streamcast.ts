import { randomUUID } from 'node:crypto';
import type Redis from 'ioredis';
import { compareStreamIds } from '../protocol/index.js';
import { BoundedSet } from './dedupe.js';
import { RoomMembership } from './membership.js';
import {
  createRedisConnection,
  getStreamSummary,
  isTransientRedisError,
  parseRedisInfoObject,
  parseStreamFields,
} from './redis-utils.js';
import type {
  BackpressureOptions,
  BroadcastOptions,
  SessionTransport,
  StreamCastErrorContext,
  StreamCastErrorPhase,
  StreamCastMessage,
  StreamCastMetricsHooks,
  StreamCastMetricsSnapshot,
  StreamCastOptions,
} from './types.js';

const CONSUMER_GROUP_PREFIX = 'sc:node:';
const DEFAULT_MAX_BUFFERED_BYTES = 1_048_576; // 1 MiB

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Transport-agnostic room fan-out engine backed by Redis Streams.
 *
 * ### Why Per-Node Consumer Groups (`sc:node:<nodeId>`)?
 * Every server node must receive *all* cluster broadcasts so it can fan out to its local
 * room sockets. Using a single shared consumer group would shard/load-balance messages across
 * nodes instead of broadcasting. Giving each node its own consumer group (`sc:node:<nodeId>`)
 * achieves full cluster fan-out while still leveraging Redis server-side cursor tracking:
 * 1. Pending Entries List (`0`) recovery on transient TCP drops guarantees entries dispatched
 *    into a broken socket right before `ECONNRESET` are drained before resuming live tail (`>`).
 * 2. Dead nodes are self-describing via `XINFO GROUPS {prefix}:events`: any group whose
 *    corresponding `{prefix}:node:<id>:hb` TTL key has expired is destroyed (`XGROUP DESTROY`)
 *    along with its stale presence sets.
 * 3. If a group is ever missing (`NOGROUP`), the loop recreates it from `lastSeenStreamId`
 *    without missing events published during the blip.
 */
export class StreamCast {
  public readonly nodeId: string;
  public readonly prefix: string;
  public readonly streamKey: string;
  public readonly consumerGroup: string;

  private readonly cmdRedis: Redis;
  private readonly subRedis: Redis;
  private readonly membership = new RoomMembership();
  private readonly sessions = new Map<string, SessionTransport>();

  private readonly processedStreamIds: BoundedSet<string>;
  private readonly processedUids: BoundedSet<string>;

  private readonly maxStreamLength: number;
  private readonly approximateTrim: boolean;
  private readonly readBlockMs: number;
  private readonly readCount: number;
  private readonly heartbeatIntervalMs: number;
  private readonly heartbeatTtlMs: number;
  private readonly cleanupIntervalMs: number;
  private readonly immediateLocalDelivery: boolean;
  private readonly publishRetryAttempts: number;
  private readonly publishRetryBaseDelayMs: number;
  private readonly backpressure: Required<BackpressureOptions>;
  private readonly onError?: (error: Error, context: StreamCastErrorContext) => void;
  private readonly metricsHooks?: StreamCastMetricsHooks;

  private readonly metricsState: StreamCastMetricsSnapshot = {
    eventsPublished: 0,
    eventsDelivered: 0,
    eventsReplayed: 0,
    eventsDeduplicated: 0,
    backpressureSkipped: 0,
    backpressureClosed: 0,
    redisReconnects: 0,
    deadNodesCleaned: 0,
    lastConsumerLagMs: 0,
  };

  private publishSeq = 0;
  private lastSeenStreamId = '0-0';
  private subClientId: number | null = null;
  private needsPelDrain = false;

  private startPromise: Promise<void> | null = null;
  private loopPromise: Promise<void> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;
  private running = false;
  private closing = false;
  private closed = false;

  constructor(options: StreamCastOptions) {
    this.nodeId = options.nodeId ?? `node-${randomUUID().replace(/-/g, '').slice(0, 12)}`;
    this.prefix = options.prefix ?? 'streamcast';
    this.streamKey = `${this.prefix}:events`;
    this.consumerGroup = `${CONSUMER_GROUP_PREFIX}${this.nodeId}`;

    this.maxStreamLength = options.maxStreamLength ?? 10_000;
    this.approximateTrim = options.approximateTrim ?? true;
    this.readBlockMs = options.readBlockMs ?? 2_000;
    this.readCount = options.readCount ?? 128;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5_000;
    this.heartbeatTtlMs = options.heartbeatTtlMs ?? 15_000;
    this.cleanupIntervalMs = options.cleanupIntervalMs ?? 15_000;
    this.immediateLocalDelivery = options.immediateLocalDelivery ?? true;
    this.publishRetryAttempts = options.publishRetryAttempts ?? 5;
    this.publishRetryBaseDelayMs = options.publishRetryBaseDelayMs ?? 40;

    this.backpressure = {
      maxBufferedBytes: options.backpressure?.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
      strategy: options.backpressure?.strategy ?? 'skip',
      closeCode: options.backpressure?.closeCode ?? 1008,
      closeReason: options.backpressure?.closeReason ?? 'Backpressure limit exceeded',
    };

    this.onError = options.onError;
    this.metricsHooks = options.metrics;

    const dedupeSize = options.dedupeWindowSize ?? 20_000;
    this.processedStreamIds = new BoundedSet<string>(dedupeSize);
    this.processedUids = new BoundedSet<string>(dedupeSize);

    this.cmdRedis = createRedisConnection(options.redis, `${this.prefix}:${this.nodeId}:cmd`);
    this.subRedis = createRedisConnection(options.redis, `${this.prefix}:${this.nodeId}:sub`);

    this.attachRedisListeners();
  }

  private attachRedisListeners(): void {
    this.cmdRedis.on('error', (err) => {
      if (!this.closing && !this.closed) {
        this.emitError(err, 'redis_command');
      }
    });

    this.subRedis.on('error', (err) => {
      if (!this.closing && !this.closed) {
        this.emitError(err, 'redis_blocking');
      }
    });

    this.subRedis.on('reconnecting', () => {
      if (!this.closing && !this.closed) {
        this.metricsState.redisReconnects += 1;
        this.needsPelDrain = true;
      }
    });

    this.subRedis.on('ready', () => {
      if (!this.closing && !this.closed) {
        void this.refreshSubClientId();
      }
    });
  }

  private emitError(err: unknown, phase: StreamCastErrorPhase, detail?: string): void {
    const error = err instanceof Error ? err : new Error(String(err));
    const ctx: StreamCastErrorContext = { phase, nodeId: this.nodeId };
    if (detail !== undefined) {
      ctx.detail = detail;
    }
    this.onError?.(error, ctx);
  }

  private async refreshSubClientId(): Promise<void> {
    try {
      const id = await this.subRedis.call('CLIENT', 'ID');
      if (typeof id === 'number') {
        this.subClientId = id;
      }
    } catch {
      // Ignore if connection is mid-transition
    }
  }

  private get nodesSetKey(): string {
    return `${this.prefix}:nodes`;
  }

  private nodeHeartbeatKey(nodeId: string): string {
    return `${this.prefix}:node:${nodeId}:hb`;
  }

  private nodeRoomsKey(nodeId: string): string {
    return `${this.prefix}:node:${nodeId}:rooms`;
  }

  private nodeRoomMembersKey(nodeId: string, room: string): string {
    return `${this.prefix}:node:${nodeId}:room:${room}`;
  }

  /**
   * Starts the consumer group, node heartbeat, dead-node sweeper, and blocking read loop.
   * Safe to call multiple times; subsequent calls return the same startup promise.
   */
  async start(): Promise<void> {
    if (this.closed) {
      throw new Error(`StreamCast node "${this.nodeId}" has already been closed`);
    }
    if (this.startPromise) {
      return this.startPromise;
    }
    this.startPromise = this.doStart();
    return this.startPromise;
  }

  /**
   * Alias for `start()` so callers can `await streamCast.ready()`.
   */
  async ready(): Promise<void> {
    return this.start();
  }

  private async doStart(): Promise<void> {
    await this.refreshSubClientId();

    const summary = await getStreamSummary(this.cmdRedis, this.streamKey);
    if (summary.lastEntryId) {
      this.lastSeenStreamId = summary.lastEntryId;
    } else if (summary.lastGeneratedId !== '0-0') {
      this.lastSeenStreamId = summary.lastGeneratedId;
    }

    await this.ensureConsumerGroup('$');
    await this.writeHeartbeat();

    this.running = true;
    this.needsPelDrain = true;

    this.heartbeatTimer = setInterval(() => {
      void this.writeHeartbeat().catch((err) => this.emitError(err, 'heartbeat'));
    }, this.heartbeatIntervalMs);
    this.heartbeatTimer.unref?.();

    this.cleanupTimer = setInterval(() => {
      void this.cleanupDeadNodes().catch((err) => this.emitError(err, 'cleanup'));
    }, this.cleanupIntervalMs);
    this.cleanupTimer.unref?.();

    this.loopPromise = this.runConsumerLoop();
  }

  private async ensureConsumerGroup(startId: string): Promise<void> {
    try {
      await this.cmdRedis.xgroup('CREATE', this.streamKey, this.consumerGroup, startId, 'MKSTREAM');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  /**
   * Gracefully stops the consumer loop, removes this node's consumer group, heartbeat,
   * and presence keys, and closes owned Redis connections.
   */
  async close(): Promise<void> {
    if (this.closed || this.closing) {
      if (this.loopPromise) {
        await this.loopPromise.catch(() => undefined);
      }
      return;
    }

    this.closing = true;
    this.running = false;

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }

    // Wake up the blocking XREADGROUP immediately via CLIENT UNBLOCK so shutdown is instant.
    if (this.subClientId !== null) {
      try {
        await this.cmdRedis.call('CLIENT', 'UNBLOCK', this.subClientId);
      } catch {
        // Ignore if already disconnected
      }
    }

    if (this.loopPromise) {
      await Promise.race([this.loopPromise.catch(() => undefined), sleep(this.readBlockMs + 250)]);
    }

    try {
      const activeRooms = this.membership.getActiveRooms();
      const pipeline = this.cmdRedis.pipeline();
      for (const room of activeRooms) {
        pipeline.del(this.nodeRoomMembersKey(this.nodeId, room));
      }
      pipeline.del(this.nodeRoomsKey(this.nodeId));
      pipeline.del(this.nodeHeartbeatKey(this.nodeId));
      pipeline.srem(this.nodesSetKey, this.nodeId);
      await pipeline.exec();

      await this.cmdRedis
        .xgroup('DESTROY', this.streamKey, this.consumerGroup)
        .catch(() => undefined);
    } catch (err) {
      this.emitError(err, 'cleanup', 'Error during graceful shutdown cleanup');
    }

    this.membership.clear();
    this.sessions.clear();

    await Promise.allSettled([
      this.subRedis.quit().catch(() => this.subRedis.disconnect()),
      this.cmdRedis.quit().catch(() => this.cmdRedis.disconnect()),
    ]);

    this.closed = true;
  }

  /**
   * Registers a local transport callback for `sessionId` so incoming broadcasts to rooms
   * containing `sessionId` can be delivered on this node.
   */
  registerSession(sessionId: string, transport: SessionTransport): void {
    this.sessions.set(sessionId, transport);
  }

  /**
   * Unregisters a local session transport and removes it from all rooms on this node.
   */
  async unregisterSession(sessionId: string): Promise<void> {
    this.sessions.delete(sessionId);
    await this.leaveAll(sessionId);
  }

  hasLocalSession(sessionId: string): boolean {
    return this.sessions.has(sessionId);
  }

  /**
   * Adds `sessionId` to `room` on this node and updates global Redis presence.
   */
  async join(sessionId: string, room: string): Promise<void> {
    await this.start();
    this.membership.join(sessionId, room);

    const pipeline = this.cmdRedis.pipeline();
    pipeline.sadd(this.nodeRoomsKey(this.nodeId), room);
    pipeline.pexpire(this.nodeRoomsKey(this.nodeId), this.heartbeatTtlMs);
    pipeline.sadd(this.nodeRoomMembersKey(this.nodeId, room), sessionId);
    pipeline.pexpire(this.nodeRoomMembersKey(this.nodeId, room), this.heartbeatTtlMs);
    await pipeline.exec();
  }

  /**
   * Removes `sessionId` from `room` on this node and updates global Redis presence.
   */
  async leave(sessionId: string, room: string): Promise<void> {
    await this.start();
    this.membership.leave(sessionId, room);

    const localRemaining = this.membership.localRoomSize(room);
    const pipeline = this.cmdRedis.pipeline();
    if (localRemaining === 0) {
      pipeline.del(this.nodeRoomMembersKey(this.nodeId, room));
      pipeline.srem(this.nodeRoomsKey(this.nodeId), room);
    } else {
      pipeline.srem(this.nodeRoomMembersKey(this.nodeId, room), sessionId);
    }
    await pipeline.exec();
  }

  /**
   * Removes `sessionId` from all rooms on this node and updates global Redis presence.
   */
  async leaveAll(sessionId: string): Promise<string[]> {
    const leftRooms = this.membership.leaveAll(sessionId);
    if (leftRooms.length === 0 || this.closed || this.closing) {
      return leftRooms;
    }

    const pipeline = this.cmdRedis.pipeline();
    for (const room of leftRooms) {
      if (this.membership.localRoomSize(room) === 0) {
        pipeline.del(this.nodeRoomMembersKey(this.nodeId, room));
        pipeline.srem(this.nodeRoomsKey(this.nodeId), room);
      } else {
        pipeline.srem(this.nodeRoomMembersKey(this.nodeId, room), sessionId);
      }
    }
    await pipeline.exec();
    return leftRooms;
  }

  /**
   * Returns the rooms `sessionId` has joined on this node.
   */
  getSessionRooms(sessionId: string): ReadonlySet<string> {
    return this.membership.getSessionRooms(sessionId);
  }

  /**
   * Returns the local session IDs currently in `room` on this node.
   */
  getLocalRoomMembers(room: string): ReadonlySet<string> {
    return this.membership.getRoomMembers(room);
  }

  /**
   * Returns all globally active member session IDs in `room` across all live nodes
   * (nodes whose heartbeat key has not expired).
   */
  async listMembers(room: string): Promise<string[]> {
    await this.start();
    const candidateNodes = await this.cmdRedis.smembers(this.nodesSetKey);
    if (candidateNodes.length === 0) {
      return [];
    }

    const pipeline = this.cmdRedis.pipeline();
    for (const nodeId of candidateNodes) {
      pipeline.exists(this.nodeHeartbeatKey(nodeId));
      pipeline.smembers(this.nodeRoomMembersKey(nodeId, room));
    }
    const results = await pipeline.exec();
    if (!results) {
      return [];
    }

    const members = new Set<string>();
    for (let i = 0; i + 1 < results.length; i += 2) {
      const hbEntry = results[i];
      const membersEntry = results[i + 1];
      const isAlive = hbEntry && hbEntry[0] === null && Number(hbEntry[1]) === 1;
      if (isAlive && membersEntry && membersEntry[0] === null && Array.isArray(membersEntry[1])) {
        for (const member of membersEntry[1]) {
          if (typeof member === 'string') {
            members.add(member);
          }
        }
      }
    }
    return Array.from(members);
  }

  /**
   * Returns the global count of unique active session IDs in `room` across all live nodes.
   */
  async roomSize(room: string): Promise<number> {
    const members = await this.listMembers(room);
    return members.length;
  }

  /**
   * Publishes `payload` to `{prefix}:events` via `XADD` with `MAXLEN` retention and fans out
   * to all nodes in the cluster. Returns the assigned Redis Stream entry ID.
   */
  async broadcast(room: string, payload: unknown, options?: BroadcastOptions): Promise<string> {
    await this.start();

    this.publishSeq += 1;
    const uid = `${this.nodeId}:${this.publishSeq}`;
    const ts = Date.now();
    const serializedPayload = JSON.stringify(payload ?? null);

    const exceptList =
      options?.except === undefined
        ? []
        : Array.isArray(options.except)
          ? options.except
          : [options.except];

    const args: (string | number)[] = [
      this.streamKey,
      'MAXLEN',
      ...(this.approximateTrim ? ['~'] : []),
      this.maxStreamLength,
      '*',
      'v',
      '1',
      'uid',
      uid,
      'node',
      this.nodeId,
      'room',
      room,
      'payload',
      serializedPayload,
      'ts',
      String(ts),
    ];
    if (exceptList.length > 0) {
      args.push('except', JSON.stringify(exceptList));
    }

    const streamId = await this.xaddWithRetry(args);

    this.metricsState.eventsPublished += 1;
    this.metricsHooks?.onPublish?.({ room, streamId, nodeId: this.nodeId });

    if (this.immediateLocalDelivery) {
      // Only deliver locally if the consumer loop hasn't already beaten us during a retry.
      if (this.processedUids.add(uid)) {
        this.processedStreamIds.add(streamId);
        const message: StreamCastMessage = {
          id: streamId,
          uid,
          room,
          payload,
          originNodeId: this.nodeId,
          ts,
          ...(exceptList.length > 0 ? { except: exceptList } : {}),
        };
        this.deliverLocally(message);
      }
    }

    return streamId;
  }

  private async xaddWithRetry(args: (string | number)[]): Promise<string> {
    let attempt = 0;
    while (true) {
      try {
        const result = await this.cmdRedis.call('XADD', ...args);
        if (typeof result !== 'string') {
          throw new Error(`Unexpected XADD response: ${String(result)}`);
        }
        return result;
      } catch (err) {
        attempt += 1;
        if (attempt > this.publishRetryAttempts || !isTransientRedisError(err) || this.closing) {
          this.emitError(err, 'publish');
          throw err;
        }
        const backoff = Math.min(this.publishRetryBaseDelayMs * Math.pow(2, attempt - 1), 1_000);
        await sleep(backoff);
      }
    }
  }

  private deliverLocally(message: StreamCastMessage): void {
    const roomMembers = this.membership.getRoomMembers(message.room);
    if (roomMembers.size === 0) {
      return;
    }

    const exceptSet = message.except && message.except.length > 0 ? new Set(message.except) : null;

    let deliveredCount = 0;
    // Snapshot members in case a backpressure close removes a session mid-iteration.
    for (const sessionId of Array.from(roomMembers)) {
      if (exceptSet?.has(sessionId)) {
        continue;
      }
      const transport = this.sessions.get(sessionId);
      if (!transport) {
        continue;
      }

      if (transport.getBufferedAmount) {
        const bufferedBytes = transport.getBufferedAmount();
        if (bufferedBytes > this.backpressure.maxBufferedBytes) {
          this.metricsHooks?.onBackpressure?.({
            sessionId,
            bufferedBytes,
            strategy: this.backpressure.strategy,
          });
          if (this.backpressure.strategy === 'close') {
            this.metricsState.backpressureClosed += 1;
            try {
              transport.close?.(this.backpressure.closeCode, this.backpressure.closeReason);
            } catch (err) {
              this.emitError(err, 'local_delivery', `Failed closing slow session ${sessionId}`);
            }
          } else {
            this.metricsState.backpressureSkipped += 1;
          }
          continue;
        }
      }

      try {
        transport.send(message);
        deliveredCount += 1;
        this.metricsState.eventsDelivered += 1;
      } catch (err) {
        this.emitError(err, 'local_delivery', `Failed delivering to session ${sessionId}`);
      }
    }

    const lagMs = Math.max(0, Date.now() - message.ts);
    this.metricsState.lastConsumerLagMs = lagMs;
    this.metricsHooks?.onDeliver?.({
      room: message.room,
      streamId: message.id,
      recipientCount: deliveredCount,
      lagMs,
    });
  }

  private async runConsumerLoop(): Promise<void> {
    let consecutiveErrors = 0;

    while (this.running && !this.closing) {
      try {
        if (this.needsPelDrain) {
          // Cleared first so a reconnect during the drain schedules another one.
          this.needsPelDrain = false;
          await this.drainPendingEntries();
        }

        const rawResponse = (await this.subRedis.call(
          'XREADGROUP',
          'GROUP',
          this.consumerGroup,
          this.nodeId,
          'COUNT',
          String(this.readCount),
          'BLOCK',
          String(this.readBlockMs),
          'STREAMS',
          this.streamKey,
          '>',
        )) as [string, [string, string[]][]][] | null;

        consecutiveErrors = 0;

        // The connection dropped while this read was in flight and ioredis resent it after
        // reconnecting, so entries Redis delivered on the dead socket may precede this batch.
        // Everything is still pending (unacked), so the drain re-reads it all in stream order.
        if (this.needsPelDrain) {
          continue;
        }

        if (!rawResponse || rawResponse.length === 0) {
          continue;
        }

        for (const streamBatch of rawResponse) {
          const entries = streamBatch[1];
          if (entries && entries.length > 0) {
            await this.processStreamEntries(entries);
          }
        }
      } catch (err) {
        if (this.closing || !this.running) {
          break;
        }
        const msg = err instanceof Error ? err.message : String(err);

        // Client was explicitly unblocked during shutdown or manual interrupt.
        if (msg.includes('UNBLOCKED')) {
          continue;
        }

        // Group disappeared (e.g., stream flushed or Redis failover): recreate from lastSeenStreamId.
        if (msg.includes('NOGROUP')) {
          try {
            await this.ensureConsumerGroup(this.lastSeenStreamId);
            this.needsPelDrain = true;
            continue;
          } catch (recreateErr) {
            this.emitError(recreateErr, 'consumer_loop', 'Failed recreating consumer group');
          }
        } else {
          this.emitError(err, 'consumer_loop');
        }

        this.needsPelDrain = true;
        consecutiveErrors += 1;
        const backoff = Math.min(50 * Math.pow(1.8, Math.min(consecutiveErrors, 8)), 1_500);
        await sleep(backoff);
      }
    }
  }

  /**
   * Drains any pending entries (`0`) that were dispatched to this node's consumer group
   * before a transient socket disconnect, ensuring zero lost events on Redis blips.
   */
  private async drainPendingEntries(): Promise<void> {
    while (this.running && !this.closing) {
      const rawPending = (await this.subRedis.call(
        'XREADGROUP',
        'GROUP',
        this.consumerGroup,
        this.nodeId,
        'COUNT',
        String(this.readCount),
        'STREAMS',
        this.streamKey,
        '0',
      )) as [string, [string, string[]][]][] | null;

      if (!rawPending || rawPending.length === 0) {
        break;
      }

      const entries = rawPending[0]?.[1];
      if (!entries || entries.length === 0) {
        break;
      }

      await this.processStreamEntries(entries);
    }
  }

  private async processStreamEntries(entries: [string, string[]][]): Promise<void> {
    const idsToAck: string[] = [];

    for (const [entryId, rawFields] of entries) {
      idsToAck.push(entryId);
      if (compareStreamIds(entryId, this.lastSeenStreamId) > 0) {
        this.lastSeenStreamId = entryId;
      }

      if (!this.processedStreamIds.add(entryId)) {
        this.metricsState.eventsDeduplicated += 1;
        continue;
      }

      const fields = parseStreamFields(rawFields);
      const uid = fields['uid'] ?? entryId;
      const originNodeId = fields['node'] ?? '';
      const room = fields['room'];
      const rawPayload = fields['payload'];
      const ts = fields['ts'] !== undefined ? Number(fields['ts']) : Date.now();

      const lagMs = Math.max(0, Date.now() - ts);
      this.metricsState.lastConsumerLagMs = lagMs;
      this.metricsHooks?.onLag?.(lagMs, entryId);

      // Deduplicate by publisher UID (also skips origin-node messages already delivered locally).
      if (!this.processedUids.add(uid)) {
        if (originNodeId !== this.nodeId) {
          this.metricsState.eventsDeduplicated += 1;
        }
        continue;
      }

      if (!room || rawPayload === undefined) {
        continue;
      }

      let payload: unknown;
      try {
        payload = JSON.parse(rawPayload);
      } catch {
        payload = rawPayload;
      }

      let except: string[] | undefined;
      if (fields['except']) {
        try {
          const parsedExcept = JSON.parse(fields['except']) as unknown;
          if (Array.isArray(parsedExcept)) {
            except = parsedExcept.filter((v): v is string => typeof v === 'string');
          }
        } catch {
          // Ignore malformed except field
        }
      }

      const message: StreamCastMessage = {
        id: entryId,
        uid,
        room,
        payload,
        originNodeId,
        ts,
        ...(except && except.length > 0 ? { except } : {}),
      };

      this.deliverLocally(message);
    }

    if (idsToAck.length > 0 && !this.closing) {
      try {
        await this.cmdRedis.xack(this.streamKey, this.consumerGroup, ...idsToAck);
      } catch (err) {
        // Non-fatal: dedupe sets protect against redelivery if XACK fails during a blip.
        if (!this.closing) {
          this.emitError(err, 'consumer_loop', 'XACK failed');
        }
      }
    }
  }

  /**
   * Refreshes this node's heartbeat key and active room presence TTLs in Redis.
   */
  async writeHeartbeat(): Promise<void> {
    if (this.closing || this.closed) return;

    const activeRooms = this.membership.getActiveRooms();
    const pipeline = this.cmdRedis.pipeline();
    pipeline.sadd(this.nodesSetKey, this.nodeId);
    pipeline.set(this.nodeHeartbeatKey(this.nodeId), String(Date.now()), 'PX', this.heartbeatTtlMs);

    if (activeRooms.length > 0) {
      pipeline.sadd(this.nodeRoomsKey(this.nodeId), ...activeRooms);
      pipeline.pexpire(this.nodeRoomsKey(this.nodeId), this.heartbeatTtlMs);
      for (const room of activeRooms) {
        const localMembers = Array.from(this.membership.getRoomMembers(room));
        if (localMembers.length > 0) {
          pipeline.sadd(this.nodeRoomMembersKey(this.nodeId, room), ...localMembers);
          pipeline.pexpire(this.nodeRoomMembersKey(this.nodeId, room), this.heartbeatTtlMs);
        }
      }
    }

    await pipeline.exec();
  }

  /**
   * Inspects `XINFO GROUPS {prefix}:events` and `{prefix}:nodes`, destroying consumer groups
   * and stale presence keys belonging to dead nodes whose heartbeat TTL has expired.
   * Returns the list of dead node IDs cleaned up during this sweep.
   */
  async cleanupDeadNodes(): Promise<string[]> {
    if (this.closing || this.closed) return [];

    const candidateNodeIds = new Set<string>();

    const registeredNodes = await this.cmdRedis.smembers(this.nodesSetKey);
    for (const id of registeredNodes) {
      if (id !== this.nodeId) {
        candidateNodeIds.add(id);
      }
    }

    try {
      const rawGroups = await this.cmdRedis.call('XINFO', 'GROUPS', this.streamKey);
      if (Array.isArray(rawGroups)) {
        for (const rawGroup of rawGroups) {
          const info = parseRedisInfoObject(rawGroup);
          const groupName = info['name'];
          if (typeof groupName === 'string' && groupName.startsWith(CONSUMER_GROUP_PREFIX)) {
            const groupNodeId = groupName.slice(CONSUMER_GROUP_PREFIX.length);
            if (groupNodeId && groupNodeId !== this.nodeId) {
              candidateNodeIds.add(groupNodeId);
            }
          }
        }
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('ERR no such key')) {
        throw err;
      }
    }

    if (candidateNodeIds.size === 0) {
      return [];
    }

    const candidates = Array.from(candidateNodeIds);
    const hbPipeline = this.cmdRedis.pipeline();
    for (const id of candidates) {
      hbPipeline.exists(this.nodeHeartbeatKey(id));
    }
    const hbResults = await hbPipeline.exec();
    if (!hbResults) {
      return [];
    }

    const cleaned: string[] = [];
    for (let i = 0; i < candidates.length; i++) {
      const targetId = candidates[i];
      const res = hbResults[i];
      if (!targetId || !res || res[0] !== null) continue;

      const isAlive = Number(res[1]) === 1;
      if (!isAlive) {
        await this.purgeDeadNode(targetId);
        cleaned.push(targetId);
        this.metricsState.deadNodesCleaned += 1;
      }
    }

    return cleaned;
  }

  private async purgeDeadNode(deadNodeId: string): Promise<void> {
    const staleRooms = await this.cmdRedis.smembers(this.nodeRoomsKey(deadNodeId));
    const pipeline = this.cmdRedis.pipeline();
    for (const room of staleRooms) {
      pipeline.del(this.nodeRoomMembersKey(deadNodeId, room));
    }
    pipeline.del(this.nodeRoomsKey(deadNodeId));
    pipeline.srem(this.nodesSetKey, deadNodeId);
    await pipeline.exec();

    await this.cmdRedis
      .xgroup('DESTROY', this.streamKey, `${CONSUMER_GROUP_PREFIX}${deadNodeId}`)
      .catch(() => undefined);
  }

  recordReplayMetric(sessionId: string, count: number): void {
    this.metricsState.eventsReplayed += count;
    this.metricsHooks?.onReplay?.({ sessionId, count });
  }

  getMetrics(): StreamCastMetricsSnapshot {
    return { ...this.metricsState };
  }

  getLastSeenStreamId(): string {
    return this.lastSeenStreamId;
  }

  /**
   * Exposes the non-blocking command connection for companion modules (`SessionRecovery`).
   */
  getCommandRedis(): Redis {
    return this.cmdRedis;
  }

  /**
   * Exposes the dedicated blocking connection (useful for diagnostics and chaos testing).
   */
  getBlockingRedis(): Redis {
    return this.subRedis;
  }
}
