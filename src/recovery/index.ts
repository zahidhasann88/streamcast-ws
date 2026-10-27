import { randomUUID } from 'node:crypto';
import type Redis from 'ioredis';
import { getStreamSummary, parseStreamFields, type StreamCast } from '../core/index.js';
import {
  PROTOCOL_VERSION,
  compareStreamIds,
  isValidStreamId,
  maxStreamId,
  type ReplayFrame,
  type WelcomeReason,
} from '../protocol/index.js';

export interface SessionState {
  sessionId: string;
  rooms: string[];
  lastEventId: string;
  createdAt: number;
  updatedAt: number;
}

export interface SessionRecoveryOptions {
  /** Redis command client (or pass `core.getCommandRedis()`). */
  redis: Redis;
  /** Redis key prefix matching `StreamCast.prefix`. Default: `'streamcast'`. */
  prefix?: string;
  /** Session state TTL in milliseconds. Default: `600_000` (10 minutes). */
  sessionTtlMs?: number;
  /** Batch size per `XRANGE` page when replaying missed events. Default: `500`. */
  replayBatchSize?: number;
}

export interface SessionResolution {
  sessionId: string;
  lastEventId: string;
  recovered: boolean;
  reason: WelcomeReason;
  rooms: string[];
  replayedEvents: ReplayFrame[];
}

/**
 * Atomic Lua script to add or remove a room in `{prefix}:session:{id}` JSON `rooms` field
 * and refresh TTL without read-modify-write race conditions.
 */
const UPDATE_ROOMS_LUA = `
local key = KEYS[1]
local op = ARGV[1]
local room = ARGV[2]
local now = ARGV[3]
local ttl = tonumber(ARGV[4])

if redis.call('EXISTS', key) == 0 then
  return 0
end

local rawRooms = redis.call('HGET', key, 'rooms')
local rooms = {}
if rawRooms and rawRooms ~= '' then
  local ok, decoded = pcall(cjson.decode, rawRooms)
  if ok and type(decoded) == 'table' then
    rooms = decoded
  end
end

local nextRooms = {}
local found = false
for i = 1, #rooms do
  local r = rooms[i]
  if r == room then
    found = true
    if op == 'join' then
      table.insert(nextRooms, r)
    end
  else
    table.insert(nextRooms, r)
  end
end

if op == 'join' and not found then
  table.insert(nextRooms, room)
end

local encoded = '[]'
if #nextRooms > 0 then
  encoded = cjson.encode(nextRooms)
end

redis.call('HSET', key, 'rooms', encoded, 'updatedAt', now)
redis.call('PEXPIRE', key, ttl)
return 1
`;

/**
 * Atomic Lua script to monotonically advance `lastEventId` in `{prefix}:session:{id}`
 * and refresh session TTL.
 */
const RECORD_ACK_LUA = `
local key = KEYS[1]
local incoming = ARGV[1]
local now = ARGV[2]
local ttl = tonumber(ARGV[3])

if redis.call('EXISTS', key) == 0 then
  return 0
end

local current = redis.call('HGET', key, 'lastEventId')
local function parse_id(s)
  if not s then return 0, 0 end
  local dash = string.find(s, '-', 1, true)
  if not dash then return 0, 0 end
  return tonumber(string.sub(s, 1, dash - 1)) or 0, tonumber(string.sub(s, dash + 1)) or 0
end

local curMs, curSeq = parse_id(current)
local newMs, newSeq = parse_id(incoming)

if newMs > curMs or (newMs == curMs and newSeq > curSeq) then
  redis.call('HSET', key, 'lastEventId', incoming, 'updatedAt', now)
else
  redis.call('HSET', key, 'updatedAt', now)
end

redis.call('PEXPIRE', key, ttl)
return 1
`;

/**
 * Manages durable WebSocket session state in Redis hashes (`{prefix}:session:{sessionId}`)
 * and replays missed room broadcasts from `{prefix}:events` via `XRANGE` on reconnect.
 */
export class SessionRecovery {
  public readonly prefix: string;
  public readonly streamKey: string;
  public readonly sessionTtlMs: number;
  public readonly replayBatchSize: number;

  private readonly redis: Redis;

  constructor(options: SessionRecoveryOptions) {
    this.redis = options.redis;
    this.prefix = options.prefix ?? 'streamcast';
    this.streamKey = `${this.prefix}:events`;
    this.sessionTtlMs = options.sessionTtlMs ?? 600_000;
    this.replayBatchSize = options.replayBatchSize ?? 500;
  }

  /**
   * Convenience constructor attaching directly to an existing `StreamCast` instance.
   */
  static fromCore(core: StreamCast, sessionTtlMs?: number): SessionRecovery {
    return new SessionRecovery({
      redis: core.getCommandRedis(),
      prefix: core.prefix,
      ...(sessionTtlMs !== undefined ? { sessionTtlMs } : {}),
    });
  }

  sessionKey(sessionId: string): string {
    return `${this.prefix}:session:${sessionId}`;
  }

  /**
   * Reads stored session state from Redis if it has not expired.
   */
  async getSession(sessionId: string): Promise<SessionState | null> {
    const raw = await this.redis.hgetall(this.sessionKey(sessionId));
    if (!raw || Object.keys(raw).length === 0 || !raw['sessionId']) {
      return null;
    }

    let rooms: string[] = [];
    if (raw['rooms']) {
      try {
        const parsed = JSON.parse(raw['rooms']) as unknown;
        if (Array.isArray(parsed)) {
          rooms = parsed.filter((r): r is string => typeof r === 'string');
        }
      } catch {
        rooms = [];
      }
    }

    const lastEventId =
      raw['lastEventId'] && isValidStreamId(raw['lastEventId']) ? raw['lastEventId'] : '0-0';

    return {
      sessionId: raw['sessionId'],
      rooms,
      lastEventId,
      createdAt: Number(raw['createdAt'] ?? Date.now()),
      updatedAt: Number(raw['updatedAt'] ?? Date.now()),
    };
  }

  /**
   * Creates a fresh session hash anchored at the current tail of `{prefix}:events`.
   */
  async createSession(
    reason: 'new_session' | 'session_not_found' = 'new_session',
  ): Promise<SessionResolution> {
    const summary = await getStreamSummary(this.redis, this.streamKey);
    const tailId = summary.lastEntryId ?? summary.lastGeneratedId ?? '0-0';
    const sessionId = `sc_${randomUUID().replace(/-/g, '')}`;
    const now = String(Date.now());
    const key = this.sessionKey(sessionId);

    const pipeline = this.redis.pipeline();
    pipeline.hset(
      key,
      'sessionId',
      sessionId,
      'rooms',
      '[]',
      'lastEventId',
      tailId,
      'createdAt',
      now,
      'updatedAt',
      now,
    );
    pipeline.pexpire(key, this.sessionTtlMs);
    await pipeline.exec();

    return {
      sessionId,
      lastEventId: tailId,
      recovered: false,
      reason,
      rooms: [],
      replayedEvents: [],
    };
  }

  /**
   * Resolves a connecting/reconnecting client:
   * - Issues a new `{ sessionId, lastEventId }` when no `requestedSessionId` is provided.
   * - Issues a new session (`recovered: false, reason: 'session_not_found'`) if the session TTL expired.
   * - Restores rooms and replays missed events with `XRANGE` from `lastEventId` when valid.
   * - Signals `recovered: false, reason: 'stream_trimmed'` if `lastEventId` fell off `{prefix}:events`.
   */
  async resolveSession(params: {
    requestedSessionId?: string;
    requestedLastEventId?: string;
  }): Promise<SessionResolution> {
    if (!params.requestedSessionId) {
      return this.createSession('new_session');
    }

    const existing = await this.getSession(params.requestedSessionId);
    if (!existing) {
      return this.createSession('session_not_found');
    }

    const anchorId =
      params.requestedLastEventId && isValidStreamId(params.requestedLastEventId)
        ? maxStreamId(existing.lastEventId, params.requestedLastEventId)
        : existing.lastEventId;

    if (compareStreamIds(anchorId, existing.lastEventId) > 0) {
      await this.recordAck(existing.sessionId, anchorId);
    } else {
      await this.touchSession(existing.sessionId);
    }

    const summary = await getStreamSummary(this.redis, this.streamKey);
    const currentTailId = summary.lastEntryId ?? summary.lastGeneratedId ?? '0-0';

    const isTrimmed = this.hasAnchorFallenOffStream(anchorId, summary);
    if (isTrimmed) {
      // Advance stored cursor to current stream tail so future events after resync are recoverable.
      const key = this.sessionKey(existing.sessionId);
      const pipeline = this.redis.pipeline();
      pipeline.hset(key, 'lastEventId', currentTailId, 'updatedAt', String(Date.now()));
      pipeline.pexpire(key, this.sessionTtlMs);
      await pipeline.exec();

      return {
        sessionId: existing.sessionId,
        lastEventId: currentTailId,
        recovered: false,
        reason: 'stream_trimmed',
        rooms: existing.rooms,
        replayedEvents: [],
      };
    }

    if (existing.rooms.length === 0 || !summary.exists || summary.length === 0) {
      return {
        sessionId: existing.sessionId,
        lastEventId: maxStreamId(anchorId, currentTailId),
        recovered: true,
        reason: 'recovered',
        rooms: existing.rooms,
        replayedEvents: [],
      };
    }

    const replayedEvents = await this.readMissedEventsForSession(
      existing.sessionId,
      anchorId,
      new Set(existing.rooms),
    );

    const latestId =
      replayedEvents.length > 0
        ? replayedEvents[replayedEvents.length - 1]!.id
        : maxStreamId(anchorId, currentTailId);

    return {
      sessionId: existing.sessionId,
      lastEventId: latestId,
      recovered: true,
      reason: 'recovered',
      rooms: existing.rooms,
      replayedEvents,
    };
  }

  private hasAnchorFallenOffStream(
    anchorId: string,
    summary: {
      exists: boolean;
      length: number;
      entriesAdded: number;
      firstEntryId: string | null;
    },
  ): boolean {
    if (anchorId === '0-0') {
      // Session started before any stream entries existed; if entries were added AND trimmed,
      // `entriesAdded > length` proves head entries were evicted.
      return summary.exists && summary.entriesAdded > summary.length;
    }

    // `anchorId > 0-0` was previously issued from the stream. If the stream is now empty
    // or its oldest surviving entry is strictly newer than `anchorId`, `anchorId` was trimmed.
    if (!summary.exists || summary.length === 0 || !summary.firstEntryId) {
      return true;
    }

    return compareStreamIds(anchorId, summary.firstEntryId) < 0;
  }

  /**
   * Scans `{prefix}:events` strictly after `afterId` (`(${afterId}` to `+`) in batches,
   * filtering to `rooms` and deduplicating by publisher `uid`.
   */
  private async readMissedEventsForSession(
    sessionId: string,
    afterId: string,
    rooms: ReadonlySet<string>,
  ): Promise<ReplayFrame[]> {
    const replayed: ReplayFrame[] = [];
    const seenUids = new Set<string>();
    let cursor = afterId;

    while (true) {
      const batch = await this.redis.xrange(
        this.streamKey,
        `(${cursor}`,
        '+',
        'COUNT',
        this.replayBatchSize,
      );

      if (!batch || batch.length === 0) {
        break;
      }

      for (const [entryId, rawFields] of batch) {
        cursor = entryId;
        const fields = parseStreamFields(rawFields);
        const room = fields['room'];
        if (!room || !rooms.has(room)) {
          continue;
        }

        const uid = fields['uid'] ?? entryId;
        if (seenUids.has(uid)) {
          continue;
        }

        if (fields['except']) {
          try {
            const parsedExcept = JSON.parse(fields['except']) as unknown;
            if (Array.isArray(parsedExcept) && parsedExcept.includes(sessionId)) {
              continue;
            }
          } catch {
            // Ignore malformed except list
          }
        }

        const rawPayload = fields['payload'];
        if (rawPayload === undefined) {
          continue;
        }

        let data: unknown;
        try {
          data = JSON.parse(rawPayload);
        } catch {
          data = rawPayload;
        }

        const ts = fields['ts'] !== undefined ? Number(fields['ts']) : Date.now();
        seenUids.add(uid);

        replayed.push({
          v: PROTOCOL_VERSION,
          type: 'replay',
          id: entryId,
          room,
          data,
          ts,
        });
      }

      if (batch.length < this.replayBatchSize) {
        break;
      }
    }

    return replayed;
  }

  async recordJoin(sessionId: string, room: string): Promise<void> {
    await this.redis.eval(
      UPDATE_ROOMS_LUA,
      1,
      this.sessionKey(sessionId),
      'join',
      room,
      String(Date.now()),
      String(this.sessionTtlMs),
    );
  }

  async recordLeave(sessionId: string, room: string): Promise<void> {
    await this.redis.eval(
      UPDATE_ROOMS_LUA,
      1,
      this.sessionKey(sessionId),
      'leave',
      room,
      String(Date.now()),
      String(this.sessionTtlMs),
    );
  }

  async recordAck(sessionId: string, eventId: string): Promise<void> {
    if (!isValidStreamId(eventId)) {
      throw new Error(`Invalid stream ID in recordAck: "${eventId}"`);
    }
    await this.redis.eval(
      RECORD_ACK_LUA,
      1,
      this.sessionKey(sessionId),
      eventId,
      String(Date.now()),
      String(this.sessionTtlMs),
    );
  }

  async touchSession(sessionId: string): Promise<void> {
    const key = this.sessionKey(sessionId);
    const exists = await this.redis.exists(key);
    if (exists === 1) {
      const pipeline = this.redis.pipeline();
      pipeline.hset(key, 'updatedAt', String(Date.now()));
      pipeline.pexpire(key, this.sessionTtlMs);
      await pipeline.exec();
    }
  }
}
