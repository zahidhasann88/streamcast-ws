import Redis, { type RedisOptions } from 'ioredis';

export type RedisInput = Redis | RedisOptions | string;

/**
 * Creates a dedicated ioredis connection configured for resilient stream operations.
 * Setting `maxRetriesPerRequest: null` allows commands and blocking `XREADGROUP` calls
 * to survive transient Redis disconnects without throwing MaxRetriesPerRequestError.
 * `protocol: 2` pins RESP2: stream replies (`XREADGROUP`, `XINFO`) are parsed as RESP2 arrays,
 * and ioredis >= 6 defaults to RESP3.
 */
export function createRedisConnection(input: RedisInput, connectionName: string): Redis {
  const baseOverrides: Partial<RedisOptions> = {
    maxRetriesPerRequest: null,
    protocol: 2,
    enableReadyCheck: true,
    connectionName,
    retryStrategy(times: number): number {
      return Math.min(25 * Math.pow(1.6, Math.min(times, 10)), 2_000);
    },
  };

  if (input instanceof Redis) {
    return input.duplicate(baseOverrides);
  }
  if (typeof input === 'string') {
    return new Redis(input, baseOverrides);
  }
  return new Redis({
    ...input,
    ...baseOverrides,
    retryStrategy: input.retryStrategy ?? baseOverrides.retryStrategy,
  });
}

/**
 * Parses flat RESP2 `[k1, v1, k2, v2, ...]` arrays OR RESP3 plain objects returned
 * by `XINFO STREAM` / `XINFO GROUPS` into a normalized dictionary.
 */
export function parseRedisInfoObject(raw: unknown): Record<string, unknown> {
  if (Array.isArray(raw)) {
    const out: Record<string, unknown> = {};
    for (let i = 0; i + 1 < raw.length; i += 2) {
      const key = raw[i];
      if (typeof key === 'string') {
        out[key] = raw[i + 1];
      }
    }
    return out;
  }
  if (typeof raw === 'object' && raw !== null) {
    return raw as Record<string, unknown>;
  }
  return {};
}

/**
 * Converts a flat Redis Stream field array `[f1, v1, f2, v2, ...]` into a string map.
 */
export function parseStreamFields(fields: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i + 1 < fields.length; i += 2) {
    const key = fields[i];
    const val = fields[i + 1];
    if (key !== undefined && val !== undefined) {
      out[key] = val;
    }
  }
  return out;
}

export interface StreamSummary {
  exists: boolean;
  length: number;
  entriesAdded: number;
  firstEntryId: string | null;
  lastEntryId: string | null;
  lastGeneratedId: string;
}

/**
 * Queries stream head/tail metadata safely via `XINFO STREAM` + `XRANGE`/`XREVRANGE` fallback.
 */
export async function getStreamSummary(redis: Redis, streamKey: string): Promise<StreamSummary> {
  try {
    const rawInfo = await redis.call('XINFO', 'STREAM', streamKey);
    const info = parseRedisInfoObject(rawInfo);

    const length = Number(info['length'] ?? 0);
    const lastGeneratedId =
      typeof info['last-generated-id'] === 'string' ? info['last-generated-id'] : '0-0';
    const entriesAdded =
      info['entries-added'] !== undefined ? Number(info['entries-added']) : length;

    let firstEntryId: string | null = null;
    const rawFirst = info['first-entry'];
    if (Array.isArray(rawFirst) && typeof rawFirst[0] === 'string') {
      firstEntryId = rawFirst[0];
    }

    let lastEntryId: string | null = null;
    const rawLast = info['last-entry'];
    if (Array.isArray(rawLast) && typeof rawLast[0] === 'string') {
      lastEntryId = rawLast[0];
    }

    if (length > 0 && (!firstEntryId || !lastEntryId)) {
      const [firstRange, lastRange] = await Promise.all([
        redis.xrange(streamKey, '-', '+', 'COUNT', 1),
        redis.xrevrange(streamKey, '+', '-', 'COUNT', 1),
      ]);
      firstEntryId = firstRange[0]?.[0] ?? null;
      lastEntryId = lastRange[0]?.[0] ?? null;
    }

    return {
      exists: true,
      length,
      entriesAdded,
      firstEntryId,
      lastEntryId,
      lastGeneratedId,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes('ERR no such key') || msg.includes('NOGROUP')) {
      return {
        exists: false,
        length: 0,
        entriesAdded: 0,
        firstEntryId: null,
        lastEntryId: null,
        lastGeneratedId: '0-0',
      };
    }
    throw err;
  }
}

export function isTransientRedisError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return (
    msg.includes('connection is closed') ||
    msg.includes('econnreset') ||
    msg.includes('econnrefused') ||
    msg.includes('epipe') ||
    msg.includes('etimedout') ||
    msg.includes('socket closed') ||
    msg.includes('loading') ||
    msg.includes('readonly') ||
    msg.includes('unblocked')
  );
}
