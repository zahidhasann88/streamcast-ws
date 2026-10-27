/**
 * Wire protocol v1 definitions, validators, and Redis Stream ID helpers.
 */

export const PROTOCOL_VERSION = 1 as const;

export type WelcomeReason = 'new_session' | 'recovered' | 'session_not_found' | 'stream_trimmed';

export interface HelloFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'hello';
  sessionId?: string;
  lastEventId?: string;
}

export interface AckFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'ack';
  id: string;
}

export interface JoinFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'join';
  room: string;
}

export interface LeaveFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'leave';
  room: string;
}

export interface ClientMessageFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'message';
  room?: string;
  data: unknown;
}

export interface PingFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'ping';
  ts?: number;
}

export type ClientFrame =
  HelloFrame | AckFrame | JoinFrame | LeaveFrame | ClientMessageFrame | PingFrame;

export interface WelcomeFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'welcome';
  sessionId: string;
  lastEventId: string;
  recovered: boolean;
  rooms: string[];
  reason: WelcomeReason;
}

export interface EventFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'event';
  id: string;
  room: string;
  data: unknown;
  ts: number;
}

export interface ReplayFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'replay';
  id: string;
  room: string;
  data: unknown;
  ts: number;
}

export interface ReplayCompleteFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'replay_complete';
  count: number;
  lastEventId: string;
}

export interface JoinedFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'joined';
  room: string;
}

export interface LeftFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'left';
  room: string;
}

export interface PongFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'pong';
  ts: number;
}

export interface ErrorFrame {
  v: typeof PROTOCOL_VERSION;
  type: 'error';
  code: string;
  message: string;
}

export type ServerFrame =
  | WelcomeFrame
  | EventFrame
  | ReplayFrame
  | ReplayCompleteFrame
  | JoinedFrame
  | LeftFrame
  | PongFrame
  | ErrorFrame;

export type ProtocolFrame = ClientFrame | ServerFrame;

export type DecodeResult<T> =
  | { ok: true; frame: T }
  | { ok: false; code: string; error: string; isNonProtocolJson?: boolean; parsed?: unknown };

const STREAM_ID_REGEX = /^(\d+)-(\d+)$/;

/**
 * Checks whether a string is a valid canonical Redis Stream entry ID (`<ms>-<seq>`).
 */
export function isValidStreamId(id: string): boolean {
  return STREAM_ID_REGEX.test(id);
}

/**
 * Parses a Redis Stream entry ID (`<ms>-<seq>`) into 64-bit safe BigInt components.
 */
export function parseStreamId(id: string): { ms: bigint; seq: bigint } {
  const match = STREAM_ID_REGEX.exec(id);
  if (!match || match[1] === undefined || match[2] === undefined) {
    throw new Error(`Invalid Redis Stream ID: "${id}"`);
  }
  return {
    ms: BigInt(match[1]),
    seq: BigInt(match[2]),
  };
}

/**
 * Compares two Redis Stream IDs numerically.
 * Returns `-1` if `a < b`, `0` if `a === b`, `1` if `a > b`.
 */
export function compareStreamIds(a: string, b: string): -1 | 0 | 1 {
  if (a === b) return 0;
  const pa = parseStreamId(a);
  const pb = parseStreamId(b);
  if (pa.ms < pb.ms) return -1;
  if (pa.ms > pb.ms) return 1;
  if (pa.seq < pb.seq) return -1;
  if (pa.seq > pb.seq) return 1;
  return 0;
}

/**
 * Returns whichever Redis Stream ID is greater.
 */
export function maxStreamId(a: string, b: string): string {
  return compareStreamIds(a, b) >= 0 ? a : b;
}

/**
 * Extracts the millisecond timestamp prefix from a Redis Stream ID.
 */
export function streamIdTimestampMs(id: string): number {
  const { ms } = parseStreamId(id);
  return Number(ms);
}

export function encodeFrame(frame: ProtocolFrame): string {
  return JSON.stringify(frame);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function decodeClientFrame(raw: string): DecodeResult<ClientFrame> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false, code: 'INVALID_JSON', error: 'Frame must be valid JSON' };
  }

  if (!isRecord(parsed)) {
    return { ok: false, code: 'INVALID_FRAME', error: 'Frame must be a JSON object', parsed };
  }

  if (!('v' in parsed) || !('type' in parsed)) {
    return {
      ok: false,
      code: 'NOT_PROTOCOL_FRAME',
      error: 'Object does not contain protocol envelope fields (v, type)',
      isNonProtocolJson: true,
      parsed,
    };
  }

  if (parsed.v !== PROTOCOL_VERSION) {
    return {
      ok: false,
      code: 'UNSUPPORTED_VERSION',
      error: `Unsupported protocol version: ${String(parsed.v)} (expected ${PROTOCOL_VERSION})`,
    };
  }

  const type = parsed.type;
  if (typeof type !== 'string') {
    return { ok: false, code: 'INVALID_TYPE', error: 'Frame type must be a string' };
  }

  switch (type) {
    case 'hello': {
      if (parsed.sessionId !== undefined && typeof parsed.sessionId !== 'string') {
        return { ok: false, code: 'INVALID_HELLO', error: 'hello.sessionId must be a string' };
      }
      if (parsed.lastEventId !== undefined) {
        if (typeof parsed.lastEventId !== 'string' || !isValidStreamId(parsed.lastEventId)) {
          return {
            ok: false,
            code: 'INVALID_HELLO',
            error: 'hello.lastEventId must be a valid stream ID (<ms>-<seq>)',
          };
        }
      }
      const frame: HelloFrame = { v: PROTOCOL_VERSION, type: 'hello' };
      if (parsed.sessionId !== undefined && parsed.sessionId.length > 0) {
        frame.sessionId = parsed.sessionId;
      }
      if (parsed.lastEventId !== undefined) {
        frame.lastEventId = parsed.lastEventId;
      }
      return { ok: true, frame };
    }

    case 'ack': {
      if (typeof parsed.id !== 'string' || !isValidStreamId(parsed.id)) {
        return {
          ok: false,
          code: 'INVALID_ACK',
          error: 'ack.id must be a valid stream ID (<ms>-<seq>)',
        };
      }
      return { ok: true, frame: { v: PROTOCOL_VERSION, type: 'ack', id: parsed.id } };
    }

    case 'join': {
      if (typeof parsed.room !== 'string' || parsed.room.trim().length === 0) {
        return { ok: false, code: 'INVALID_ROOM', error: 'join.room must be a non-empty string' };
      }
      return { ok: true, frame: { v: PROTOCOL_VERSION, type: 'join', room: parsed.room } };
    }

    case 'leave': {
      if (typeof parsed.room !== 'string' || parsed.room.trim().length === 0) {
        return { ok: false, code: 'INVALID_ROOM', error: 'leave.room must be a non-empty string' };
      }
      return { ok: true, frame: { v: PROTOCOL_VERSION, type: 'leave', room: parsed.room } };
    }

    case 'message': {
      if (parsed.room !== undefined && typeof parsed.room !== 'string') {
        return { ok: false, code: 'INVALID_MESSAGE', error: 'message.room must be a string' };
      }
      const frame: ClientMessageFrame = {
        v: PROTOCOL_VERSION,
        type: 'message',
        data: parsed.data,
      };
      if (parsed.room !== undefined) {
        frame.room = parsed.room;
      }
      return { ok: true, frame };
    }

    case 'ping': {
      if (parsed.ts !== undefined && typeof parsed.ts !== 'number') {
        return { ok: false, code: 'INVALID_PING', error: 'ping.ts must be a number' };
      }
      const frame: PingFrame = { v: PROTOCOL_VERSION, type: 'ping' };
      if (parsed.ts !== undefined) {
        frame.ts = parsed.ts;
      }
      return { ok: true, frame };
    }

    default:
      return {
        ok: false,
        code: 'UNKNOWN_FRAME_TYPE',
        error: `Unknown client frame type: "${type}"`,
      };
  }
}

export function decodeServerFrame(raw: string): DecodeResult<ServerFrame> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false, code: 'INVALID_JSON', error: 'Frame must be valid JSON' };
  }

  if (!isRecord(parsed)) {
    return { ok: false, code: 'INVALID_FRAME', error: 'Frame must be a JSON object', parsed };
  }

  if (parsed.v !== PROTOCOL_VERSION) {
    return {
      ok: false,
      code: 'UNSUPPORTED_VERSION',
      error: `Unsupported protocol version: ${String(parsed.v)}`,
    };
  }

  const type = parsed.type;
  if (typeof type !== 'string') {
    return { ok: false, code: 'INVALID_TYPE', error: 'Frame type must be a string' };
  }

  switch (type) {
    case 'welcome': {
      if (
        typeof parsed.sessionId !== 'string' ||
        typeof parsed.lastEventId !== 'string' ||
        !isValidStreamId(parsed.lastEventId) ||
        typeof parsed.recovered !== 'boolean' ||
        !Array.isArray(parsed.rooms) ||
        !parsed.rooms.every((r) => typeof r === 'string') ||
        (parsed.reason !== 'new_session' &&
          parsed.reason !== 'recovered' &&
          parsed.reason !== 'session_not_found' &&
          parsed.reason !== 'stream_trimmed')
      ) {
        return { ok: false, code: 'INVALID_WELCOME', error: 'Malformed welcome frame' };
      }
      return {
        ok: true,
        frame: {
          v: PROTOCOL_VERSION,
          type: 'welcome',
          sessionId: parsed.sessionId,
          lastEventId: parsed.lastEventId,
          recovered: parsed.recovered,
          rooms: parsed.rooms,
          reason: parsed.reason,
        },
      };
    }

    case 'event':
    case 'replay': {
      if (
        typeof parsed.id !== 'string' ||
        !isValidStreamId(parsed.id) ||
        typeof parsed.room !== 'string' ||
        typeof parsed.ts !== 'number'
      ) {
        return { ok: false, code: 'INVALID_EVENT', error: `Malformed ${type} frame` };
      }
      return {
        ok: true,
        frame: {
          v: PROTOCOL_VERSION,
          type,
          id: parsed.id,
          room: parsed.room,
          data: parsed.data,
          ts: parsed.ts,
        },
      };
    }

    case 'replay_complete': {
      if (
        typeof parsed.count !== 'number' ||
        typeof parsed.lastEventId !== 'string' ||
        !isValidStreamId(parsed.lastEventId)
      ) {
        return {
          ok: false,
          code: 'INVALID_REPLAY_COMPLETE',
          error: 'Malformed replay_complete frame',
        };
      }
      return {
        ok: true,
        frame: {
          v: PROTOCOL_VERSION,
          type: 'replay_complete',
          count: parsed.count,
          lastEventId: parsed.lastEventId,
        },
      };
    }

    case 'joined':
    case 'left': {
      if (typeof parsed.room !== 'string') {
        return { ok: false, code: 'INVALID_ROOM_FRAME', error: `Malformed ${type} frame` };
      }
      return {
        ok: true,
        frame: {
          v: PROTOCOL_VERSION,
          type,
          room: parsed.room,
        },
      };
    }

    case 'pong': {
      if (typeof parsed.ts !== 'number') {
        return { ok: false, code: 'INVALID_PONG', error: 'Malformed pong frame' };
      }
      return {
        ok: true,
        frame: {
          v: PROTOCOL_VERSION,
          type: 'pong',
          ts: parsed.ts,
        },
      };
    }

    case 'error': {
      if (typeof parsed.code !== 'string' || typeof parsed.message !== 'string') {
        return { ok: false, code: 'INVALID_ERROR', error: 'Malformed error frame' };
      }
      return {
        ok: true,
        frame: {
          v: PROTOCOL_VERSION,
          type: 'error',
          code: parsed.code,
          message: parsed.message,
        },
      };
    }

    default:
      return {
        ok: false,
        code: 'UNKNOWN_FRAME_TYPE',
        error: `Unknown server frame type: "${type}"`,
      };
  }
}
