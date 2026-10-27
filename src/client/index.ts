import {
  PROTOCOL_VERSION,
  compareStreamIds,
  decodeServerFrame,
  encodeFrame,
  isValidStreamId,
  maxStreamId,
  type ReplayCompleteFrame,
  type ReplayFrame,
  type WelcomeFrame,
  type WelcomeReason,
} from '../protocol/index.js';

/**
 * Minimal WebSocket interface compatible with both Browser `WebSocket` and Node `ws`.
 */
export interface WebSocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener?: (
    type: string,
    listener: (event: { data?: unknown; code?: number; reason?: string }) => void,
  ) => void;
  on?: (event: string, listener: (...args: unknown[]) => void) => void;
}

export type WebSocketConstructor = new (url: string) => WebSocketLike;

/**
 * Minimal key-value storage interface compatible with `localStorage` / `sessionStorage`.
 */
export interface SessionStorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export class MemorySessionStorage implements SessionStorageLike {
  private readonly store = new Map<string, string>();

  getItem(key: string): string | null {
    return this.store.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.store.set(key, value);
  }

  removeItem(key: string): void {
    this.store.delete(key);
  }
}

export interface StoredSessionCursor {
  sessionId: string;
  lastEventId: string;
}

export interface BackoffOptions {
  /** Initial reconnect delay in ms. Default: `250`. */
  minDelayMs?: number;
  /** Maximum reconnect delay cap in ms. Default: `15_000`. */
  maxDelayMs?: number;
  /** Exponential multiplier per attempt. Default: `2`. */
  factor?: number;
  /** Random jitter ratio (`0` to `1`). Default: `0.3`. */
  jitter?: number;
  /** Maximum reconnect attempts before giving up. Default: `Infinity`. */
  maxAttempts?: number;
}

export interface StreamCastClientOptions {
  /** Target WebSocket URL (or function returning URL on each connect/reconnect). */
  url: string | (() => string);
  /** Custom WebSocket constructor, for runtimes without a global `WebSocket` (e.g. `ws`). */
  WebSocket?: WebSocketConstructor;
  /** Storage for `sessionId` and `lastEventId`. Defaults to an in-memory store. */
  storage?: SessionStorageLike;
  /** Storage key used to persist session state. Default: `'streamcast:session'`. */
  storageKey?: string;
  /** Automatically connect upon construction. Default: `true`. */
  autoConnect?: boolean;
  /** Automatically reconnect on unexpected disconnects. Default: `true`. */
  reconnect?: boolean;
  /** Exponential backoff + jitter settings. */
  backoff?: BackoffOptions;
  /** Automatically send `{ v: 1, type: 'ack', id }` on event/replay receipt. Default: `true`. */
  autoAck?: boolean;
  /**
   * Optional coalescing window in ms for `autoAck`. `0` sends an ack immediately for every
   * event; `> 0` batches rapid events and sends the highest `lastEventId` every `ackIntervalMs`.
   * Default: `0`.
   */
  ackIntervalMs?: number;
}

export interface ClientDeliveredEvent {
  id: string;
  room: string;
  data: unknown;
  ts: number;
  replayed: boolean;
}

export interface StreamCastClientEventMap {
  open: () => void;
  welcome: (frame: WelcomeFrame) => void;
  event: (event: ClientDeliveredEvent) => void;
  replay: (frame: ReplayFrame) => void;
  replayComplete: (frame: ReplayCompleteFrame) => void;
  resync: (info: { reason: WelcomeReason; sessionId: string; rooms: string[] }) => void;
  joined: (room: string) => void;
  left: (room: string) => void;
  reconnecting: (info: { attempt: number; delayMs: number }) => void;
  close: (info: { code: number; reason: string }) => void;
  error: (error: Error) => void;
}

const WS_OPEN = 1;

/**
 * Computes exponential backoff delay with random jitter:
 * `clamp(base * (1 + uniform(-jitter, +jitter)), 0, maxDelayMs)`.
 */
export function computeReconnectDelay(
  attempt: number,
  options?: BackoffOptions,
  randomFn: () => number = Math.random,
): number {
  const minDelayMs = options?.minDelayMs ?? 250;
  const maxDelayMs = options?.maxDelayMs ?? 15_000;
  const factor = options?.factor ?? 2;
  const jitter = Math.max(0, Math.min(1, options?.jitter ?? 0.3));

  const exp = minDelayMs * Math.pow(factor, Math.max(0, attempt));
  const capped = Math.min(exp, maxDelayMs);
  const offset = capped * jitter * (randomFn() * 2 - 1);
  return Math.max(0, Math.min(maxDelayMs, Math.round(capped + offset)));
}

function appendWaitHelloParam(rawUrl: string): string {
  const hasQuery = rawUrl.includes('?');
  if (rawUrl.includes('waitHello=')) {
    return rawUrl;
  }
  return `${rawUrl}${hasQuery ? '&' : '?'}waitHello=1`;
}

/**
 * Browser and Node.js WebSocket client helper for `streamcast-ws`.
 * Automatically manages handshake (`hello`/`welcome`), `sessionId` & `lastEventId` persistence,
 * `ack` frames, reconnect with exponential backoff + jitter, and trimmed-stream `resync` signals.
 */
export class StreamCastClient {
  private readonly urlProvider: () => string;
  private readonly WsImpl: WebSocketConstructor;
  private readonly storage: SessionStorageLike;
  private readonly storageKey: string;
  private readonly shouldReconnect: boolean;
  private readonly backoff: BackoffOptions;
  private readonly autoAck: boolean;
  private readonly ackIntervalMs: number;

  private readonly listeners = new Map<
    keyof StreamCastClientEventMap,
    Set<(...args: never[]) => void>
  >();
  private readonly desiredRooms = new Set<string>();
  private readonly recentEventIds: string[] = [];
  private readonly recentEventIdSet = new Set<string>();

  private ws: WebSocketLike | null = null;
  private sessionId: string | undefined;
  private lastEventId: string | undefined;
  private hadPriorWelcome = false;
  private manuallyClosed = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pendingAckId: string | null = null;
  private ackTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: StreamCastClientOptions) {
    this.urlProvider =
      typeof options.url === 'function' ? options.url : () => options.url as string;

    const ResolvedWs =
      options.WebSocket ??
      (globalThis as unknown as { WebSocket?: WebSocketConstructor }).WebSocket ??
      undefined;
    if (!ResolvedWs) {
      throw new Error(
        'No global WebSocket implementation found; pass `WebSocket` (e.g. from "ws") in StreamCastClientOptions.',
      );
    }
    this.WsImpl = ResolvedWs;

    this.storage = options.storage ?? new MemorySessionStorage();
    this.storageKey = options.storageKey ?? 'streamcast:session';
    this.shouldReconnect = options.reconnect ?? true;
    this.backoff = options.backoff ?? {};
    this.autoAck = options.autoAck ?? true;
    this.ackIntervalMs = options.ackIntervalMs ?? 0;

    this.loadStoredSession();

    if (options.autoConnect !== false) {
      this.connect();
    }
  }

  on<K extends keyof StreamCastClientEventMap>(
    event: K,
    listener: StreamCastClientEventMap[K],
  ): this {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    set.add(listener as unknown as (...args: never[]) => void);
    return this;
  }

  off<K extends keyof StreamCastClientEventMap>(
    event: K,
    listener: StreamCastClientEventMap[K],
  ): this {
    this.listeners.get(event)?.delete(listener as unknown as (...args: never[]) => void);
    return this;
  }

  once<K extends keyof StreamCastClientEventMap>(
    event: K,
    listener: StreamCastClientEventMap[K],
  ): this {
    const wrapper = ((...args: unknown[]) => {
      this.off(event, wrapper as unknown as StreamCastClientEventMap[K]);
      (listener as unknown as (...a: unknown[]) => void)(...args);
    }) as unknown as StreamCastClientEventMap[K];
    return this.on(event, wrapper);
  }

  private emit<K extends keyof StreamCastClientEventMap>(
    event: K,
    ...args: Parameters<StreamCastClientEventMap[K]>
  ): void {
    const handlers = this.listeners.get(event);
    if (!handlers) return;
    for (const fn of Array.from(handlers)) {
      (fn as unknown as (...a: unknown[]) => void)(...args);
    }
  }

  getSessionId(): string | undefined {
    return this.sessionId;
  }

  getLastEventId(): string | undefined {
    return this.lastEventId;
  }

  isConnected(): boolean {
    return this.ws !== null && this.ws.readyState === WS_OPEN;
  }

  private loadStoredSession(): void {
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw) as Partial<StoredSessionCursor>;
      if (typeof parsed.sessionId === 'string' && parsed.sessionId.length > 0) {
        this.sessionId = parsed.sessionId;
        this.hadPriorWelcome = true;
      }
      if (typeof parsed.lastEventId === 'string' && isValidStreamId(parsed.lastEventId)) {
        this.lastEventId = parsed.lastEventId;
      }
    } catch {
      // Ignore corrupted client storage
    }
  }

  private persistSession(): void {
    if (!this.sessionId) return;
    const payload: StoredSessionCursor = {
      sessionId: this.sessionId,
      lastEventId: this.lastEventId ?? '0-0',
    };
    try {
      this.storage.setItem(this.storageKey, JSON.stringify(payload));
    } catch {
      // Ignore storage quota errors
    }
  }

  clearStoredSession(): void {
    this.sessionId = undefined;
    this.lastEventId = undefined;
    this.hadPriorWelcome = false;
    this.storage.removeItem(this.storageKey);
  }

  connect(): void {
    this.manuallyClosed = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    const targetUrl = appendWaitHelloParam(this.urlProvider());
    const socket = new this.WsImpl(targetUrl);
    this.ws = socket;

    const handleOpen = () => {
      if (this.ws !== socket) return;
      this.reconnectAttempt = 0;
      this.emit('open');

      socket.send(
        encodeFrame({
          v: PROTOCOL_VERSION,
          type: 'hello',
          ...(this.sessionId ? { sessionId: this.sessionId } : {}),
          ...(this.lastEventId ? { lastEventId: this.lastEventId } : {}),
        }),
      );
    };

    const handleMessage = (rawData: unknown) => {
      if (this.ws !== socket) return;
      const text =
        typeof rawData === 'string'
          ? rawData
          : rawData instanceof Uint8Array
            ? new TextDecoder().decode(rawData)
            : String(rawData);
      this.onServerMessage(text);
    };

    const handleClose = (code = 1006, reason = '') => {
      if (this.ws !== socket) return;
      this.ws = null;
      this.emit('close', { code, reason });
      if (!this.manuallyClosed && this.shouldReconnect) {
        this.scheduleReconnect();
      }
    };

    const handleError = (err: unknown) => {
      const error = err instanceof Error ? err : new Error('WebSocket connection error');
      this.emit('error', error);
    };

    if (typeof socket.addEventListener === 'function') {
      socket.addEventListener('open', () => handleOpen());
      socket.addEventListener('message', (ev) => handleMessage(ev.data));
      socket.addEventListener('close', (ev) =>
        handleClose(ev.code ?? 1006, typeof ev.reason === 'string' ? ev.reason : ''),
      );
      socket.addEventListener('error', (ev) => handleError(ev));
    } else if (typeof socket.on === 'function') {
      socket.on('open', () => handleOpen());
      socket.on('message', (data) => handleMessage(data));
      socket.on('close', (code, reason) =>
        handleClose(typeof code === 'number' ? code : 1006, String(reason ?? '')),
      );
      socket.on('error', (err) => handleError(err));
    }
  }

  private scheduleReconnect(): void {
    const maxAttempts = this.backoff.maxAttempts ?? Infinity;
    if (this.reconnectAttempt >= maxAttempts) {
      return;
    }

    const attempt = this.reconnectAttempt;
    this.reconnectAttempt += 1;
    const delayMs = computeReconnectDelay(attempt, this.backoff);

    this.emit('reconnecting', { attempt: attempt + 1, delayMs });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.manuallyClosed) {
        this.connect();
      }
    }, delayMs);
    this.reconnectTimer.unref?.();
  }

  private onServerMessage(raw: string): void {
    const decoded = decodeServerFrame(raw);
    if (!decoded.ok) {
      this.emit('error', new Error(`Protocol decode error (${decoded.code}): ${decoded.error}`));
      return;
    }

    const frame = decoded.frame;
    switch (frame.type) {
      case 'welcome': {
        const expectedRecovery = this.hadPriorWelcome;
        this.sessionId = frame.sessionId;
        this.lastEventId = this.lastEventId
          ? frame.recovered
            ? maxStreamId(this.lastEventId, frame.lastEventId)
            : frame.lastEventId
          : frame.lastEventId;
        this.hadPriorWelcome = true;
        this.persistSession();

        this.emit('welcome', frame);

        // If session expired on Redis, re-subscribe desired rooms automatically
        if (frame.reason === 'session_not_found' && this.desiredRooms.size > 0) {
          for (const room of this.desiredRooms) {
            this.sendRawFrame({ v: PROTOCOL_VERSION, type: 'join', room });
          }
        }

        if (expectedRecovery && !frame.recovered) {
          this.emit('resync', {
            reason: frame.reason,
            sessionId: frame.sessionId,
            rooms: frame.rooms,
          });
        }
        break;
      }

      case 'replay': {
        this.handleIncomingStreamEvent(frame.id, frame.room, frame.data, frame.ts, true);
        this.emit('replay', frame);
        break;
      }

      case 'replay_complete': {
        this.advanceCursor(frame.lastEventId);
        this.emit('replayComplete', frame);
        break;
      }

      case 'event': {
        this.handleIncomingStreamEvent(frame.id, frame.room, frame.data, frame.ts, false);
        break;
      }

      case 'joined': {
        this.emit('joined', frame.room);
        break;
      }

      case 'left': {
        this.emit('left', frame.room);
        break;
      }

      case 'pong':
        break;

      case 'error': {
        this.emit('error', new Error(`[${frame.code}] ${frame.message}`));
        break;
      }
    }
  }

  private handleIncomingStreamEvent(
    id: string,
    room: string,
    data: unknown,
    ts: number,
    replayed: boolean,
  ): void {
    this.advanceCursor(id);
    if (this.autoAck) {
      this.queueAck(id);
    }

    if (this.recentEventIdSet.has(id)) {
      return;
    }
    this.recordSeenId(id);

    this.emit('event', { id, room, data, ts, replayed });
  }

  private recordSeenId(id: string): void {
    if (this.recentEventIds.length >= 5_000) {
      const oldest = this.recentEventIds.shift();
      if (oldest !== undefined) {
        this.recentEventIdSet.delete(oldest);
      }
    }
    this.recentEventIds.push(id);
    this.recentEventIdSet.add(id);
  }

  private advanceCursor(id: string): void {
    if (!this.lastEventId || compareStreamIds(id, this.lastEventId) > 0) {
      this.lastEventId = id;
      this.persistSession();
    }
  }

  private queueAck(id: string): void {
    if (!this.pendingAckId || compareStreamIds(id, this.pendingAckId) > 0) {
      this.pendingAckId = id;
    }

    if (this.ackIntervalMs <= 0) {
      this.flushAck();
      return;
    }

    if (!this.ackTimer) {
      this.ackTimer = setTimeout(() => {
        this.ackTimer = null;
        this.flushAck();
      }, this.ackIntervalMs);
      this.ackTimer.unref?.();
    }
  }

  private flushAck(): void {
    if (!this.pendingAckId) return;
    const id = this.pendingAckId;
    this.pendingAckId = null;
    this.ack(id);
  }

  ack(eventId?: string): void {
    const idToAck = eventId ?? this.lastEventId;
    if (!idToAck || !isValidStreamId(idToAck)) return;
    this.advanceCursor(idToAck);
    this.sendRawFrame({ v: PROTOCOL_VERSION, type: 'ack', id: idToAck });
  }

  join(room: string): void {
    this.desiredRooms.add(room);
    this.sendRawFrame({ v: PROTOCOL_VERSION, type: 'join', room });
  }

  leave(room: string): void {
    this.desiredRooms.delete(room);
    this.sendRawFrame({ v: PROTOCOL_VERSION, type: 'leave', room });
  }

  send(data: unknown, room?: string): void {
    this.sendRawFrame({
      v: PROTOCOL_VERSION,
      type: 'message',
      ...(room !== undefined ? { room } : {}),
      data,
    });
  }

  private sendRawFrame(frame: Parameters<typeof encodeFrame>[0]): void {
    if (this.ws && this.ws.readyState === WS_OPEN) {
      this.ws.send(encodeFrame(frame));
    }
  }

  close(code = 1000, reason = 'Client closed'): void {
    this.manuallyClosed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ackTimer) {
      clearTimeout(this.ackTimer);
      this.ackTimer = null;
    }
    this.flushAck();
    if (this.ws) {
      this.ws.close(code, reason);
      this.ws = null;
    }
  }
}
