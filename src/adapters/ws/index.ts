import { EventEmitter } from 'node:events';
import type { IncomingMessage } from 'node:http';
import { WebSocket, type RawData, type WebSocketServer } from 'ws';
import { StreamCast, type StreamCastMessage, type StreamCastOptions } from '../../core/index.js';
import {
  PROTOCOL_VERSION,
  compareStreamIds,
  decodeClientFrame,
  encodeFrame,
  isValidStreamId,
  maxStreamId,
  type ReplayFrame,
  type ServerFrame,
  type WelcomeReason,
} from '../../protocol/index.js';
import { SessionRecovery } from '../../recovery/index.js';

export interface HandshakeRecoveryResult {
  sessionId: string;
  lastEventId: string;
  recovered: boolean;
  reason: WelcomeReason;
  rooms: string[];
  replayedEvents: ReplayFrame[];
}

export interface SessionLifecycleProvider {
  resolveSession(params: {
    requestedSessionId?: string;
    requestedLastEventId?: string;
  }): Promise<HandshakeRecoveryResult>;
  recordJoin(sessionId: string, room: string): Promise<void>;
  recordLeave(sessionId: string, room: string): Promise<void>;
  recordAck(sessionId: string, eventId: string): Promise<void>;
  touchSession(sessionId: string): Promise<void>;
}

export interface WsClientSession {
  readonly id: string;
  readonly ws: WebSocket;
  readonly recovered: boolean;
  readonly rooms: ReadonlySet<string>;
  lastEventId: string;
  join(room: string): Promise<void>;
  leave(room: string): Promise<void>;
  sendFrame(frame: ServerFrame): void;
}

export type SessionTarget = string | WebSocket | WsClientSession;

export interface WsBroadcastOptions {
  except?: SessionTarget | SessionTarget[];
}

export type HandshakeMode = 'auto' | 'hello';

export type RoomAuthorizer =
  boolean | ((session: WsClientSession, room: string) => boolean | Promise<boolean>);

export interface WsStreamCastOptions extends StreamCastOptions {
  /**
   * Handshake mode:
   * - `'auto'` (default): completes handshake immediately on connect (supporting `?sessionId=`
   *   and `?lastEventId=` query params), unless `?waitHello=1` is present in the URL. Also
   *   accepts an explicit `hello` frame to upgrade/recover an active connection.
   * - `'hello'`: always waits for a client `{ v: 1, type: 'hello' }` frame before emitting
   *   `welcome` or `connection`.
   */
  handshake?: HandshakeMode;
  /** Max time in ms to wait for a `hello` frame when waiting is active. Default: `5_000`. */
  handshakeTimeoutMs?: number;
  /**
   * Whether clients are allowed to join/leave rooms directly via `{ v: 1, type: 'join' | 'leave' }`
   * frames, or an async guard function. Default: `true`.
   */
  allowClientRooms?: RoomAuthorizer;
  /** How long a disconnected session stays recoverable in Redis, in ms. Default: `600_000` (10 min). */
  sessionTtlMs?: number;
  /** Custom session provider (internal hook). Defaults to the Redis-backed `SessionRecovery`. */
  sessionProvider?: SessionLifecycleProvider;
}

export interface StreamCastWsServerEvents {
  connection: [session: WsClientSession, request: IncomingMessage, info: HandshakeRecoveryResult];
  disconnect: [session: WsClientSession, code: number, reason: string];
  message: [session: WsClientSession, data: unknown, room: string | undefined];
  error: [error: Error];
}

class ManagedWsSession implements WsClientSession {
  public id: string;
  public readonly ws: WebSocket;
  public recovered: boolean;
  public lastEventId: string;

  private readonly server: StreamCastWsServer;
  private replaying = false;
  private maxReplayedId = '0-0';
  private readonly liveBuffer: StreamCastMessage[] = [];

  constructor(
    server: StreamCastWsServer,
    ws: WebSocket,
    id: string,
    lastEventId: string,
    recovered: boolean,
  ) {
    this.server = server;
    this.ws = ws;
    this.id = id;
    this.lastEventId = lastEventId;
    this.recovered = recovered;
  }

  get rooms(): ReadonlySet<string> {
    return this.server.core.getSessionRooms(this.id);
  }

  async join(room: string): Promise<void> {
    await this.server.join(this.id, room);
  }

  async leave(room: string): Promise<void> {
    await this.server.leave(this.id, room);
  }

  sendFrame(frame: ServerFrame): void {
    if (this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    this.ws.send(encodeFrame(frame));
  }

  beginReplayBuffering(): void {
    this.replaying = true;
    this.liveBuffer.length = 0;
  }

  finishReplayBuffering(maxReplayedId: string): void {
    this.maxReplayedId = maxReplayedId;
    this.replaying = false;
    for (const msg of this.liveBuffer) {
      if (compareStreamIds(msg.id, this.maxReplayedId) > 0) {
        this.deliverLiveMessage(msg);
      }
    }
    this.liveBuffer.length = 0;
  }

  handleCoreDelivery(msg: StreamCastMessage): void {
    if (this.replaying) {
      this.liveBuffer.push(msg);
      return;
    }
    this.deliverLiveMessage(msg);
  }

  private deliverLiveMessage(msg: StreamCastMessage): void {
    this.sendFrame({
      v: PROTOCOL_VERSION,
      type: 'event',
      id: msg.id,
      room: msg.room,
      data: msg.payload,
      ts: msg.ts,
    });
  }
}

export class StreamCastWsServer extends EventEmitter<StreamCastWsServerEvents> {
  public readonly wss: WebSocketServer;
  public readonly core: StreamCast;
  public readonly recovery: SessionRecovery;
  public readonly sessionProvider: SessionLifecycleProvider;

  private readonly sessionsById = new Map<string, ManagedWsSession>();
  private readonly sessionsBySocket = new Map<WebSocket, ManagedWsSession>();
  private readonly handshakeMode: HandshakeMode;
  private readonly handshakeTimeoutMs: number;
  private readonly allowClientRooms: RoomAuthorizer;
  private readonly onWsConnectionBound: (ws: WebSocket, req: IncomingMessage) => void;
  private closed = false;

  constructor(wss: WebSocketServer, options: WsStreamCastOptions) {
    super();
    this.wss = wss;

    const userOnError = options.onError;
    this.core = new StreamCast({
      ...options,
      onError: (err, ctx) => {
        userOnError?.(err, ctx);
        if (this.listenerCount('error') > 0) {
          this.emit('error', err);
        }
      },
    });

    this.recovery = SessionRecovery.fromCore(this.core, options.sessionTtlMs);
    this.sessionProvider = options.sessionProvider ?? this.recovery;

    this.handshakeMode = options.handshake ?? 'auto';
    this.handshakeTimeoutMs = options.handshakeTimeoutMs ?? 5_000;
    this.allowClientRooms = options.allowClientRooms ?? true;

    this.onWsConnectionBound = (ws, req) => {
      void this.handleNewSocket(ws, req);
    };
    this.wss.on('connection', this.onWsConnectionBound);

    // Kick off startup eagerly in the background; operations also await core.ready().
    void this.core.ready().catch((err) => {
      if (this.listenerCount('error') > 0) {
        this.emit('error', err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  async ready(): Promise<void> {
    await this.core.ready();
  }

  private parseUrlParams(req: IncomingMessage): {
    sessionId?: string;
    lastEventId?: string;
    waitHello: boolean;
  } {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      const sessionId = url.searchParams.get('sessionId') ?? undefined;
      const rawLastId = url.searchParams.get('lastEventId') ?? undefined;
      const lastEventId = rawLastId && isValidStreamId(rawLastId) ? rawLastId : undefined;
      const waitHello = url.searchParams.get('waitHello') === '1';
      return {
        ...(sessionId ? { sessionId } : {}),
        ...(lastEventId ? { lastEventId } : {}),
        waitHello,
      };
    } catch {
      return { waitHello: false };
    }
  }

  private async handleNewSocket(ws: WebSocket, req: IncomingMessage): Promise<void> {
    if (this.closed) {
      ws.close(1001, 'Server shutting down');
      return;
    }

    const query = this.parseUrlParams(req);
    const shouldWaitHello = this.handshakeMode === 'hello' || query.waitHello;

    // Queue frames received while async handshake/recovery is in progress so nothing is lost.
    let handshakeComplete = false;
    const preHandshakeMessages: RawData[] = [];
    let helloTimer: ReturnType<typeof setTimeout> | null = null;
    let helloReceived = false;

    const completeHandshake = async (params: {
      requestedSessionId?: string;
      requestedLastEventId?: string;
    }): Promise<ManagedWsSession | null> => {
      try {
        await this.core.ready();
        if (ws.readyState !== WebSocket.OPEN && ws.readyState !== WebSocket.CONNECTING) {
          return null;
        }

        const recovery = await this.sessionProvider.resolveSession(params);
        if (ws.readyState !== WebSocket.OPEN) {
          return null;
        }

        // If an older stale socket still held this sessionId locally, detach it cleanly.
        const existing = this.sessionsById.get(recovery.sessionId);
        if (existing && existing.ws !== ws) {
          this.sessionsBySocket.delete(existing.ws);
          await this.core.unregisterSession(existing.id);
          try {
            existing.ws.close(4000, 'Session resumed on another connection');
          } catch {
            // Ignore close errors on stale socket
          }
        }

        const managed = new ManagedWsSession(
          this,
          ws,
          recovery.sessionId,
          recovery.lastEventId,
          recovery.recovered,
        );

        this.sessionsById.set(managed.id, managed);
        this.sessionsBySocket.set(ws, managed);

        managed.beginReplayBuffering();

        this.core.registerSession(managed.id, {
          send: (message) => managed.handleCoreDelivery(message),
          getBufferedAmount: () => ws.bufferedAmount,
          close: (code, reason) => ws.close(code, reason),
        });

        // Join rooms while live deliveries are still buffered, so no event slips between XRANGE and join.
        for (const room of recovery.rooms) {
          await this.core.join(managed.id, room);
        }

        let replayedEvents = recovery.replayedEvents;
        let finalLastEventId = recovery.lastEventId;

        if (recovery.recovered && recovery.rooms.length > 0 && params.requestedSessionId) {
          // Catch up on events published between the initial XRANGE and core.join.
          const catchupAnchor =
            replayedEvents.length > 0
              ? replayedEvents[replayedEvents.length - 1]!.id
              : recovery.lastEventId;
          const extra = await this.sessionProvider.resolveSession({
            requestedSessionId: recovery.sessionId,
            requestedLastEventId: catchupAnchor,
          });
          if (extra.recovered && extra.replayedEvents.length > 0) {
            const seen = new Set(replayedEvents.map((e) => e.id));
            for (const ev of extra.replayedEvents) {
              if (!seen.has(ev.id)) {
                replayedEvents = [...replayedEvents, ev];
              }
            }
          }
        }

        if (replayedEvents.length > 0) {
          finalLastEventId = replayedEvents[replayedEvents.length - 1]!.id;
          managed.lastEventId = finalLastEventId;
        }

        managed.sendFrame({
          v: PROTOCOL_VERSION,
          type: 'welcome',
          sessionId: managed.id,
          lastEventId: finalLastEventId,
          recovered: recovery.recovered,
          rooms: recovery.rooms,
          reason: recovery.reason,
        });

        for (const replayFrame of replayedEvents) {
          managed.sendFrame(replayFrame);
        }

        if (recovery.recovered) {
          managed.sendFrame({
            v: PROTOCOL_VERSION,
            type: 'replay_complete',
            count: replayedEvents.length,
            lastEventId: finalLastEventId,
          });
          if (replayedEvents.length > 0) {
            this.core.recordReplayMetric(managed.id, replayedEvents.length);
          }
        }

        managed.finishReplayBuffering(finalLastEventId);

        this.emit('connection', managed, req, {
          ...recovery,
          lastEventId: finalLastEventId,
          replayedEvents,
        });

        return managed;
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        if (this.listenerCount('error') > 0) {
          this.emit('error', error);
        }
        try {
          ws.close(1011, 'Handshake error');
        } catch {
          // Ignore
        }
        return null;
      }
    };

    ws.on('message', (raw: RawData) => {
      if (!handshakeComplete) {
        if (shouldWaitHello && !helloReceived) {
          const text = rawDataToString(raw);
          const decoded = decodeClientFrame(text);
          if (decoded.ok && decoded.frame.type === 'hello') {
            helloReceived = true;
            if (helloTimer) {
              clearTimeout(helloTimer);
              helloTimer = null;
            }
            void completeHandshake({
              requestedSessionId: decoded.frame.sessionId ?? query.sessionId,
              requestedLastEventId: decoded.frame.lastEventId ?? query.lastEventId,
            }).then(async (session) => {
              handshakeComplete = true;
              if (session) {
                for (const buffered of preHandshakeMessages.splice(0)) {
                  await this.handleSocketMessage(session, buffered);
                }
              }
            });
            return;
          } else {
            ws.send(
              encodeFrame({
                v: PROTOCOL_VERSION,
                type: 'error',
                code: 'EXPECTED_HELLO',
                message: 'First frame must be a valid hello handshake frame',
              }),
            );
            ws.close(1002, 'Expected hello frame');
            return;
          }
        }

        preHandshakeMessages.push(raw);
        return;
      }

      const session = this.sessionsBySocket.get(ws);
      if (session) {
        void this.handleSocketMessage(session, raw);
      }
    });

    ws.on('close', (code: number, reasonBuf: Buffer) => {
      if (helloTimer) {
        clearTimeout(helloTimer);
        helloTimer = null;
      }
      const session = this.sessionsBySocket.get(ws);
      if (session) {
        this.sessionsBySocket.delete(ws);
        if (this.sessionsById.get(session.id) === session) {
          this.sessionsById.delete(session.id);
        }
        void this.core.unregisterSession(session.id).catch(() => undefined);
        void this.sessionProvider.touchSession(session.id).catch(() => undefined);
        this.emit('disconnect', session, code, reasonBuf.toString('utf8'));
      }
    });

    ws.on('error', (err) => {
      if (this.listenerCount('error') > 0) {
        this.emit('error', err);
      }
    });

    if (shouldWaitHello) {
      helloTimer = setTimeout(() => {
        if (!helloReceived && ws.readyState === WebSocket.OPEN) {
          ws.send(
            encodeFrame({
              v: PROTOCOL_VERSION,
              type: 'error',
              code: 'HANDSHAKE_TIMEOUT',
              message: 'Timed out waiting for hello frame',
            }),
          );
          ws.close(1008, 'Handshake timeout');
        }
      }, this.handshakeTimeoutMs);
      helloTimer.unref?.();
    } else {
      const session = await completeHandshake({
        requestedSessionId: query.sessionId,
        requestedLastEventId: query.lastEventId,
      });
      handshakeComplete = true;
      if (session) {
        for (const buffered of preHandshakeMessages.splice(0)) {
          await this.handleSocketMessage(session, buffered);
        }
      }
    }
  }

  private async handleSocketMessage(session: ManagedWsSession, raw: RawData): Promise<void> {
    const text = rawDataToString(raw);
    const decoded = decodeClientFrame(text);

    if (!decoded.ok) {
      if (decoded.isNonProtocolJson) {
        this.emit('message', session, decoded.parsed, undefined);
        return;
      }
      session.sendFrame({
        v: PROTOCOL_VERSION,
        type: 'error',
        code: decoded.code,
        message: decoded.error,
      });
      return;
    }

    const frame = decoded.frame;
    switch (frame.type) {
      case 'hello': {
        // Late hello frame on an auto-handshaken connection: if client supplied a sessionId
        // to recover, migrate this socket to that session.
        if (frame.sessionId && frame.sessionId !== session.id) {
          await this.migrateSessionOnHello(session, frame.sessionId, frame.lastEventId);
        } else {
          session.sendFrame({
            v: PROTOCOL_VERSION,
            type: 'welcome',
            sessionId: session.id,
            lastEventId: session.lastEventId,
            recovered: session.recovered,
            rooms: Array.from(session.rooms),
            reason: session.recovered ? 'recovered' : 'new_session',
          });
        }
        break;
      }

      case 'ack': {
        session.lastEventId = maxStreamId(session.lastEventId, frame.id);
        await this.sessionProvider.recordAck(session.id, session.lastEventId);
        break;
      }

      case 'join': {
        const permitted =
          typeof this.allowClientRooms === 'function'
            ? await this.allowClientRooms(session, frame.room)
            : this.allowClientRooms;
        if (!permitted) {
          session.sendFrame({
            v: PROTOCOL_VERSION,
            type: 'error',
            code: 'FORBIDDEN_ROOM',
            message: `Not authorized to join room "${frame.room}"`,
          });
          return;
        }
        await this.join(session.id, frame.room);
        session.sendFrame({ v: PROTOCOL_VERSION, type: 'joined', room: frame.room });
        break;
      }

      case 'leave': {
        const permitted =
          typeof this.allowClientRooms === 'function'
            ? await this.allowClientRooms(session, frame.room)
            : this.allowClientRooms;
        if (!permitted) {
          session.sendFrame({
            v: PROTOCOL_VERSION,
            type: 'error',
            code: 'FORBIDDEN_ROOM',
            message: `Not authorized to leave room "${frame.room}"`,
          });
          return;
        }
        await this.leave(session.id, frame.room);
        session.sendFrame({ v: PROTOCOL_VERSION, type: 'left', room: frame.room });
        break;
      }

      case 'ping': {
        session.sendFrame({
          v: PROTOCOL_VERSION,
          type: 'pong',
          ts: frame.ts ?? Date.now(),
        });
        break;
      }

      case 'message': {
        this.emit('message', session, frame.data, frame.room);
        break;
      }
    }
  }

  private async migrateSessionOnHello(
    session: ManagedWsSession,
    requestedSessionId: string,
    requestedLastEventId?: string,
  ): Promise<void> {
    const oldId = session.id;
    this.sessionsById.delete(oldId);
    await this.core.unregisterSession(oldId);

    const recovery = await this.sessionProvider.resolveSession({
      requestedSessionId,
      requestedLastEventId,
    });

    session.id = recovery.sessionId;
    session.lastEventId = recovery.lastEventId;
    session.recovered = recovery.recovered;
    this.sessionsById.set(session.id, session);

    session.beginReplayBuffering();
    this.core.registerSession(session.id, {
      send: (m) => session.handleCoreDelivery(m),
      getBufferedAmount: () => session.ws.bufferedAmount,
      close: (code, reason) => session.ws.close(code, reason),
    });

    for (const room of recovery.rooms) {
      await this.core.join(session.id, room);
    }

    const finalId =
      recovery.replayedEvents.length > 0
        ? recovery.replayedEvents[recovery.replayedEvents.length - 1]!.id
        : recovery.lastEventId;
    session.lastEventId = finalId;

    session.sendFrame({
      v: PROTOCOL_VERSION,
      type: 'welcome',
      sessionId: session.id,
      lastEventId: finalId,
      recovered: recovery.recovered,
      rooms: recovery.rooms,
      reason: recovery.reason,
    });

    for (const replayFrame of recovery.replayedEvents) {
      session.sendFrame(replayFrame);
    }

    if (recovery.recovered) {
      session.sendFrame({
        v: PROTOCOL_VERSION,
        type: 'replay_complete',
        count: recovery.replayedEvents.length,
        lastEventId: finalId,
      });
      if (recovery.replayedEvents.length > 0) {
        this.core.recordReplayMetric(session.id, recovery.replayedEvents.length);
      }
    }

    session.finishReplayBuffering(finalId);
  }

  private resolveTargetSessionId(target: SessionTarget): string {
    if (typeof target === 'string') {
      return target;
    }
    if ('ws' in target && typeof target.id === 'string') {
      return target.id;
    }
    const found = this.sessionsBySocket.get(target as WebSocket);
    if (!found) {
      throw new Error('Unknown WebSocket instance passed to StreamCastWsServer');
    }
    return found.id;
  }

  async join(target: SessionTarget, room: string): Promise<void> {
    const sessionId = this.resolveTargetSessionId(target);
    await this.core.join(sessionId, room);
    await this.sessionProvider.recordJoin(sessionId, room);
  }

  async leave(target: SessionTarget, room: string): Promise<void> {
    const sessionId = this.resolveTargetSessionId(target);
    await this.core.leave(sessionId, room);
    await this.sessionProvider.recordLeave(sessionId, room);
  }

  async broadcast(room: string, payload: unknown, options?: WsBroadcastOptions): Promise<string> {
    let exceptIds: string[] | undefined;
    if (options?.except !== undefined) {
      const rawTargets = Array.isArray(options.except) ? options.except : [options.except];
      exceptIds = rawTargets.map((t) => this.resolveTargetSessionId(t));
    }

    return this.core.broadcast(room, payload, exceptIds ? { except: exceptIds } : undefined);
  }

  async roomSize(room: string): Promise<number> {
    return this.core.roomSize(room);
  }

  async listMembers(room: string): Promise<string[]> {
    return this.core.listMembers(room);
  }

  getSession(sessionId: string): WsClientSession | undefined {
    return this.sessionsById.get(sessionId);
  }

  getSessionBySocket(ws: WebSocket): WsClientSession | undefined {
    return this.sessionsBySocket.get(ws);
  }

  async close(options?: { closeWebSocketServer?: boolean }): Promise<void> {
    if (this.closed) return;
    this.closed = true;

    this.wss.off('connection', this.onWsConnectionBound);

    if (options?.closeWebSocketServer) {
      for (const client of this.wss.clients) {
        try {
          client.close(1001, 'Server shutting down');
        } catch {
          // Ignore
        }
      }
      await new Promise<void>((resolve) => this.wss.close(() => resolve()));
    }

    this.sessionsById.clear();
    this.sessionsBySocket.clear();
    await this.core.close();
  }
}

function rawDataToString(raw: RawData): string {
  if (typeof raw === 'string') return raw;
  if (Buffer.isBuffer(raw)) return raw.toString('utf8');
  if (Array.isArray(raw)) return Buffer.concat(raw).toString('utf8');
  return Buffer.from(raw).toString('utf8');
}

/**
 * Wraps a `ws` `WebSocketServer` with Redis Streams room scaling and session recovery.
 */
export function createStreamCastServer(
  wss: WebSocketServer,
  options: WsStreamCastOptions,
): StreamCastWsServer {
  return new StreamCastWsServer(wss, options);
}
