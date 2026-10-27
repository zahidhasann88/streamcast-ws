import {
  Adapter,
  type BroadcastFlags,
  type BroadcastOptions,
  type Room,
  type SocketId,
} from 'socket.io-adapter';
import { StreamCast, type StreamCastMessage, type StreamCastOptions } from '../../core/index.js';

const CLUSTER_CHANNEL = '__sio_cluster__';
const ALL_SIDS_ROOM = '__sio_all_sids__';

interface SerializedSocketIoBroadcast {
  packet: {
    type: number;
    data?: unknown;
    nsp: string;
    id?: number;
  };
  rooms: Room[];
  except: Room[];
  flags?: BroadcastFlags;
}

export type SocketIoStreamCastOptions = Omit<StreamCastOptions, 'immediateLocalDelivery'>;

/**
 * Redis Streams-backed Socket.io `Adapter` implementation.
 *
 * @experimental Supports multi-node room broadcasts (`io.emit`, `io.to(room).emit`,
 * `socket.to(room).emit`, multi-room union, `except`), global `sockets(rooms)`, and
 * `serverCount()`. Cluster RPC features (`fetchSockets`, `serverSideEmit`,
 * `broadcastWithAck`, and binary Buffer attachments) are not yet implemented.
 */
export class StreamCastSocketIoAdapter extends Adapter {
  public readonly core: StreamCast;
  private readonly listenerSessionId: string;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(nsp: any, options: SocketIoStreamCastOptions) {
    super(nsp);
    const nspName = typeof nsp?.name === 'string' ? nsp.name : '/';
    const basePrefix = options.prefix ?? 'streamcast';

    this.core = new StreamCast({
      ...options,
      prefix: `${basePrefix}:sio:${nspName}`,
      immediateLocalDelivery: false,
    });

    this.listenerSessionId = `__node_listener__:${this.core.nodeId}`;
    this.core.registerSession(this.listenerSessionId, {
      send: (msg: StreamCastMessage) => this.onRemoteBroadcast(msg),
    });
  }

  override async init(): Promise<void> {
    await this.core.ready();
    await this.core.join(this.listenerSessionId, CLUSTER_CHANNEL);
  }

  override async close(): Promise<void> {
    await this.core.close();
  }

  override async addAll(id: SocketId, rooms: Set<Room>): Promise<void> {
    super.addAll(id, rooms);
    await this.core.ready();
    const ops: Promise<void>[] = [this.core.join(id, ALL_SIDS_ROOM)];
    for (const room of rooms) {
      ops.push(this.core.join(id, room));
    }
    await Promise.all(ops);
  }

  override async del(id: SocketId, room: Room): Promise<void> {
    super.del(id, room);
    await this.core.leave(id, room);
  }

  override delAll(id: SocketId): void {
    super.delAll(id);
    void this.core.leaveAll(id).catch(() => undefined);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  override broadcast(packet: any, opts: BroadcastOptions): void {
    super.broadcast(packet, opts);

    // Respect Socket.io's `local` flag (do not fan out across Redis)
    if (opts.flags?.local) {
      return;
    }

    const nspName = typeof this.nsp?.name === 'string' ? this.nsp.name : '/';
    const envelope: SerializedSocketIoBroadcast = {
      packet: {
        type: typeof packet?.type === 'number' ? packet.type : 2,
        data: packet?.data,
        nsp: packet?.nsp ?? nspName,
      },
      rooms: Array.from(opts.rooms),
      except: opts.except ? Array.from(opts.except) : [],
      ...(opts.flags ? { flags: opts.flags } : {}),
    };

    void this.core.broadcast(CLUSTER_CHANNEL, envelope).catch(() => undefined);
  }

  private onRemoteBroadcast(msg: StreamCastMessage): void {
    // Origin node already called `super.broadcast` synchronously in `broadcast()`
    if (msg.originNodeId === this.core.nodeId) {
      return;
    }

    const env = msg.payload as SerializedSocketIoBroadcast | null;
    if (!env || typeof env !== 'object' || !env.packet) {
      return;
    }

    const reconstructedOpts: BroadcastOptions = {
      rooms: new Set(env.rooms ?? []),
      except: new Set(env.except ?? []),
      ...(env.flags ? { flags: env.flags } : {}),
    };

    super.broadcast(env.packet, reconstructedOpts);
  }

  override async sockets(rooms: Set<Room>): Promise<Set<SocketId>> {
    await this.core.ready();
    if (rooms.size === 0) {
      const all = await this.core.listMembers(ALL_SIDS_ROOM);
      return new Set(all);
    }

    const lists = await Promise.all(Array.from(rooms).map((room) => this.core.listMembers(room)));
    const out = new Set<SocketId>();
    for (const list of lists) {
      for (const sid of list) {
        if (sid !== this.listenerSessionId && !sid.startsWith('__node_listener__:')) {
          out.add(sid);
        }
      }
    }
    return out;
  }

  override async serverCount(): Promise<number> {
    await this.core.ready();
    const listeners = await this.core.listMembers(CLUSTER_CHANNEL);
    return Math.max(1, listeners.length);
  }
}

/**
 * Factory returning a Socket.io adapter constructor configured for Redis Streams.
 *
 * @experimental See {@link StreamCastSocketIoAdapter} for supported features and limitations.
 */
export function createSocketIoAdapter(options: SocketIoStreamCastOptions) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return function createAdapterForNamespace(nsp: any): StreamCastSocketIoAdapter {
    return new StreamCastSocketIoAdapter(nsp, options);
  };
}
