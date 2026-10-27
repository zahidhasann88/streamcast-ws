const EMPTY_SET: ReadonlySet<string> = Object.freeze(new Set<string>());

/**
 * In-memory per-node bidirectional room membership index.
 * Maps `room <-> sessionId` in O(1) time.
 */
export class RoomMembership {
  private readonly roomToSessions = new Map<string, Set<string>>();
  private readonly sessionToRooms = new Map<string, Set<string>>();

  /**
   * Adds `sessionId` to `room`. Returns `true` if newly added, `false` if already present.
   */
  join(sessionId: string, room: string): boolean {
    let sessions = this.roomToSessions.get(room);
    if (!sessions) {
      sessions = new Set<string>();
      this.roomToSessions.set(room, sessions);
    }
    if (sessions.has(sessionId)) {
      return false;
    }
    sessions.add(sessionId);

    let rooms = this.sessionToRooms.get(sessionId);
    if (!rooms) {
      rooms = new Set<string>();
      this.sessionToRooms.set(sessionId, rooms);
    }
    rooms.add(room);
    return true;
  }

  /**
   * Removes `sessionId` from `room`. Returns `true` if removed, `false` if not present.
   */
  leave(sessionId: string, room: string): boolean {
    const sessions = this.roomToSessions.get(room);
    if (!sessions || !sessions.has(sessionId)) {
      return false;
    }
    sessions.delete(sessionId);
    if (sessions.size === 0) {
      this.roomToSessions.delete(room);
    }

    const rooms = this.sessionToRooms.get(sessionId);
    if (rooms) {
      rooms.delete(room);
      if (rooms.size === 0) {
        this.sessionToRooms.delete(sessionId);
      }
    }
    return true;
  }

  /**
   * Removes `sessionId` from all rooms on this node and returns the rooms it left.
   */
  leaveAll(sessionId: string): string[] {
    const rooms = this.sessionToRooms.get(sessionId);
    if (!rooms) {
      return [];
    }
    const leftRooms = Array.from(rooms);
    for (const room of leftRooms) {
      const sessions = this.roomToSessions.get(room);
      if (sessions) {
        sessions.delete(sessionId);
        if (sessions.size === 0) {
          this.roomToSessions.delete(room);
        }
      }
    }
    this.sessionToRooms.delete(sessionId);
    return leftRooms;
  }

  hasMember(sessionId: string, room: string): boolean {
    return this.roomToSessions.get(room)?.has(sessionId) ?? false;
  }

  getRoomMembers(room: string): ReadonlySet<string> {
    return this.roomToSessions.get(room) ?? EMPTY_SET;
  }

  getSessionRooms(sessionId: string): ReadonlySet<string> {
    return this.sessionToRooms.get(sessionId) ?? EMPTY_SET;
  }

  localRoomSize(room: string): number {
    return this.roomToSessions.get(room)?.size ?? 0;
  }

  getActiveRooms(): string[] {
    return Array.from(this.roomToSessions.keys());
  }

  get roomCount(): number {
    return this.roomToSessions.size;
  }

  get sessionCount(): number {
    return this.sessionToRooms.size;
  }

  clear(): void {
    this.roomToSessions.clear();
    this.sessionToRooms.clear();
  }
}
