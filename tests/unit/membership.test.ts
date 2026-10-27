import { describe, expect, it } from 'vitest';
import { BoundedSet } from '../../src/core/dedupe.js';
import { RoomMembership } from '../../src/core/membership.js';

describe('RoomMembership & BoundedSet unit tests', () => {
  it('tracks bidirectional session <-> room membership accurately', () => {
    const m = new RoomMembership();

    expect(m.join('s1', 'room-a')).toBe(true);
    expect(m.join('s1', 'room-a')).toBe(false); // idempotent
    expect(m.join('s1', 'room-b')).toBe(true);
    expect(m.join('s2', 'room-a')).toBe(true);

    expect(m.localRoomSize('room-a')).toBe(2);
    expect(m.localRoomSize('room-b')).toBe(1);
    expect(m.localRoomSize('nonexistent')).toBe(0);

    expect(Array.from(m.getRoomMembers('room-a')).sort()).toEqual(['s1', 's2']);
    expect(Array.from(m.getSessionRooms('s1')).sort()).toEqual(['room-a', 'room-b']);

    expect(m.leave('s1', 'room-a')).toBe(true);
    expect(m.leave('s1', 'room-a')).toBe(false);
    expect(m.hasMember('s1', 'room-a')).toBe(false);
    expect(m.localRoomSize('room-a')).toBe(1);

    const left = m.leaveAll('s1');
    expect(left).toEqual(['room-b']);
    expect(m.localRoomSize('room-b')).toBe(0);
    expect(m.getActiveRooms()).toEqual(['room-a']);

    m.leave('s2', 'room-a');
    expect(m.roomCount).toBe(0);
    expect(m.sessionCount).toBe(0);
  });

  it('evicts oldest entries in BoundedSet when capacity is reached', () => {
    const dedupe = new BoundedSet<string>(3);
    expect(dedupe.add('1-0')).toBe(true);
    expect(dedupe.add('2-0')).toBe(true);
    expect(dedupe.add('3-0')).toBe(true);
    expect(dedupe.add('2-0')).toBe(false); // already exists

    // Adding 4th entry evicts '1-0'
    expect(dedupe.add('4-0')).toBe(true);
    expect(dedupe.has('1-0')).toBe(false);
    expect(dedupe.has('2-0')).toBe(true);
    expect(dedupe.has('3-0')).toBe(true);
    expect(dedupe.has('4-0')).toBe(true);
    expect(dedupe.size).toBe(3);
  });
});
