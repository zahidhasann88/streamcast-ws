import { describe, expect, it } from 'vitest';
import {
  PROTOCOL_VERSION,
  compareStreamIds,
  decodeClientFrame,
  decodeServerFrame,
  encodeFrame,
  isValidStreamId,
  maxStreamId,
  parseStreamId,
  streamIdTimestampMs,
} from '../../src/protocol/index.js';

describe('Protocol v1 & Stream ID utilities', () => {
  it('validates and compares Redis Stream IDs accurately including large sequence numbers', () => {
    expect(isValidStreamId('0-0')).toBe(true);
    expect(isValidStreamId('1761546000123-42')).toBe(true);
    expect(isValidStreamId('invalid')).toBe(false);
    expect(isValidStreamId('123-')).toBe(false);
    expect(isValidStreamId('-0')).toBe(false);

    expect(parseStreamId('1761546000123-9')).toEqual({
      ms: 1761546000123n,
      seq: 9n,
    });
    expect(streamIdTimestampMs('1761546000123-9')).toBe(1761546000123);

    expect(compareStreamIds('100-0', '100-0')).toBe(0);
    expect(compareStreamIds('100-2', '100-10')).toBe(-1); // numeric, not lexicographic!
    expect(compareStreamIds('100-10', '100-2')).toBe(1);
    expect(compareStreamIds('99-999', '100-0')).toBe(-1);

    expect(maxStreamId('100-2', '100-10')).toBe('100-10');
  });

  it('encodes and decodes valid client frames', () => {
    const helloRaw = encodeFrame({
      v: PROTOCOL_VERSION,
      type: 'hello',
      sessionId: 'sess_123',
      lastEventId: '1700000000000-1',
    });
    const hello = decodeClientFrame(helloRaw);
    expect(hello).toEqual({
      ok: true,
      frame: {
        v: 1,
        type: 'hello',
        sessionId: 'sess_123',
        lastEventId: '1700000000000-1',
      },
    });

    const ack = decodeClientFrame(encodeFrame({ v: 1, type: 'ack', id: '1700000000005-0' }));
    expect(ack).toEqual({
      ok: true,
      frame: { v: 1, type: 'ack', id: '1700000000005-0' },
    });

    const join = decodeClientFrame(encodeFrame({ v: 1, type: 'join', room: 'lobby' }));
    expect(join).toEqual({
      ok: true,
      frame: { v: 1, type: 'join', room: 'lobby' },
    });
  });

  it('rejects malformed client frames and distinguishes plain application JSON', () => {
    const badJson = decodeClientFrame('{not json');
    expect(badJson.ok).toBe(false);
    if (!badJson.ok) {
      expect(badJson.code).toBe('INVALID_JSON');
    }

    const appJson = decodeClientFrame(JSON.stringify({ chat: 'hi there' }));
    expect(appJson.ok).toBe(false);
    if (!appJson.ok) {
      expect(appJson.code).toBe('NOT_PROTOCOL_FRAME');
      expect(appJson.isNonProtocolJson).toBe(true);
    }

    const wrongVersion = decodeClientFrame(JSON.stringify({ v: 99, type: 'ack', id: '1-0' }));
    expect(wrongVersion.ok).toBe(false);
    if (!wrongVersion.ok) {
      expect(wrongVersion.code).toBe('UNSUPPORTED_VERSION');
    }

    const badAck = decodeClientFrame(JSON.stringify({ v: 1, type: 'ack', id: 'bad-id' }));
    expect(badAck.ok).toBe(false);
    if (!badAck.ok) {
      expect(badAck.code).toBe('INVALID_ACK');
    }
  });

  it('encodes and decodes server frames cleanly', () => {
    const welcome = decodeServerFrame(
      encodeFrame({
        v: 1,
        type: 'welcome',
        sessionId: 'sc_abc',
        lastEventId: '0-0',
        recovered: false,
        rooms: ['general'],
        reason: 'new_session',
      }),
    );
    expect(welcome.ok).toBe(true);

    const replay = decodeServerFrame(
      encodeFrame({
        v: 1,
        type: 'replay',
        id: '100-1',
        room: 'general',
        data: { seq: 1 },
        ts: 100,
      }),
    );
    expect(replay).toEqual({
      ok: true,
      frame: {
        v: 1,
        type: 'replay',
        id: '100-1',
        room: 'general',
        data: { seq: 1 },
        ts: 100,
      },
    });
  });
});
