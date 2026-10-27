import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'adapters/ws/index': 'src/adapters/ws/index.ts',
    'adapters/socketio/index': 'src/adapters/socketio/index.ts',
    'recovery/index': 'src/recovery/index.ts',
    'client/index': 'src/client/index.ts',
    'protocol/index': 'src/protocol/index.ts',
  },
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  target: 'node22',
  external: ['ioredis', 'ws', 'socket.io-adapter', 'socket.io'],
});
