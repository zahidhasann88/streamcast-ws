FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json tsconfig.json tsup.config.ts ./
COPY src ./src
RUN npm ci && npm run build

COPY examples/ws-chat ./examples/ws-chat

EXPOSE 4001
CMD ["node", "examples/ws-chat/server.mjs"]
