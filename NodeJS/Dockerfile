FROM node:22-bookworm-slim

WORKDIR /app

COPY echo_server/package.json echo_server/tsconfig.json echo_server/
RUN npm install --prefix echo_server
COPY echo_server/vonage_video_echo_server.ts echo_server/

WORKDIR /app/echo_server
ENTRYPOINT ["npm", "run", "start", "--prefix", "/app/echo_server", "--"]
