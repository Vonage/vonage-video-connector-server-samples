FROM node:22-bookworm-slim

WORKDIR /app

# Avatar mode connects over WSS and needs the system trust store, which the slim image omits.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY package.json tsconfig.json ./
RUN npm install
COPY *.ts ./

ENTRYPOINT ["node", "main.ts"]
