# Vonage Video Connector Node.js Echo Server

This example connects to a Vonage Video session, echoes incoming audio, and republishes incoming video as grayscale at 640x480.

## Prerequisites

- Docker
- Node.js 18.17 or later
- Access to a Vonage Video session

## Run with Docker

Run these commands from the `NodeJS/` directory.

1. Build the image:

   ```bash
   docker build -t vonage-video-connector-nodejs-echo-server .
   ```

   The image defaults to Node.js 22. Build with another supported version using `--build-arg NODE_IMAGE=node:20-bookworm-slim`.

2. Create `session.json` with session credentials:

   ```json
   {
     "apiKey": "<your-api-key>",
     "sessionId": "<your-session-id>",
     "token": "<your-token>"
   }
   ```

3. Start the echo server:

   ```bash
   docker run --rm -it \
     vonage-video-connector-nodejs-echo-server \
     "$(cat session.json)"
   ```

4. Press `Ctrl+C` to disconnect and exit.

## Run on a Linux Host

From the `NodeJS/` directory, install the sample dependencies from npm and run the server:

```bash
npm install --prefix echo_server
npm --prefix echo_server run start -- "$(cat session.json)"
```

## Flow

1. The server connects with the provided session credentials.
2. It subscribes to incoming streams and queues audio and video frames.
3. It publishes the audio frames and grayscale video frames into the same session.
