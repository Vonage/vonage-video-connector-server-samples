# Vonage Video Echo Server Example

This example shows an echo server that connects to a Vonage Video session and immediately echoes back all received audio and video to the session in real time.

## Pre-requisites

- Docker

## Usage

All commands below should be run from the `echo_server/` directory.

1. Build the Docker image:

   ```bash
   docker build -t vonage-echo-server .
   ```

2. Fetch session credentials:

   ```bash
   curl https://meet.tokbox.com/echo-video > session.json
   ```

3. Run the container:

   ```bash
   docker run --rm -it \
       -v "$(pwd)":/app \
       -v "$(pwd)/uv-cache":/root/.cache/uv \
       -v vonage-echo-server-venv:/app/.venv \
       -e API_HOSTNAME=api.dev.opentok.com \
       vonage-echo-server \
       "$(cat session.json)"
   ```

4. To stop the server, press **Enter** or **Ctrl+C**. The container will disconnect from the session and exit.

## How It Works

1. **Connects** to the Vonage Video session using the provided credentials.
2. **Subscribes** to incoming audio and video streams from other participants.
3. **Echoes** all received audio and video back to the session immediately, frame by frame.
