# Vonage Video Echo Server Example

This example provides a simple echo server that connects to a Vonage Video session and echoes received audio and video back to the session in real time, frame by frame.

## Pre-requisites

- Docker

## Usage

All commands below should be run from the `echo_server/` directory.

1. Build the Docker image:

   ```bash
   docker build -t vonage-echo-server .
   ```

2. Obtain session credentials as a JSON object with the following fields:

   ```json
   {
     "apiKey": "<your-api-key>",
     "sessionId": "<your-session-id>",
     "token": "<your-token>"
   }
   ```

   Save it to a file or pass it inline.

3. Run the echo server:

   ```bash
   docker run --rm -it \
       -v "$(pwd)":/app \
       -v "$(pwd)/uv-cache":/root/.cache/uv \
       -v vonage-echo-server-venv:/app/.venv \
       vonage-echo-server \
       vonage_video_echo_server.py \
       "$(cat session.json)"
   ```

4. To stop the server, press **Enter** or **Ctrl+C**. The container will disconnect from the session and exit.

## How It Works

1. **Connect** to the Vonage Video session using the provided credentials.
2. **Subscribe** to incoming audio and video streams from other participants.
3. **Publish** a stream back into the session carrying the echoed audio and video.
