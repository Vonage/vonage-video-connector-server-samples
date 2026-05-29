# Vonage Video Echo Server Example

This example provides two variants of an echo server that connects to a Vonage Video session and echoes received audio and video back to the session.

## Variants

### 1. Simple echo server (`vonage_video_echo_server.py`)

Echoes all received audio and video back to the session immediately, frame by frame, in real time. Use this when you want the lowest-latency pass-through with no processing.

### 2. VAD echo server (`vonage_video_echo_server_vad.py`)

Uses Voice Activity Detection (VAD) to detect speech boundaries. Audio and the corresponding video frames are buffered for the duration of each utterance; once a brief silence is detected the complete segment — audio and video — is echoed back together so the two streams remain in sync.

While an utterance is being buffered, the published video track holds a still of the last frame captured before speech began (rather than going black or leaking the live video prematurely). This makes the VAD variant a better fit when you need the echo to feel like a coherent, lip-synced response rather than a continuous feed.

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

3. Run the **simple** echo server:

   ```bash
   docker run --rm -it \
       -v "$(pwd)":/app \
       -v "$(pwd)/uv-cache":/root/.cache/uv \
       -v vonage-echo-server-venv:/app/.venv \
       vonage-echo-server \
       vonage_video_echo_server.py \
       "$(cat session.json)"
   ```

   Or run the **VAD** echo server:

   ```bash
   docker run --rm -it \
       -v "$(pwd)":/app \
       -v "$(pwd)/uv-cache":/root/.cache/uv \
       -v vonage-echo-server-venv:/app/.venv \
       vonage-echo-server \
       vonage_video_echo_server_vad.py \
       "$(cat session.json)"
   ```

4. To stop the server, press **Enter** or **Ctrl+C**. The container will disconnect from the session and exit.

## How It Works

Both variants follow the same lifecycle:

1. **Connect** to the Vonage Video session using the provided credentials.
2. **Subscribe** to incoming audio and video streams from other participants.
3. **Publish** a stream back into the session carrying the echoed audio and video.

The difference is in step 3:

- **Simple**: frames are forwarded to the publisher immediately as they arrive.
- **VAD**: audio frames are analysed by `webrtcvad`. Speech frames are buffered together with the video frames captured during the same interval. When silence is detected, the full utterance (audio + video) is sent to the publisher as a unit. Between utterances the published video track shows a frozen still of the last frame received before speech began.
