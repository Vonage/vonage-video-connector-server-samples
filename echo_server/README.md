# Vonage Video Echo Server Example

This example shows an echo server that connects to a Vonage Video session and echoes back audio using Voice Activity Detection (VAD). Audio is buffered while the remote participant is speaking and played back once speech ends.

## Pre-requisites

- Python ~3.13
- [uv](https://docs.astral.sh/uv/) package manager

## Usage

1. Install dependencies:

   ```bash
   uv sync
   ```

2. Create a `session.json` file with your Vonage Video session credentials:

   ```json
   {
     "apiKey": "your-api-key",
     "sessionId": "your-session-id",
     "token": "your-session-token"
   }
   ```

3. Run the example:

   ```bash
   uv run python vonage_video_echo_server.py session.json
   ```

   You can also pass the session credentials as a JSON string directly:

   ```bash
   uv run python vonage_video_echo_server.py '{"apiKey":"...","sessionId":"...","token":"..."}'
   ```

## How It Works

1. **Connects** to the Vonage Video session using the provided credentials.
2. **Subscribes** to incoming audio streams from other participants.
3. **Buffers** audio frames while Voice Activity Detection (VAD) detects speech.
4. **Echoes** the buffered audio back to the session once the speaker stops talking.
