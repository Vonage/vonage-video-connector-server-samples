
# Nova Sonic with Vonage Video Connector transport for pipecat example

This example shows how to use the Vonage Video Connector transport with Nova Sonic in pipecat.

Optionally you can enable a **HeyGen** LiveAvatar to represent the bot visually in the video session.

## Requirements

This example is meant to be run inside the Docker image built from the Dockerfile in this folder.

**You need valid AWS credentials in the environment for the bot to reach the Nova Sonic service.**

## Usage

1. Build the Docker image:

```bash
docker build -t vonage-nova-sonic .
```

2. Create a `session.json` file with your Vonage Video session credentials:

```json
{
  "apiKey": "your-api-key",
  "sessionId": "your-session-id",
  "token": "your-session-token"
}
```

3. Create an `env_file` with your AWS credentials (no `export` prefix — Docker format):

```env
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
AWS_SESSION_TOKEN=your-session-token
AWS_REGION=your-aws-region
```

4. (Optional) Enable the HeyGen LiveAvatar by adding the following to `env_file`:

```env
HEYGEN_API_KEY=your-heygen-api-key
```

5. Run the example, mounting the source code and a uv cache directory:

```bash
docker run --rm -it \
    -v .:/app \
    -v ./uv-cache:/root/.cache/uv \
    -v vonage-nova-sonic-venv:/app/.venv \
    --env-file env_file \
    vonage-nova-sonic \
    "$(cat session.json)"
```

The mounted directories ensure that Python dependencies are cached on your machine and don't need to be re-downloaded on each run.
The named volume `vonage-nova-sonic-venv` keeps the virtual environment inside Docker — this is required on macOS to avoid a venv built for the wrong platform.

## What to Expect

This example demonstrates how to use pipecat to add an AWS Nova Sonic LLM bot participant to a Vonage Video session.
Participants in the session will be able to talk to the bot and hear its responses.
If a HeyGen API key is provided, the bot will also appear as a video avatar in the session.
