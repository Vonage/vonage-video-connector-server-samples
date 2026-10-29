# Moondream with Vonage Video Connector transport for pipecat example
This example shows how to use the Vonage Video Connector transport with Moondream in pipecat.

## Requirements

*IMPORTANT* Moondream runs best with a GPU. If you run this example on a machine without a GPU or in a docker container
without access to the GPU (e.g. docker on OSX), you will have to wait much longer for an image analysis to finish.

## Usage

1. Create a `session.json` file with your Vonage Video session credentials with the following format:

```json
{
  "apiKey": "your-api-key",
  "sessionId": "your-session-id", 
  "token": "your-session-token"
}
```

2. Run the example script with the session file as an argument:

```bash
uv run python pipecat_moondream.py "$(cat session.json)"
```

## Running with Docker

1. Build the image:

```bash
docker build -t pipecat-moondream .
```

2. Run the container, mounting the source code, a uv cache directory, and a Moondream model cache directory:

```bash
docker run --rm -it \
  -v .:/app \
  -v ./uv-cache:/root/.cache/uv \
  -v ./model-cache:/root/.cache/huggingface/hub \
  -v vonage-moondream-venv:/app/.venv \
  pipecat-moondream "$(cat session.json)"
```

The container will run `uv sync` at startup before launching the script. The mounted directories ensure that both Python dependencies and the Moondream model persist across runs and don't need to be re-downloaded each time.

## What to Expect
This example will subscribe to all the session's video streams and randomly select one frame from one of them,
 analyze it using Moondream, and speak out loud the description of the image using a text-to-speech engine (Piper, running locally).

Then it will repeat the process.
