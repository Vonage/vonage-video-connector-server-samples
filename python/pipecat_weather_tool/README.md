
# LLM tool calls with Vonage Video Connector transport for pipecat

This example adds an AWS Nova Sonic bot to a Vonage Video session and gives it one tool,
`get_current_weather`. Ask the bot about the weather somewhere and it calls the tool rather
than guessing.

The tool reads live data from [Open-Meteo](https://open-meteo.com/), which needs no API key.

## Requirements

This example is meant to be run inside the Docker image built from the Dockerfile in this folder.

**You need valid AWS credentials in the environment for the bot to reach the Nova Sonic service.**

## Usage

1. Build the Docker image:

```bash
docker build -t vonage-weather-tool .
```

2. Get a session to join. Any room on `meet.tokbox.com` returns the credentials as JSON:

```bash
curl -s -H 'Accept: application/json' https://meet.tokbox.com/test-weather > session.json
```

The response carries `apiKey`, `sessionId` and `token`, which is what the example expects.
A token lasts 24 hours, so re-run the curl when it expires.

3. Get AWS credentials. The `native-sdk-wrappers` repo has a script that borrows the
   sip-endpoint IRSA role and writes them in Docker env-file format. It needs Teleport access
   plus the `aws`, `kubectl` and `jq` CLIs:

```bash
~/path/to/native-sdk-wrappers/scripts/get_aws_creds.sh --output-format docker --file env_file
```

Run it from this folder so `env_file` lands next to the sample. The credentials are temporary,
so expect to repeat this between sessions. If you would rather write it by hand:

```env
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
AWS_SESSION_TOKEN=your-session-token
AWS_REGION=your-aws-region
```

4. Run the example, mounting the source code and a uv cache directory:

```bash
docker run --rm -it \
    -v .:/app \
    -v ./uv-cache:/root/.cache/uv \
    -v vonage-weather-tool-venv:/app/.venv \
    --env-file env_file \
    -e API_HOSTNAME=api.dev.opentok.com \
    vonage-weather-tool \
    "$(cat session.json)"
```

The mounted directories ensure that Python dependencies are cached on your machine and don't need to be re-downloaded on each run.
The named volume `vonage-weather-tool-venv` keeps the virtual environment inside Docker, which is required on macOS to avoid a venv built for the wrong platform.

## What to Expect

Open https://meet.tokbox.com/test-weather in a browser, the same room you curled for `session.json`,
and ask something like "what is the weather in Barcelona?". The logs show the tool call and its
result, and the bot answers with the live temperature and conditions.

Three parts are worth reading in `pipecat_weather_tool.py`, because they are what a declarative
tool definition would have to express:

- `WEATHER_TOOL` is the `FunctionSchema` the model sees: name, description, input properties, required fields.
- `fetch_current_weather` is the execution: an outbound HTTP call with a timeout.
- `make_weather_handler` is the binding between the two, plus argument checks and the error contract.

Every call returns exactly one terminal object. Success looks like:

```json
{"ok": true, "location": "Barcelona, Spain", "temperature": 18.4, "unit": "celsius", "conditions": "partly cloudy"}
```

Failure uses the same shape with a machine-readable code, so the model has something to say and
the pipeline never waits on a call that already finished:

```json
{"ok": false, "error": {"code": "TOOL_TIMEOUT", "message": "The weather service did not respond in time.", "retryable": true}}
```

Pipecat cancels the call if the user interrupts the bot while it runs, which is the default.
Registering it with `cancel_on_interruption=False` instead makes it an async tool: pipecat then
appends standing guidance to the system instruction telling the model its results may arrive on a
later turn. That is right for a slow side-effecting call and wrong for a sub-second lookup.
