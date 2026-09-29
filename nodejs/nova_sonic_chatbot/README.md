# Vonage Video Connector Node.js Nova Sonic Chatbot

This example adds an AI bot participant to a Vonage Video session. The bot streams the mixed audio of the other participants to [AWS Nova Sonic](https://docs.aws.amazon.com/nova/latest/userguide/speech.html) and publishes the generated speech back into the session. Nova Sonic performs turn detection and barge-in, so the sample runs no local voice activity detection.

Set `HEYGEN_API_KEY` to publish a lip-synced [HeyGen LiveAvatar](https://docs.liveavatar.com) video track alongside the audio.

## Prerequisites

- Docker
- Node.js 22.18 or later, which runs the TypeScript entrypoint without a build step
- Access to the `amazon.nova-2-sonic-v1:0` model in Amazon Bedrock
- AWS credentials resolvable by the default AWS SDK provider chain
- Access to a Vonage Video session

## Files

| File | Contents |
|---|---|
| `main.ts` | Entrypoint: arguments, signals, shutdown |
| `nova_sonic_bot.ts` | Configuration and wiring between the two sides |
| `vonage_video_transport.ts` | Vonage Video Connector plumbing |
| `nova_sonic_client.ts` | Bedrock bidirectional streaming protocol |
| `heygen_avatar.ts` | Optional HeyGen LiveAvatar video layer |

## Run with Docker

Run these commands from the `nodejs/nova_sonic_chatbot/` directory.

1. Build the image:

   ```bash
   docker build -t vonage-video-connector-nodejs-nova-sonic-chatbot .
   ```

2. Create an `env_file` with your AWS credentials, in Docker format without an `export` prefix:

   ```env
   AWS_ACCESS_KEY_ID=your-access-key-id
   AWS_SECRET_ACCESS_KEY=your-secret-access-key
   AWS_SESSION_TOKEN=your-session-token
   AWS_REGION=us-east-1
   ```

3. Create `session.json` with session credentials:

   ```json
   {
     "apiKey": "<your-api-key>",
     "sessionId": "<your-session-id>",
     "token": "<your-token>"
   }
   ```

4. Start the bot:

   ```bash
   docker run --rm -it \
     --env-file env_file \
     vonage-video-connector-nodejs-nova-sonic-chatbot \
     "$(cat session.json)"
   ```

5. Join the same session from a Vonage Video client and talk to the bot. Press `Ctrl+C` to disconnect and exit.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | Region hosting the Nova Sonic model. `AWS_DEFAULT_REGION` also applies |
| `NOVA_SONIC_MODEL_ID` | `amazon.nova-2-sonic-v1:0` | Bedrock model id |
| `NOVA_SONIC_VOICE_ID` | `tiffany` | Output voice |
| `NOVA_SONIC_GREETING` | `Tell me a fun fact!` | Prompt that makes the bot speak first. Set it to an empty string to stay silent |
| `NOVA_SONIC_ENDPOINTING_SENSITIVITY` | `HIGH` | How quickly the model detects the end of a turn. `HIGH` responds fastest but can cut off slower speakers, `MEDIUM` is the setting AWS recommends, `LOW` waits longest |

To keep the bot silent until someone speaks to it, pass an empty greeting:

```bash
docker run --rm -it \
  --env-file env_file -e NOVA_SONIC_GREETING= \
  vonage-video-connector-nodejs-nova-sonic-chatbot \
  "$(cat session.json)"
```

> [!NOTE]
> The first-generation model `amazon.nova-sonic-v1:0` ignores the text greeting and does not support
> endpointing sensitivity, so it never speaks first. To use it, add `-e NOVA_SONIC_ENDPOINTING_SENSITIVITY=`.

## Optional avatar video

The bot publishes audio only until you set `HEYGEN_API_KEY`. Get the key from
[app.liveavatar.com](https://app.liveavatar.com); keys issued by `api.heygen.com` do not work.

| Variable | Default | Description |
|---|---|---|
| `HEYGEN_API_KEY` | _(unset)_ | Enables LiveAvatar video |
| `HEYGEN_AVATAR_ID` | `513fd1b7-7ef9-466d-9af2-344e51eeb833` (Ann Therapist) | Avatar to render |

Add `HEYGEN_API_KEY` to `env_file`, then run the bot normally. LiveAvatar sessions consume credits and
have a maximum duration. The default avatar renders at 1280x720. If another avatar uses a different
resolution, update `AVATAR_VIDEO_SETTINGS` in `nova_sonic_bot.ts`.

## Run on a Linux Host

From the `nodejs/nova_sonic_chatbot/` directory, install the dependencies from npm and start the bot:

```bash
npm install
npm start -- "$(cat session.json)"
```

Node.js removes the TypeScript annotations as it loads each file, so `main.ts` runs without a compile
step. To check the types, run the compiler separately:

```bash
npm run typecheck
```

## Flow

1. The bot connects to the session and receives mixed participant audio at 16 kHz mono.
2. It sends that audio to Nova Sonic and receives generated speech at 24 kHz.
3. It publishes the speech directly, or routes it through LiveAvatar to publish synchronized audio and video when `HEYGEN_API_KEY` is set.
4. On barge-in it drops queued bot media so the bot stops immediately.
5. It logs user and assistant transcripts to the console.
