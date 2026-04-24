# Vonage Video Connector Server Samples 

This bundle provides examples using the `vonage_video_connector` module.

## What's Included

- [Python Echo server](./echo_server/): An example application that demonstrates audio echoing in video sessions
- [Pipecat with AWS Nova Sonic and optional HeyGen AI Avatar bot](./pipecat_nova_sonic/): Integration with Nova Sonic for an example of pipecat using Vonage to interact with an AWS Nova Sonic bot
- [Pipecat with Moondream live video analysis](./pipecat_moondream/): Integration with Moondream for live video analysis using Vonage Video sessions. Moondream descriptions are read aloud into the session using Piper TTS.

> [!NOTE]
> Each example has its own setup and run instructions — refer to the individual example folders for details.

## Prerequisites

- Python ~3.13 + [uv](https://docs.astral.sh/uv/) (for the Echo Server)
- Docker (for the Pipecat examples)
- Access to a Vonage Video session
