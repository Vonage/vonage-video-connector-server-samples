# Copyright 2026 Vonage
"""AWS Nova Sonic + Vonage Video Connector example with optional HeyGen AI avatar.

This example demonstrates how to use the Pipecat framework to add an AI bot
participant to a Vonage Video session using AWS Nova Sonic as the speech-to-speech
LLM service.  The bot listens to other participants and responds with synthesised
speech.

Optionally, the bot can be represented visually by a HeyGen LiveAvatar.
Set the HEYGEN_API_KEY environment variable to enable it.

Environment variables:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    AWS_SESSION_TOKEN          - AWS credentials for Nova Sonic.
    HEYGEN_API_KEY (optional)  - Enables the HeyGen LiveAvatar.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Callable

import aiohttp

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.frames.frames import LLMRunFrame
from pipecat.observers.loggers.transcription_log_observer import TranscriptionLogObserver
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.services.aws.nova_sonic.llm import AWSNovaSonicLLMService, Params as NovaSonicParams
from pipecat.services.heygen.api_liveavatar import LiveAvatarNewSessionRequest
from pipecat.services.heygen.client import ServiceType
from pipecat.services.heygen.video import HeyGenVideoService
from pipecat.transports.vonage.video_connector import (
    VonageVideoConnectorTransport,
    VonageVideoConnectorTransportParams,
)

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stderr,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Stock / demo avatar ID
HEYGEN_AVATAR_ID = "513fd1b7-7ef9-466d-9af2-344e51eeb833"  # Ann Therapist


async def main(session_str: str) -> None:
    """Run the Nova Sonic pipeline inside a Vonage Video session.

    Connects to the Vonage Video session described by *session_str*, sets up the
    AWS Nova Sonic LLM, and optionally enables a HeyGen LiveAvatar for video
    output.  The pipeline runs until the session ends.

    Args:
        session_str: JSON string containing ``apiKey``, ``sessionId``, and ``token``.
    """
    system_instruction = (
        "You are a friendly assistant. The user and you will engage in a spoken dialog exchanging "
        "the transcripts of a natural real-time conversation. Keep your responses short, generally "
        "two or three sentences for chatty scenarios."
    )
    chans = 1
    in_sr = 16000
    out_sr = 24000

    session_obj = json.loads(session_str)
    application_id = session_obj.get("apiKey", "")
    session_id = session_obj.get("sessionId", "")
    token = session_obj.get("token", "")

    heygen_api_key = os.getenv("HEYGEN_API_KEY", "")
    has_video = bool(heygen_api_key)

    transport = VonageVideoConnectorTransport(
        application_id,
        session_id,
        token,
        VonageVideoConnectorTransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(params=VADParams(start_secs=0.01, stop_secs=0.01)),
            publisher_name="TTS bot",
            audio_in_sample_rate=in_sr,
            audio_in_channels=chans,
            audio_out_sample_rate=out_sr,
            audio_out_channels=chans,
            video_out_enabled=has_video,
            video_out_color_format="RGB",
            video_out_framerate=30,
            video_out_is_live=True,
            video_out_width=1280,
            video_out_height=720,
        ),
    )

    ns_params = NovaSonicParams()
    ns_params.input_sample_rate = in_sr
    ns_params.output_sample_rate = out_sr
    ns_params.input_channel_count = chans
    ns_params.output_channel_count = chans
    ns_params.endpointing_sensitivity = "HIGH"

    llm = AWSNovaSonicLLMService(
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        region=os.getenv("AWS_REGION", ""),
        session_token=os.getenv("AWS_SESSION_TOKEN", ""),
        voice_id="tiffany",
        params=ns_params,
    )
    context = LLMContext(
        messages=[
            {"role": "system", "content": f"{system_instruction}"},
            {
                "role": "user",
                "content": "Tell me a fun fact!",
            },
        ],
    )
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(context)

    avatar: None | HeyGenVideoService = None
    async with aiohttp.ClientSession() as http_session:
        if heygen_api_key:
            logger.info("Using HeyGen LiveAvatar")
            avatar = HeyGenVideoService(
                api_key=heygen_api_key,
                session=http_session,
                service_type=ServiceType.LIVE_AVATAR,
                session_request=LiveAvatarNewSessionRequest(
                    avatar_id=HEYGEN_AVATAR_ID,
                ),
            )

        pipeline = Pipeline(
            [
                transport.input(),
                user_aggregator,
                llm,
                *([avatar] if avatar is not None else []),
                transport.output(),
                assistant_aggregator,
            ]
        )

        task = PipelineTask(
            pipeline, params=PipelineParams(enable_metrics=True), observers=[TranscriptionLogObserver()]
        )

        # Handle client connection events
        event_handler: Callable[[str], Callable[[Any], Any]] = transport.event_handler

        @event_handler("on_client_connected")
        async def on_client_connected(transport: VonageVideoConnectorTransport, client: object) -> None:
            logger.info("Client connected")
            await task.queue_frames([LLMRunFrame()])

        @event_handler("on_client_disconnected")
        async def on_client_disconnected(transport: VonageVideoConnectorTransport, client: object) -> None:
            logger.info("Client disconnected — shutting down")
            await task.cancel()

        runner = PipelineRunner()
        await asyncio.gather(runner.run(task))


def cli_main() -> None:
    """Parse CLI arguments and launch the pipeline.

    Expects a single positional argument: a JSON string with Vonage Video session
    credentials (``apiKey``, ``sessionId``, ``token``).
    """
    if len(sys.argv) > 1:
        session_str = sys.argv[1]
        try:
            session = json.loads(session_str)
            logger.info(
                "Session received: apiKey=%s, sessionId=%s, token=%s",
                session.get("apiKey"),
                session.get("sessionId"),
                "<redacted>" if "token" in session else None,
            )
        except json.JSONDecodeError:
            logger.error("Invalid session JSON CLI argument: expected a valid JSON string.")
            logger.error(f"Usage: {sys.argv[0]} <session_json>")
            logger.error("The session_json argument should be a JSON string with the following format:")
            logger.error('{"apiKey": "your_api_key", "sessionId": "your_session_id", "token": "your_token"}')
            sys.exit(1)
    else:
        logger.error(f"Usage: {sys.argv[0]} <session_json>")
        logger.error("The session_json argument should be a JSON string with the following format:")
        logger.error('{"apiKey": "your_api_key", "sessionId": "your_session_id", "token": "your_token"}')
        sys.exit(1)

    asyncio.run(main(session_str))


if __name__ == "__main__":
    cli_main()
