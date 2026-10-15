# Copyright 2026 Vonage
"""AWS Nova Sonic + Vonage Video Connector example with an LLM tool call.

The bot joins a Vonage Video session and answers questions about the current
weather by calling a tool. The tool resolves the requested place name and reads
the current conditions from the Open-Meteo API, which needs no credentials.

The tool returns a single terminal result object on both success and failure, so
the model always has something to say and the pipeline never waits on a call that
has already finished.

Environment variables:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    AWS_SESSION_TOKEN          - AWS credentials for Nova Sonic.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Callable

import aiohttp

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.frames.frames import LLMRunFrame
from pipecat.observers.loggers.transcription_log_observer import TranscriptionLogObserver
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker, ProcessorUnusablePolicy
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.services.aws.nova_sonic.llm import AudioConfig, AWSNovaSonicLLMService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.vonage.video_connector import (
    VonageVideoConnectorTransport,
    VonageVideoConnectorTransportParams,
)
from pipecat.workers.runner import WorkerRunner

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stderr,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

for _noisy_logger in ("smithy_core", "smithy_aws_event_stream", "smithy_aws_core"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_TIMEOUT_SECONDS = 5

# Condensed WMO weather interpretation codes used by Open-Meteo.
WEATHER_CODES = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "freezing fog",
    51: "light drizzle",
    53: "drizzle",
    55: "heavy drizzle",
    61: "light rain",
    63: "rain",
    65: "heavy rain",
    71: "light snow",
    73: "snow",
    75: "heavy snow",
    80: "rain showers",
    95: "thunderstorm",
    99: "thunderstorm with hail",
}

WEATHER_TOOL = FunctionSchema(
    name="get_current_weather",
    description="Get the current weather for a place.",
    properties={
        "location": {
            "type": "string",
            "description": "The city and country, for example Barcelona, Spain.",
        },
        "format": {
            "type": "string",
            "enum": ["celsius", "fahrenheit"],
            "description": "The temperature unit to use. Infer it from the location.",
        },
    },
    required=["location", "format"],
)


def tool_error(code: str, message: str, retryable: bool) -> dict[str, Any]:
    """Build the terminal error result handed back to the model.

    Args:
        code: Machine-readable failure reason.
        message: Short sentence the model can read out.
        retryable: Whether calling the tool again could succeed.
    """
    return {"ok": False, "error": {"code": code, "message": message, "retryable": retryable}}


async def fetch_current_weather(
    http_session: aiohttp.ClientSession, location: str, unit: str
) -> dict[str, Any]:
    """Resolve *location* and read its current weather from Open-Meteo.

    Args:
        http_session: Shared HTTP session.
        location: Place name supplied by the model.
        unit: Either ``celsius`` or ``fahrenheit``.

    Returns:
        A terminal result object, successful or otherwise.
    """
    timeout = aiohttp.ClientTimeout(total=WEATHER_TIMEOUT_SECONDS)

    async with http_session.get(
        GEOCODING_URL, params={"name": location, "count": 1}, timeout=timeout
    ) as response:
        response.raise_for_status()
        places = (await response.json()).get("results") or []

    if not places:
        return tool_error("LOCATION_NOT_FOUND", f"I could not find a place called {location}.", False)

    place = places[0]
    async with http_session.get(
        FORECAST_URL,
        params={
            "latitude": place["latitude"],
            "longitude": place["longitude"],
            "current": "temperature_2m,weather_code",
            "temperature_unit": unit,
        },
        timeout=timeout,
    ) as response:
        response.raise_for_status()
        current = (await response.json())["current"]

    resolved = ", ".join(p for p in (place.get("name"), place.get("country")) if p)
    return {
        "ok": True,
        "location": resolved,
        "temperature": current["temperature_2m"],
        "unit": unit,
        "conditions": WEATHER_CODES.get(current["weather_code"], "unknown conditions"),
    }


def make_weather_handler(
    http_session: aiohttp.ClientSession,
) -> Callable[[FunctionCallParams], Any]:
    """Build the registered handler for ``get_current_weather``.

    Args:
        http_session: Shared HTTP session used by the tool.
    """

    async def handler(params: FunctionCallParams) -> None:
        location = str(params.arguments.get("location", "")).strip()
        unit = params.arguments.get("format", "celsius")

        if not location:
            await params.result_callback(
                tool_error("INVALID_ARGUMENTS", "I need a place name to check the weather.", False)
            )
            return
        if unit not in ("celsius", "fahrenheit"):
            unit = "celsius"

        logger.info("Tool call %s: get_current_weather(%s)", params.tool_call_id, location)
        try:
            result = await fetch_current_weather(http_session, location, unit)
        except asyncio.TimeoutError:
            result = tool_error("TOOL_TIMEOUT", "The weather service did not respond in time.", True)
        except aiohttp.ClientError as error:
            logger.warning("Weather lookup failed: %s", error)
            result = tool_error("UPSTREAM_ERROR", "The weather service is unavailable.", True)
        except (KeyError, ValueError) as error:
            logger.warning("Unexpected weather response: %s", error)
            result = tool_error("INVALID_RESPONSE", "The weather service sent something I could not read.", False)

        logger.info("Tool result %s: %s", params.tool_call_id, result)
        await params.result_callback(result)

    return handler


async def main(session_str: str) -> None:
    """Run the weather-tool pipeline inside a Vonage Video session.

    Args:
        session_str: JSON string containing ``apiKey``, ``sessionId``, and ``token``.
    """
    system_instruction = (
        "You are a friendly assistant. The user and you will engage in a spoken dialog exchanging "
        "the transcripts of a natural real-time conversation. Keep your responses short, generally "
        "two or three sentences for chatty scenarios. Call get_current_weather whenever the user "
        "asks about the weather, and do not guess a temperature. If the tool reports an error, say "
        "what went wrong instead of inventing an answer."
    )
    chans = 1
    in_sr = 16000
    out_sr = 24000

    session_obj = json.loads(session_str)
    application_id = session_obj.get("apiKey", "")
    session_id = session_obj.get("sessionId", "")
    token = session_obj.get("token", "")

    transport = VonageVideoConnectorTransport(
        application_id,
        session_id,
        token,
        VonageVideoConnectorTransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            publisher_name="Weather bot",
            audio_in_sample_rate=in_sr,
            audio_in_channels=chans,
            audio_out_sample_rate=out_sr,
            audio_out_channels=chans,
        ),
    )

    llm = AWSNovaSonicLLMService(
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        region=os.getenv("AWS_REGION", ""),
        session_token=os.getenv("AWS_SESSION_TOKEN", ""),
        audio_config=AudioConfig(
            input_sample_rate=in_sr,
            input_channel_count=chans,
            output_sample_rate=out_sr,
            output_channel_count=chans,
        ),
        settings=AWSNovaSonicLLMService.Settings(
            voice="tiffany",
            endpointing_sensitivity="HIGH",
            system_instruction=system_instruction,
        ),
    )

    context = LLMContext(
        messages=[
            {
                "role": "user",
                "content": "Say hello and mention you can check the weather anywhere.",
            },
        ],
        tools=ToolsSchema(standard_tools=[WEATHER_TOOL]),
    )
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(context)

    async with aiohttp.ClientSession() as http_session:
        llm.register_function("get_current_weather", make_weather_handler(http_session))

        pipeline = Pipeline(
            [
                transport.input(),
                user_aggregator,
                llm,
                transport.output(),
                assistant_aggregator,
            ]
        )

        worker = PipelineWorker(
            pipeline,
            params=PipelineParams(enable_metrics=True),
            observers=[TranscriptionLogObserver()],
            processor_unusable_policy=ProcessorUnusablePolicy.CANCEL,
        )

        # Handle client connection events
        event_handler: Callable[[str], Callable[[Any], Any]] = transport.event_handler

        @event_handler("on_client_connected")
        async def on_client_connected(transport: VonageVideoConnectorTransport, client: object) -> None:
            logger.info("Client connected")
            await worker.queue_frames([LLMRunFrame()])

        @event_handler("on_client_disconnected")
        async def on_client_disconnected(transport: VonageVideoConnectorTransport, client: object) -> None:
            logger.info("Client disconnected, shutting down")
            await worker.cancel()

        runner = WorkerRunner()
        await runner.add_workers(worker)
        await runner.run()


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
