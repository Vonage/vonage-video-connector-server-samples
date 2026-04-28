# Copyright 2026 Vonage
"""Pipecat pipeline that uses Moondream for vision analysis over a Vonage Video session.

Subscribes to a Vonage Video Connector session, captures incoming video frames,
runs them through the Moondream vision model for image description, converts the
resulting text to speech via Piper TTS, and publishes audio and annotated video
back to the session.
"""

import asyncio
import json
import sys

from PIL import Image, ImageDraw, ImageFont


from loguru import logger
from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    Frame,
    InputImageRawFrame,
    InterruptionFrame,
    OutputImageRawFrame,
    SpriteFrame,
    TextFrame,
    TTSSpeakFrame,
    UserImageRawFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.services.moondream.vision import MoondreamService
from pipecat.services.piper.tts import PiperTTSService

from pipecat.transports.vonage.video_connector import (
    VonageVideoConnectorTransport,
    VonageVideoConnectorTransportParams,
)


FONT_SIZE = 20
FPS = 24

try:
    FONT: ImageFont.FreeTypeFont | ImageFont.ImageFont = ImageFont.truetype("arial.ttf", FONT_SIZE)
except IOError:
    FONT = ImageFont.load_default(FONT_SIZE)


def write_text_on_image(image: Image.Image, text: str, text_gradient: float) -> Image.Image:
    """Draw centered text at the bottom of an image with a red-green gradient color.

    Args:
        image: The PIL image to draw on (modified in place and returned).
        text: The text string to render.
        text_gradient: A value between 0.0 (green) and 1.0 (red) controlling
            the text color interpolation.

    Returns:
        The same image with the text drawn on it.
    """
    draw = ImageDraw.Draw(image)

    bbox = draw.textbbox((0, 0), text, font=FONT)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    position = ((image.width - text_width) // 2, image.height - text_height - 10)

    # Create gradient color
    r = int(255 * text_gradient)
    g = int(255 * (1 - text_gradient))
    b = 0
    gradient_color = (r, g, b)

    draw.text(position, text, fill=gradient_color, font=FONT)

    return image


def write_text_on_image_animation(frame: InputImageRawFrame, text: str, total_frames: int) -> SpriteFrame:
    """Create a looping color-cycling text animation over a static video frame.

    Generates a sprite sequence where the text color transitions from green to
    red and back, producing a pulsing visual effect.

    Args:
        frame: The source video frame used as the background for every sprite.
        text: The text to overlay on each animation frame.
        total_frames: The total number of frames in the animation cycle.

    Returns:
        A SpriteFrame containing the full animation loop.
    """
    pil_image = Image.frombytes(frame.format or "RGB", frame.size, frame.image)
    images = []
    for i in range(total_frames // 2):
        gradient = i / (total_frames // 2 - 1)
        frame_image = write_text_on_image(pil_image.copy(), text, gradient)
        images.append(frame_image)
    return SpriteFrame(
        images=[
            OutputImageRawFrame(size=frame.size, format=frame.format, image=image.tobytes())
            for image in images + images[::-1]
        ]
    )


class InputVideoMultiplexProcessor(FrameProcessor):
    """Gate that serializes vision analysis — one frame at a time.

    Accepts incoming video frames and forwards only one to the Moondream vision
    service at a time, dropping subsequent frames until the current analysis
    completes. While waiting, an "Analyzing..." sprite animation is shown on the
    output video. Once the bot finishes speaking the description, the gate
    reopens for the next frame.
    """

    def __init__(self) -> None:
        super().__init__()
        self._current_frame: OutputImageRawFrame | None = None

    async def process_upstream_frame(self, frame: Frame) -> None:
        """Process frames coming back up from downstream processors."""

        if isinstance(frame, BotStartedSpeakingFrame) and self._current_frame:
            logger.info("Bot started speaking, analysis complete")
            await self.push_frame(self._current_frame, FrameDirection.DOWNSTREAM)
        elif isinstance(frame, BotStoppedSpeakingFrame):
            logger.info("Resetting analysis in progress flag due to BotStoppedSpeakingFrame")
            self._current_frame = None

        await self.push_frame(frame, FrameDirection.UPSTREAM)

    async def process_downstream_frame(self, frame: Frame) -> None:
        """Process frames going downstream to processors."""

        # Pass through all other non image downstream frames
        if not isinstance(frame, InputImageRawFrame):
            await self.push_frame(frame, FrameDirection.DOWNSTREAM)
            return

        # Handle downstream InputImageRawFrame
        if self._current_frame:
            # Drop the frame - we're already analyzing one
            return

        # Start new analysis
        await self.push_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)

        logger.info("Pushing new output frame before starting analysis")
        self._current_frame = OutputImageRawFrame(
            size=frame.size,
            format=frame.format,
            image=frame.image,
        )
        text_animation = write_text_on_image_animation(frame, "Analyzing...", total_frames=FPS)
        await self.push_frame(text_animation, FrameDirection.DOWNSTREAM)

        # Wait a moment to let the animation be seen, without it the sprite frame may be stuck in the pipeline
        # waiting for vision processing
        await asyncio.sleep(0.250)

        logger.info("Starting analysis")
        # Moondream only works with "RGB" images
        image = frame.image
        format = frame.format or "RGB"
        if frame.format == "YCbCr":
            pil_image = Image.frombytes("YCbCr", frame.size, frame.image)
            rgb_image = pil_image.convert("RGB")
            image = rgb_image.tobytes()
            format = "RGB"

        vision_frame = UserImageRawFrame(image=image, size=frame.size, format=format, text="describe")
        await self.push_frame(vision_frame, FrameDirection.DOWNSTREAM)

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        # Handle upstream frames (from downstream processors back up)
        if direction == FrameDirection.UPSTREAM:
            await self.process_upstream_frame(frame)
        else:
            await self.process_downstream_frame(frame)


class TextToTtsProcessor(FrameProcessor):
    """Convert vision-model text output into TTS speak frames.

    Wraps downstream ``TextFrame`` instances in ``TTSSpeakFrame`` so the TTS
    service picks them up. On the upstream side, it intercepts
    ``BotStoppedSpeakingFrame`` and delays it by the estimated speaking duration
    so that the output video frame stays visible while the audio is playing.
    """

    def __init__(self) -> None:
        super().__init__()
        self._last_text = ""

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if isinstance(frame, BotStoppedSpeakingFrame) and direction == FrameDirection.UPSTREAM:
            # count text words and add up the time it takes to speak them
            words = self._last_text.split()
            num_words = len(words)
            wpm = 150  # average words per minute
            duration = (num_words / wpm) * 60.0
            logger.info(f"Text ready, estimated speaking duration: {duration:.2f} seconds for {num_words} words")

            # wait until the description has been read before pushing the bot stopped frame
            async def delayed_push() -> None:
                await asyncio.sleep(duration)
                await self.push_frame(frame, direction)

            asyncio.create_task(delayed_push())

        elif isinstance(frame, TextFrame) and direction == FrameDirection.DOWNSTREAM:
            logger.debug(f"STT  {frame.text} DIR {direction}")
            self._last_text = frame.text
            tts_frame = TTSSpeakFrame(text=frame.text)
            await self.push_frame(tts_frame, direction)
        else:
            await self.push_frame(frame, direction)


async def main(session_str: str) -> None:
    """Build and run the Moondream vision pipeline for a Vonage Video session.

    Parses the session credentials from ``session_str``, sets up the Vonage
    Video Connector transport, Moondream vision service, and Piper TTS, then
    runs the pipeline until completion or signal interruption.

    Args:
        session_str: JSON string containing ``apiKey``, ``sessionId``, and
            ``token`` fields for the Vonage Video session.
    """
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
            video_in_enabled=True,
            video_in_auto_subscribe=True,
            video_in_preferred_resolution=(640, 480),
            video_in_preferred_framerate=1,
            video_out_enabled=True,
            video_out_width=1280,
            video_out_height=720,
            video_out_framerate=FPS,
            video_out_color_format="YCbCr",
            audio_out_enabled=True,
            video_out_is_live=False,
            publisher_name="Vision bot",
            audio_out_sample_rate=48000,
        ),
    )

    vision = MoondreamService()  # type: ignore[no-untyped-call]
    tts = PiperTTSService(voice_id="en_GB-alan-medium")

    pipeline = Pipeline(
        [
            transport.input(),
            InputVideoMultiplexProcessor(),
            vision,
            TextToTtsProcessor(),
            tts,
            transport.output(),
        ]
    )

    task = PipelineTask(pipeline)

    runner = PipelineRunner(handle_sigint=True, handle_sigterm=True)

    await runner.run(task)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        session_str = sys.argv[1]
        logger.info(f"Session str: {session_str}")
    else:
        logger.error(f"Usage: {sys.argv[0]} <VONAGE_SESSION_STR>")
        sys.exit(1)

    asyncio.run(main(session_str))
