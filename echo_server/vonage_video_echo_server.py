# Copyright 2026 Vonage

import argparse
import json
import logging
import os
import queue
import sys
import threading
from collections import deque
from typing import Optional, cast

from webrtcvad import Vad  # type: ignore[import-untyped]

import vonage_video_connector
from vonage_video_connector.models import (
    AudioData,
    Connection,
    LoggingSettings,
    Publisher,
    PublisherAudioSettings,
    PublisherSettings,
    Session,
    SessionAudioSettings,
    SessionAVSettings,
    SessionSettings,
    Stream,
    Subscriber,
)

logger = logging.getLogger(__name__)


def read_session_info(session_info_arg: str) -> dict[str, str]:
    """Read session info from a file path or a direct JSON string.

    Args:
        session_info_arg: Either a path to a JSON file or a raw JSON string.

    Returns:
        Parsed session info dictionary.

    Raises:
        FileNotFoundError: If the path looks like a file but cannot be read.
        json.JSONDecodeError: If the JSON is malformed.
    """
    if os.path.isfile(session_info_arg):
        with open(session_info_arg, "r") as f:
            return cast(dict[str, str], json.load(f))

    return cast(dict[str, str], json.loads(session_info_arg))


class VonageVideoEchoServer:
    # At 48 kHz mono with 10 ms frames the SDK delivers ~100 frames/sec.
    # A threshold of 10 silent frames therefore equals roughly 100 ms of
    # silence before speech is considered finished.
    SILENCE_THRESHOLD: int = 10

    def __init__(self, session_info: dict[str, str]) -> None:
        self.client = vonage_video_connector.VonageVideoClient()
        self.audio_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.audio_queue: queue.Queue[AudioData] = queue.Queue()
        self.is_publishing: bool = False

        # VAD (Voice Activity Detection) related attributes
        self.vad: Vad = Vad(2)  # Aggressiveness 0-3, higher = more aggressive
        self.speech_frames: deque[AudioData] = deque()
        self.is_speech_active: bool = False
        self.silence_frames_count: int = 0

        # Build settings
        audio_settings = SessionAudioSettings(sample_rate=48000, number_of_channels=1)
        session_av_settings = SessionAVSettings(
            audio_publisher=audio_settings, audio_subscribers_mix=audio_settings, video_publisher=None
        )
        self.session_info = session_info
        self.session_settings = SessionSettings(
            enable_migration=False,
            av=session_av_settings,
            logging=LoggingSettings(level="warn"),
        )
        self.publisher_settings = PublisherSettings(
            name="Video Connector Example Echo Server",
            has_audio=True,
            has_video=False,
            audio_settings=PublisherAudioSettings(enable_stereo_mode=False, enable_opus_dtx=True),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _copy_audio_frame(audio_data: AudioData) -> AudioData:
        """Create an independent copy of an AudioData frame.

        The SDK may reuse the underlying buffer between callbacks, so we
        must snapshot the sample data before queuing it for later playback.
        """
        buffer_copy = audio_data.sample_buffer.tobytes()
        mem_view = memoryview(buffer_copy).cast("h")
        return AudioData(
            sample_buffer=mem_view,
            sample_rate=audio_data.sample_rate,
            number_of_channels=audio_data.number_of_channels,
            number_of_frames=audio_data.number_of_frames,
        )

    # ------------------------------------------------------------------
    # Session callbacks
    # ------------------------------------------------------------------

    def on_session_error(self, session: Session, error_description: str, error_code: int) -> None:
        logger.error("Session error: session_id=%s desc=%s code=%d", session.id, error_description, error_code)

    def on_session_connected(self, session: Session) -> None:
        logger.info("Session connected: session_id=%s", session.id)

        own_connection = self.client.get_connection()
        if own_connection:
            logger.info("Own connection: id=%s creation_time=%s", own_connection.id, own_connection.creation_time)

        logger.info("Starting echo server publisher...")
        success = self.client.publish(
            settings=self.publisher_settings,
            on_error_cb=self.on_publisher_error,
            on_stream_created_cb=self.on_stream_created,
            on_stream_destroyed_cb=self.on_stream_destroyed,
        )
        if not success:
            logger.error("Failed to start publishing")

    def on_ready_for_audio(self, session: Session) -> None:
        logger.info("Audio system ready, echo server is now active: session_id=%s", session.id)
        self.is_publishing = True

        self.audio_thread = threading.Thread(target=self.audio_echo_thread, daemon=False)
        self.audio_thread.start()

    def on_session_disconnected(self, session: Session) -> None:
        logger.info("Session disconnected: session_id=%s", session.id)
        self.stop()

    def on_stream_received(self, session: Session, stream: Stream) -> None:
        logger.info("Stream received: session_id=%s stream_id=%s", session.id, stream.id)
        success = self.client.subscribe(
            stream=stream,
            on_error_cb=self.on_subscriber_error,
            on_connected_cb=self.on_subscriber_connected,
            on_disconnected_cb=self.on_subscriber_disconnected,
        )
        if not success:
            logger.error("Failed to subscribe to stream %s", stream.id)

    def on_stream_dropped(self, session: Session, stream: Stream) -> None:
        logger.info("Stream dropped: session_id=%s stream_id=%s", session.id, stream.id)

    def on_connection_created(self, session: Session, connection: Connection) -> None:
        logger.info(
            "Connection created: session_id=%s connection_id=%s creation_time=%s",
            session.id,
            connection.id,
            connection.creation_time,
        )

    def on_connection_dropped(self, session: Session, connection: Connection) -> None:
        own_connection = self.client.get_connection()
        is_own = own_connection and connection.id == own_connection.id
        label = "OWN" if is_own else "OTHER"
        logger.info(
            "Connection dropped [%s]: session_id=%s connection_id=%s",
            label,
            session.id,
            connection.id,
        )

    # ------------------------------------------------------------------
    # Audio / VAD processing
    # ------------------------------------------------------------------

    def on_audio_data(self, session: Session, audio_data: AudioData) -> None:
        """Receive audio data and process with VAD for intelligent echoing."""
        if not self.is_publishing:
            return
        self._process_audio_with_vad(audio_data)

    def _process_audio_with_vad(self, audio_data: AudioData) -> None:
        """Analyse incoming audio with VAD and buffer speech segments.

        While speech is detected, frames are accumulated in an internal
        buffer.  Once silence persists for ``SILENCE_THRESHOLD`` consecutive
        frames the entire buffered utterance is queued for echo playback.
        """
        buffer_bytes = audio_data.sample_buffer.tobytes()

        try:
            is_speech = self.vad.is_speech(buffer_bytes, audio_data.sample_rate)
        except ValueError:
            logger.debug("VAD rejected frame (size=%d, rate=%d)", len(buffer_bytes), audio_data.sample_rate)
            return

        frame_copy = self._copy_audio_frame(audio_data)

        if is_speech:
            self.silence_frames_count = 0
            self.speech_frames.append(frame_copy)

            if not self.is_speech_active:
                self.is_speech_active = True
                logger.info("Speech detected, buffering audio")
            return

        # Silence frame
        if not self.is_speech_active:
            return

        self.silence_frames_count += 1
        # Keep buffering silence to maintain natural timing
        self.speech_frames.append(frame_copy)

        if self.silence_frames_count >= self.SILENCE_THRESHOLD:
            logger.info("Speech ended, queuing %d frames for echo", len(self.speech_frames))
            for frame in self.speech_frames:
                self.audio_queue.put(frame)
            self.speech_frames.clear()
            self.is_speech_active = False
            self.silence_frames_count = 0

    # ------------------------------------------------------------------
    # Subscriber callbacks
    # ------------------------------------------------------------------

    def on_subscriber_error(self, subscriber: Subscriber, error_description: str, error_code: int) -> None:
        logger.error(
            "Subscriber error: stream_id=%s desc=%s code=%d", subscriber.stream.id, error_description, error_code
        )

    def on_subscriber_connected(self, subscriber: Subscriber) -> None:
        logger.info("Subscriber connected: stream_id=%s", subscriber.stream.id)

    def on_subscriber_disconnected(self, subscriber: Subscriber) -> None:
        logger.info("Subscriber disconnected: stream_id=%s", subscriber.stream.id)

    # ------------------------------------------------------------------
    # Publisher callbacks
    # ------------------------------------------------------------------

    def on_publisher_error(self, publisher: Publisher, error_description: str, error_code: int) -> None:
        logger.error(
            "Publisher error: stream_id=%s desc=%s code=%d", publisher.stream.id, error_description, error_code
        )

    def on_stream_created(self, publisher: Publisher) -> None:
        logger.info("Publisher stream created: stream_id=%s", publisher.stream.id)

    def on_stream_destroyed(self, publisher: Publisher) -> None:
        logger.info("Publisher stream destroyed: stream_id=%s", publisher.stream.id)
        self.stop()

    # ------------------------------------------------------------------
    # Echo thread
    # ------------------------------------------------------------------

    def audio_echo_thread(self) -> None:
        """Drain the audio queue and send frames back to the session."""
        logger.info("Echo thread started")

        while not self._stop_event.is_set():
            try:
                audio_data = self.audio_queue.get(timeout=0.01)
            except queue.Empty:
                continue
            try:
                self.client.add_audio(audio_data)
            except Exception:
                logger.exception("Error injecting echo audio")

        logger.info("Echo thread stopped")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Signal the echo thread to stop and wait for it to finish."""
        if self.audio_thread and self.audio_thread.is_alive():
            logger.info("Stopping echo thread...")
            self._stop_event.set()
            self.audio_thread.join(timeout=2.0)
            if self.audio_thread.is_alive():
                logger.warning("Echo thread did not stop cleanly")

    def connect(self) -> bool:
        """Connect to the session and start the echo server."""
        logger.info("Connecting to session...")

        success = self.client.connect(
            application_id=self.session_info["apiKey"],
            session_id=self.session_info["sessionId"],
            token=self.session_info["token"],
            session_settings=self.session_settings,
            on_error_cb=self.on_session_error,
            on_connected_cb=self.on_session_connected,
            on_disconnected_cb=self.on_session_disconnected,
            on_connection_created_cb=self.on_connection_created,
            on_connection_dropped_cb=self.on_connection_dropped,
            on_stream_received_cb=self.on_stream_received,
            on_stream_dropped_cb=self.on_stream_dropped,
            on_audio_data_cb=self.on_audio_data,
            on_ready_for_audio_cb=self.on_ready_for_audio,
        )

        if not success:
            logger.error("Failed to connect to session")
            return False

        logger.info("Connected, echo server will echo back any received audio")
        return True

    def cleanup(self) -> None:
        """Clean up resources."""
        self.stop()

        logger.info("Stopping publishing...")
        if not self.client.unpublish():
            logger.warning("Failed to stop publishing")

        if self.client.is_connected():
            logger.info("Disconnecting...")
            if not self.client.disconnect():
                logger.warning("Failed to disconnect")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Vonage Video Echo Server with Voice Activity Detection")
    parser.add_argument("session_info", help="Path to JSON file with session info or direct JSON string")
    args = parser.parse_args()

    echo_server = VonageVideoEchoServer(read_session_info(args.session_info))

    try:
        if not echo_server.connect():
            sys.exit(1)

        logger.info("Echo server running. Press Enter to stop...")
        input()

    except KeyboardInterrupt:
        logger.info("Shutting down echo server...")

    finally:
        echo_server.cleanup()


if __name__ == "__main__":
    main()
