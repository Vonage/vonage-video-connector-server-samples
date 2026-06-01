# Copyright 2026 Vonage

import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
from typing import Optional, cast

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
    SessionVideoPublisherSettings,
    Stream,
    Subscriber,
    SubscriberSettings,
    SubscriberVideoSettings,
    VideoFrame,
    VideoResolution,
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
    def __init__(self, session_info: dict[str, str]) -> None:
        self.client = vonage_video_connector.VonageVideoClient()
        self.audio_thread: Optional[threading.Thread] = None
        self.video_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Simple pass-through queues: frames go in from callbacks, out to publisher.
        # No buffering or gating — echo happens immediately at the natural frame rate.
        self.audio_queue: queue.Queue[AudioData] = queue.Queue()
        self.video_queue: queue.Queue[VideoFrame] = queue.Queue()

        self.is_publishing: bool = False
        self._subscribed_stream_id: Optional[str] = None

        # Build settings
        audio_settings = SessionAudioSettings(sample_rate=48000, number_of_channels=1)
        self.video_resolution = VideoResolution(width=1280, height=720)
        video_publisher_settings = SessionVideoPublisherSettings(
            resolution=self.video_resolution,
            fps=30,
            format="YUV420P",
        )
        session_av_settings = SessionAVSettings(
            audio_publisher=audio_settings, audio_subscribers_mix=audio_settings, video_publisher=video_publisher_settings
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
            has_video=True,
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

    @staticmethod
    def _copy_video_frame(video_frame: VideoFrame) -> VideoFrame:
        """Create an independent copy of a VideoFrame.

        The SDK may reuse the underlying buffer between callbacks, so we
        must snapshot the pixel data before queuing it for later use.
        """
        buffer_copy = bytes(video_frame.frame_buffer)
        mem_view = memoryview(buffer_copy).cast("B")
        return VideoFrame(
            frame_buffer=mem_view,
            resolution=video_frame.resolution,
            format=video_frame.format,
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
        # Fired when the audio capturer is started (maps to on_ready_to_publish in the
        # C++ SDK, a misleading name).  There is no equivalent callback exposed for
        # video; we start the video echo thread here as well since by this point the
        # publisher pipeline is fully initialised.
        logger.info("Audio system ready, starting echo server: session_id=%s", session.id)
        self.is_publishing = True

        self.audio_thread = threading.Thread(target=self.audio_echo_thread, daemon=False)
        self.audio_thread.start()

        self.video_thread = threading.Thread(target=self.video_echo_thread, daemon=False)
        self.video_thread.start()

    def on_session_disconnected(self, session: Session) -> None:
        logger.info("Session disconnected: session_id=%s", session.id)
        self.stop()

    def on_stream_received(self, session: Session, stream: Stream) -> None:
        logger.info("Stream received: session_id=%s stream_id=%s", session.id, stream.id)
        if self._subscribed_stream_id is not None:
            logger.info("Already subscribed to stream %s, ignoring stream %s", self._subscribed_stream_id, stream.id)
            return
        self._subscribed_stream_id = stream.id
        success = self.client.subscribe(
            stream=stream,
            settings=SubscriberSettings(
                video_settings=SubscriberVideoSettings(
                    preferred_resolution=VideoResolution(width=1280, height=720),
                )
            ),
            on_error_cb=self.on_subscriber_error,
            on_connected_cb=self.on_subscriber_connected,
            on_disconnected_cb=self.on_subscriber_disconnected,
            on_render_frame_cb=self.on_video_frame,
        )
        if not success:
            logger.error("Failed to subscribe to stream %s", stream.id)

    def on_stream_dropped(self, session: Session, stream: Stream) -> None:
        logger.info("Stream dropped: session_id=%s stream_id=%s", session.id, stream.id)
        if self._subscribed_stream_id == stream.id:
            logger.info("Subscribed stream dropped, ready to subscribe to the next available stream")
            self._subscribed_stream_id = None

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
    # Audio processing
    # ------------------------------------------------------------------

    def on_audio_data(self, session: Session, audio_data: AudioData) -> None:
        """Receive audio data and queue it for immediate echo."""
        if not self.is_publishing:
            return
        frame_copy = self._copy_audio_frame(audio_data)
        self.audio_queue.put(frame_copy)

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
    # Video processing
    # ------------------------------------------------------------------

    def on_video_frame(self, subscriber: Subscriber, video_frame: VideoFrame) -> None:
        """Receive a video frame and queue it for immediate echo.

        Frames at the wrong resolution are discarded — the browser ramps up
        through lower resolutions before settling at 1280x720 and C++ rejects
        mismatched frames anyway.
        """
        if not self.is_publishing:
            return
        if video_frame.resolution.width != self.video_resolution.width or video_frame.resolution.height != self.video_resolution.height:
            return
        frame_copy = self._copy_video_frame(video_frame)
        self.video_queue.put(frame_copy)

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
    # Echo threads
    # ------------------------------------------------------------------

    def audio_echo_thread(self) -> None:
        """Drain the audio queue and send frames back to the session immediately."""
        logger.info("Audio echo thread started")

        while not self._stop_event.is_set():
            try:
                audio_data = self.audio_queue.get(timeout=0.01)
            except queue.Empty:
                continue
            try:
                self.client.add_audio(audio_data)
            except Exception:
                logger.exception("Error injecting echo audio")

        logger.info("Audio echo thread stopped")

    def video_echo_thread(self) -> None:
        """Drain the video queue and send frames back to the session immediately."""
        logger.info("Video echo thread started")

        while not self._stop_event.is_set():
            try:
                frame = self.video_queue.get(timeout=0.01)
            except queue.Empty:
                continue
            try:
                if not self.client.add_video(frame):
                    logger.warning("add_video() returned False — C++ buffer full, frame dropped")
            except Exception:
                logger.exception("Error injecting video frame")

        logger.info("Video echo thread stopped")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Signal the echo threads to stop and wait for them to finish."""
        if self.audio_thread and self.audio_thread.is_alive():
            logger.info("Stopping echo threads...")
            self._stop_event.set()
            self.audio_thread.join(timeout=2.0)
            if self.audio_thread.is_alive():
                logger.warning("Audio echo thread did not stop cleanly")

        if self.video_thread and self.video_thread.is_alive():
            self._stop_event.set()
            self.video_thread.join(timeout=2.0)
            if self.video_thread.is_alive():
                logger.warning("Video echo thread did not stop cleanly")

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

        logger.info("Connected, echo server will echo back any received audio and video")
        return True

    def cleanup(self) -> None:
        """Clean up resources and properly leave the session."""
        self.stop()

        logger.info("Stopping publishing...")
        if not self.client.unpublish():
            logger.warning("Failed to stop publishing")

        if self.client.is_connected():
            logger.info("Disconnecting from session...")
            if not self.client.disconnect():
                logger.warning("Failed to disconnect")

        logger.info("Echo server shut down.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Vonage Video Echo Server")
    parser.add_argument("session_info", help="Path to JSON file with session info or direct JSON string")
    args = parser.parse_args()

    echo_server = VonageVideoEchoServer(read_session_info(args.session_info))

    stop_event = threading.Event()

    def handle_signal(sig: int, frame: object) -> None:
        sig_name = signal.Signals(sig).name
        if stop_event.is_set():
            # Already shutting down — force exit on repeated signal
            logger.warning("Received %s again during cleanup, forcing exit...", sig_name)
            os._exit(1)
        logger.info("Received %s, shutting down echo server...", sig_name)
        stop_event.set()

    def stdin_listener() -> None:
        """Block until Enter is pressed or stdin is closed, then trigger shutdown."""
        try:
            input()
        except EOFError:
            # stdin closed (e.g. non-interactive / detached container) — wait forever
            stop_event.wait()
            return
        logger.info("Enter pressed, shutting down echo server...")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Daemon thread: listens for Enter key so the server also exits on Enter.
    stdin_thread = threading.Thread(target=stdin_listener, daemon=True)
    stdin_thread.start()

    try:
        if not echo_server.connect():
            sys.exit(1)

        logger.info("Echo server running. Press Enter or Ctrl+C to stop...")
        stop_event.wait()

    finally:
        echo_server.cleanup()


if __name__ == "__main__":
    main()
