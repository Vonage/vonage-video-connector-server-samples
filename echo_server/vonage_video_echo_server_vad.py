# Copyright 2026 Vonage
#
# VAD-gated audio + video echo server.
#
# Unlike the simple echo server (vonage_video_echo_server.py), this variant
# uses webrtcvad to detect speech boundaries.  Audio and the matching video
# frames are buffered for the duration of each utterance; once a brief
# silence is detected the complete segment — audio *and* video — is echoed
# back together so the two streams remain in sync.
#
# Why this is necessary:
#   A plain pass-through cannot be used with VAD because audio is held back
#   until the end of an utterance while video would continue in real-time,
#   making the two streams drift arbitrarily out of sync.  Here both are
#   buffered together and replayed as a pair.

import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
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


class VonageVideoVadEchoServer:
    # At 48 kHz mono with 10 ms frames the SDK delivers ~100 frames/sec.
    # A threshold of 10 silent frames therefore equals roughly 100 ms of
    # silence before an utterance is considered finished.
    SILENCE_THRESHOLD: int = 10

    # Target video replay rate.  Frames are echoed back one at a time with
    # this inter-frame delay so the C++ publisher buffer is not overwhelmed
    # when an entire speech segment's worth of frames is queued at once.
    VIDEO_FPS: int = 30

    # Rate at which the last received frame is re-sent while the echo queue
    # is idle (no speech segment playing back).  Keeps the published video
    # track visible instead of going black between utterances and at startup.
    KEEPALIVE_FPS: int = 5

    def __init__(self, session_info: dict[str, str]) -> None:
        self.client = vonage_video_connector.VonageVideoClient()
        self.audio_thread: Optional[threading.Thread] = None
        self.video_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Echo queues: filled when an utterance ends, drained by echo threads.
        self.audio_queue: queue.Queue[AudioData] = queue.Queue()
        self.video_queue: queue.Queue[VideoFrame] = queue.Queue()

        self.is_publishing: bool = False
        self._subscribed_stream_id: Optional[str] = None

        # VAD state — all fields guarded by _vad_lock because on_audio_data
        # and on_video_frame may be called from different SDK threads.
        self.vad: Vad = Vad(2)  # aggressiveness 0-3; 2 is a balanced default
        self._vad_lock = threading.Lock()
        self.speech_audio_frames: deque[AudioData] = deque()
        self.speech_video_frames: deque[VideoFrame] = deque()
        self.is_speech_active: bool = False
        self.silence_frames_count: int = 0

        # Most recently received video frame; kept up-to-date at all times
        # so it is always available even before the first speech segment.
        self._last_video_frame: Optional[VideoFrame] = None

        # Build settings
        audio_settings = SessionAudioSettings(sample_rate=48000, number_of_channels=1)
        self.video_resolution = VideoResolution(width=1280, height=720)
        video_publisher_settings = SessionVideoPublisherSettings(
            resolution=self.video_resolution,
            fps=self.VIDEO_FPS,
            format="YUV420P",
        )
        session_av_settings = SessionAVSettings(
            audio_publisher=audio_settings,
            audio_subscribers_mix=audio_settings,
            video_publisher=video_publisher_settings,
        )
        self.session_info = session_info
        self.session_settings = SessionSettings(
            enable_migration=False,
            av=session_av_settings,
            logging=LoggingSettings(level="warn"),
        )
        self.publisher_settings = PublisherSettings(
            name="Video Connector Example Echo Server (VAD)",
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
    # Audio / VAD processing
    # ------------------------------------------------------------------

    def on_audio_data(self, session: Session, audio_data: AudioData) -> None:
        """Receive audio data and process with VAD for speech-gated echoing."""
        if not self.is_publishing:
            return
        self._process_audio_with_vad(audio_data)

    def _process_audio_with_vad(self, audio_data: AudioData) -> None:
        """Analyse incoming audio with VAD and buffer speech segments.

        While speech is detected, audio frames are accumulated together with
        any video frames that arrived during the same interval (via
        on_video_frame).  Once silence persists for SILENCE_THRESHOLD
        consecutive frames the entire buffered utterance — both audio and
        the corresponding video frames — is queued for echo playback so the
        two streams are echoed in sync.

        Silence frames up to the threshold are included in the audio buffer
        to preserve the natural trailing silence of the utterance.
        """
        buffer_bytes = audio_data.sample_buffer.tobytes()

        try:
            is_speech = self.vad.is_speech(buffer_bytes, audio_data.sample_rate)
        except ValueError:
            logger.debug("VAD rejected frame (size=%d, rate=%d)", len(buffer_bytes), audio_data.sample_rate)
            return

        frame_copy = self._copy_audio_frame(audio_data)

        # Snapshot the frames to echo outside the lock to keep the critical
        # section as short as possible.
        audio_frames_to_echo: list[AudioData] = []
        video_frames_to_echo: list[VideoFrame] = []

        with self._vad_lock:
            if is_speech:
                self.silence_frames_count = 0
                self.speech_audio_frames.append(frame_copy)
                if not self.is_speech_active:
                    self.is_speech_active = True
                    logger.info("Speech detected, buffering audio and video")
            else:
                # Silence frame
                if not self.is_speech_active:
                    return

                self.silence_frames_count += 1
                # Buffer silence to preserve natural trailing pause in the echo.
                self.speech_audio_frames.append(frame_copy)

                if self.silence_frames_count >= self.SILENCE_THRESHOLD:
                    audio_frames_to_echo = list(self.speech_audio_frames)
                    video_frames_to_echo = list(self.speech_video_frames)
                    self.speech_audio_frames.clear()
                    self.speech_video_frames.clear()
                    self.is_speech_active = False
                    self.silence_frames_count = 0

        # Queue frames outside the lock — queue.put() may block under backpressure.
        if audio_frames_to_echo:
            logger.info(
                "Speech ended, queuing %d audio and %d video frames for echo",
                len(audio_frames_to_echo),
                len(video_frames_to_echo),
            )
            for frame in audio_frames_to_echo:
                self.audio_queue.put(frame)
            for frame in video_frames_to_echo:
                self.video_queue.put(frame)

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
        """Receive a video frame and track it for VAD-gated echo.

        When speech is NOT active, _last_video_frame is kept up-to-date so the
        keepalive thread always has a fresh still to display.  Once speech
        begins, _last_video_frame is frozen on the last pre-speech frame —
        the keepalive will hold that still while the utterance is buffered,
        avoiding the "double playback" effect where the live video leaks
        through before the echo plays back.  The incoming frames are still
        accumulated in speech_video_frames for the upcoming echo.

        Frames at the wrong resolution are discarded — the browser ramps up
        through lower resolutions before settling at 1280x720 and the C++
        publisher rejects mismatched frames anyway.
        """
        if not self.is_publishing:
            return
        if (
            video_frame.resolution.width != self.video_resolution.width
            or video_frame.resolution.height != self.video_resolution.height
        ):
            return

        frame_copy = self._copy_video_frame(video_frame)

        with self._vad_lock:
            if self.is_speech_active:
                # Freeze the still — do not update _last_video_frame while speaking.
                self.speech_video_frames.append(frame_copy)
            else:
                self._last_video_frame = frame_copy

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
        """Drain the audio queue and send frames back to the session."""
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
        """Drain the video queue and send frames back at the target frame rate.

        Two modes:
        - Draining: real speech-segment frames are in the queue — send them at
          VIDEO_FPS so the SDK's C++ buffer is not overwhelmed when all frames
          for an utterance arrive at once.
        - Keepalive: queue is empty — re-send the last received frame at
          KEEPALIVE_FPS so the published track stays visible instead of going
          black between utterances and during the initial startup window.
        """
        logger.info("Video echo thread started")
        frame_interval = 1.0 / self.VIDEO_FPS
        keepalive_interval = 1.0 / self.KEEPALIVE_FPS
        last_keepalive_sent = 0.0

        while not self._stop_event.is_set():
            try:
                frame = self.video_queue.get(timeout=0.01)
            except queue.Empty:
                # No real frames — send last seen frame as a keepalive.
                now = time.monotonic()
                if now - last_keepalive_sent >= keepalive_interval:
                    with self._vad_lock:
                        keepalive = self._last_video_frame
                    if keepalive is not None:
                        try:
                            self.client.add_video(keepalive)
                        except Exception:
                            logger.exception("Error sending keepalive video frame")
                        last_keepalive_sent = now
                continue
            try:
                if not self.client.add_video(frame):
                    logger.warning("add_video() returned False — C++ buffer full, frame dropped")
            except Exception:
                logger.exception("Error injecting video frame")
            time.sleep(frame_interval)

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

        logger.info("Connected, echo server will echo back speech segments with corresponding video")
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

    parser = argparse.ArgumentParser(
        description="Vonage Video Echo Server with Voice Activity Detection (audio + video)"
    )
    parser.add_argument("session_info", help="Path to JSON file with session info or direct JSON string")
    args = parser.parse_args()

    echo_server = VonageVideoVadEchoServer(read_session_info(args.session_info))

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
