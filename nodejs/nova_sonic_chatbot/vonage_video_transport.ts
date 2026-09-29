// Vonage Video Connector transport for a speech-to-speech bot.
//
// Owns everything connector-related: joining the session, publishing media,
// subscribing to participants, delivering mixed participant audio, and pacing
// generated speech back into the session. It knows nothing about the AI service.

import {
  VonageVideoClient,
  type AudioData,
  type Connection,
  type LogLevel,
  type NumberOfChannels,
  type Publisher,
  type SampleRate,
  type Session,
  type SessionSettings,
  type Stream,
  type Subscriber,
} from '@vonage/video-connector';

/** Vonage Video session credentials. */
export interface SessionInfo {
  apiKey: string;
  sessionId: string;
  token: string;
}

/** Shared log helper, so every module writes the same format. */
export function log(level: string, msg: string, ...args: unknown[]): void {
  const ts = new Date().toISOString();
  const line = `${ts} [${level.toUpperCase()}] ${msg}`;
  if (level === 'error') {
    console.error(line, ...args);
  } else if (level === 'warn') {
    console.warn(line, ...args);
  } else {
    console.log(line, ...args);
  }
}

/** Transport configuration. */
export interface VonageVideoTransportOptions {
  sessionInfo: SessionInfo;
  /** Name of the published stream. */
  publisherName: string;
  /** Sample rate of the mixed participant audio handed to `onAudioReceived`. */
  inputSampleRate: SampleRate;
  /** Sample rate of the audio pushed with `playAudio()`. */
  outputSampleRate: SampleRate;
  /** Channel count used for both directions. */
  channels: NumberOfChannels;
  /** Size of the chunks passed to `onAudioReceived`, in bytes. */
  inputChunkBytes: number;
  /**
   * Publish video as well as audio. Omit for an audio-only stream.
   * The geometry must match the frames handed to `playVideo()`.
   */
  videoSettings?: VideoPublishSettings;
  /** Native SDK log verbosity. */
  logLevel: LogLevel;
}

/** Geometry of the published video stream. */
export interface VideoPublishSettings {
  width: number;
  height: number;
  fps: number;
}

/** A raw YUV420P frame to publish. */
export interface VideoFrameInput {
  data: Buffer;
  width: number;
  height: number;
}

/** Transport events. */
export interface VonageVideoTransportHandlers {
  /** Mixed participant audio, PCM16 at `inputSampleRate`, `inputChunkBytes` per call. */
  onAudioReceived: (pcm: Buffer) => void;
  /** The publishing pipeline is ready to receive audio and video. */
  onReady: () => void;
  /** A participant stream arrived and was subscribed to. */
  onParticipantJoined: () => void;
  /** The session ended or the published stream was destroyed. */
  onClosed: () => void;
}

export class VonageVideoTransport {
  private static readonly AUDIO_TICK_INTERVAL = 10; // ms
  private static readonly OUTPUT_FRAME_MS = 10;
  /** Upper bound of speech queued in the publisher buffer. */
  private static readonly MAX_BUFFERED_MS = 200;

  private readonly options: VonageVideoTransportOptions;
  private readonly handlers: VonageVideoTransportHandlers;
  private readonly client = new VonageVideoClient();
  private readonly sessionSettings: SessionSettings;
  private readonly outputFrameSamples: number;
  private readonly outputFrameBytes: number;

  private readonly streams = new Set<string>();
  private audioTimer: NodeJS.Timeout | null = null;
  private inputBuffer: Buffer = Buffer.alloc(0);
  private outputBuffer: Buffer = Buffer.alloc(0);
  private isReady = false;
  private hasLoggedGeometryWarning = false;

  constructor(options: VonageVideoTransportOptions, handlers: VonageVideoTransportHandlers) {
    this.options = options;
    this.handlers = handlers;

    // PCM16: 2 bytes per sample per channel.
    this.outputFrameSamples =
      (options.outputSampleRate * VonageVideoTransport.OUTPUT_FRAME_MS) / 1000;
    this.outputFrameBytes = this.outputFrameSamples * options.channels * 2;

    // Requesting the rates the AI service uses lets the connector do the
    // resampling, so no audio conversion is needed in this sample.
    this.sessionSettings = {
      enableMigration: false,
      av: {
        audioPublisher: { sampleRate: options.outputSampleRate, channels: options.channels },
        audioSubscribersMix: { sampleRate: options.inputSampleRate, channels: options.channels },
        ...(options.videoSettings
          ? {
              videoPublisher: {
                width: options.videoSettings.width,
                height: options.videoSettings.height,
                fps: options.videoSettings.fps,
                format: 'YUV420P' as const,
              },
            }
          : {}),
      },
      logging: { level: options.logLevel },
    };
  }

  /** Number of participant streams currently subscribed to. */
  get participantCount(): number {
    return this.streams.size;
  }

  // ── Lifecycle ───────────────────────────────────────────────────────

  async connect(): Promise<boolean> {
    log('info', 'Connecting to session...');

    const session = await this.client.connect(
      this.options.sessionInfo.apiKey,
      this.options.sessionInfo.sessionId,
      this.options.sessionInfo.token,
      {
        sessionSettings: this.sessionSettings,
        onError: this.onSessionError.bind(this),
        onDisconnected: this.onSessionDisconnected.bind(this),
        onConnectionCreated: this.onConnectionCreated.bind(this),
        onConnectionDropped: this.onConnectionDropped.bind(this),
        onStreamReceived: this.onStreamReceived.bind(this),
        onStreamDropped: this.onStreamDropped.bind(this),
        onAudioData: this.onSessionAudioData.bind(this),
        onReadyForAudio: this.onReadyForAudio.bind(this),
      },
    );

    if (!session) {
      log('error', 'Failed to connect to session');
      return false;
    }

    log('info', `Connected to session: session_id=${session?.sessionId}`);
    return true;
  }

  async publish(): Promise<boolean> {
    log('info', 'Starting publishing...');

    const publisher = await this.client.publish({
      settings: {
        name: this.options.publisherName,
        hasAudio: true,
        hasVideo: Boolean(this.options.videoSettings),
        audioSettings: { enableStereoMode: false, enableOpusDtx: true },
      },
      onError: this.onPublisherError.bind(this),
      onStreamDestroyed: this.onStreamDestroyed.bind(this),
    });

    if (!publisher) {
      log('error', 'Failed to start publishing');
      return false;
    }

    return true;
  }

  /** Stop media output without leaving the session. */
  stop(): void {
    this.isReady = false;
    if (this.audioTimer) {
      clearInterval(this.audioTimer);
      this.audioTimer = null;
    }
  }

  /** Stop publishing and leave the session. */
  async cleanup(): Promise<void> {
    this.stop();

    if (this.client.isPublishing()) {
      log('info', 'Stopping publishing...');
      try {
        await this.client.unpublish();
      } catch {
        log('warn', 'Failed to stop publishing');
      }
    }

    if (this.client.isConnected()) {
      log('info', 'Disconnecting...');
      try {
        await this.client.disconnect();
      } catch {
        log('warn', 'Failed to disconnect');
      }
    }
  }

  // ── Audio out ───────────────────────────────────────────────────────

  /** Queue PCM16 audio at `outputSampleRate` for publishing into the session. */
  playAudio(pcm: Buffer): void {
    this.outputBuffer = Buffer.concat([this.outputBuffer, pcm]);
  }

  /** Drop everything queued, locally and in the native publisher buffer. */
  clearAudio(): void {
    this.outputBuffer = Buffer.alloc(0);
    this.client.clearMediaBuffers();
  }

  // ── Video out ───────────────────────────────────────────────────────

  /**
   * Publish a YUV420P frame. Frames already arrive paced by their source, so
   * they are pushed straight through rather than buffered on a timer.
   *
   * Frames whose geometry differs from the configured publisher geometry are
   * dropped: the encoder is fixed at publish time and cannot switch mid-stream.
   */
  playVideo(frame: VideoFrameInput): void {
    const videoSettings = this.options.videoSettings;
    if (!videoSettings || !this.isReady) return;

    if (frame.width !== videoSettings.width || frame.height !== videoSettings.height) {
      if (!this.hasLoggedGeometryWarning) {
        this.hasLoggedGeometryWarning = true;
        log(
          'warn',
          `Dropping video: source is ${frame.width}x${frame.height} but the publisher ` +
          `is ${videoSettings.width}x${videoSettings.height}. Set the publisher geometry to match.`,
        );
      }
      return;
    }

    try {
      this.client.addVideo({
        data: frame.data,
        width: frame.width,
        height: frame.height,
        format: 'YUV420P',
      });
    } catch (error) {
      log('error', 'Error injecting video', error);
    }
  }

  private audioOutputTick(): void {
    if (this.outputBuffer.length < this.outputFrameBytes) return;

    try {
      // Keep the publisher buffer topped up instead of pushing exactly one frame
      // per tick: speech arrives in bursts and timer drift would otherwise
      // introduce gaps.
      let bufferedMs = this.client.getMediaBufferStats().audio?.durationMs ?? 0;

      while (
        this.outputBuffer.length >= this.outputFrameBytes &&
        bufferedMs < VonageVideoTransport.MAX_BUFFERED_MS
      ) {
        const frame = Buffer.from(this.outputBuffer.subarray(0, this.outputFrameBytes));
        this.outputBuffer = this.outputBuffer.subarray(this.outputFrameBytes);

        this.client.addAudio({
          data: frame,
          sampleRate: this.options.outputSampleRate,
          numberOfChannels: this.options.channels,
          numberOfFrames: this.outputFrameSamples,
        });

        bufferedMs += VonageVideoTransport.OUTPUT_FRAME_MS;
      }
    } catch (error) {
      log('error', 'Error injecting audio', error);
    }
  }

  // ── Session callbacks ───────────────────────────────────────────────

  private onReadyForAudio(session: Session): void {
    log('info', `Audio system ready: session_id=${session?.sessionId}`);
    this.isReady = true;

    if (!this.audioTimer) {
      this.audioTimer = setInterval(
        () => this.audioOutputTick(),
        VonageVideoTransport.AUDIO_TICK_INTERVAL,
      );
    }

    this.handlers.onReady();
  }

  private onSessionAudioData(_session: Session, audioData: AudioData): void {
    if (!this.isReady || this.streams.size === 0) return;

    // Batch the 10 ms callbacks into larger chunks for the consumer.
    this.inputBuffer = Buffer.concat([this.inputBuffer, audioData.data]);
    while (this.inputBuffer.length >= this.options.inputChunkBytes) {
      const chunk = this.inputBuffer.subarray(0, this.options.inputChunkBytes);
      this.inputBuffer = this.inputBuffer.subarray(this.options.inputChunkBytes);
      this.handlers.onAudioReceived(Buffer.from(chunk));
    }
  }

  private async onStreamReceived(session: Session, stream: Stream): Promise<void> {
    log('info', `Stream received: session_id=${session?.sessionId} stream_id=${stream?.streamId}`);
    this.streams.add(stream?.streamId);

    // Subscribing is what includes the stream in the mixed audio.
    const subscriber = await this.client.subscribe(stream, {
      settings: { subscribeToAudio: true, subscribeToVideo: false },
      onError: this.onSubscriberError.bind(this),
      onDisconnected: this.onSubscriberDisconnected.bind(this),
    });

    if (!subscriber) {
      log('error', `Failed to subscribe to stream ${stream?.streamId}`);
      this.streams.delete(stream?.streamId);
      return;
    }

    this.handlers.onParticipantJoined();
  }

  private onStreamDropped(session: Session, stream: Stream): void {
    log('info', `Stream dropped: session_id=${session?.sessionId} stream_id=${stream?.streamId}`);
    this.streams.delete(stream?.streamId);
  }

  private onSessionError(session: Session, description: string, code: number): void {
    log('error', `Session error: session_id=${session?.sessionId} desc=${description} code=${code}`);
  }

  private onSessionDisconnected(session: Session): void {
    log('info', `Session disconnected: session_id=${session?.sessionId}`);
    this.handlers.onClosed();
  }

  private onConnectionCreated(session: Session, connection: Connection): void {
    log(
      'info',
      `Connection created: session_id=${session?.sessionId} connection_id=${connection?.connectionId}`,
    );
  }

  private onConnectionDropped(session: Session, connection: Connection): void {
    log(
      'info',
      `Connection dropped: session_id=${session?.sessionId} connection_id=${connection?.connectionId}`,
    );
  }

  private onSubscriberError(subscriber: Subscriber, description: string, code: number): void {
    log(
      'error',
      `Subscriber error: stream_id=${subscriber?.stream?.streamId} desc=${description} code=${code}`,
    );
  }

  private onSubscriberDisconnected(subscriber: Subscriber): void {
    log('info', `Subscriber disconnected: stream_id=${subscriber?.stream?.streamId}`);
  }

  private onPublisherError(publisher: Publisher, description: string, code: number): void {
    log(
      'error',
      `Publisher error: stream_id=${publisher?.stream?.streamId} desc=${description} code=${code}`,
    );
  }

  private onStreamDestroyed(publisher: Publisher): void {
    log('info', `Publisher stream destroyed: stream_id=${publisher?.stream?.streamId}`);
    this.handlers.onClosed();
  }
}
