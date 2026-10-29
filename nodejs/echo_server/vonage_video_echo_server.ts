#!/usr/bin/env node


import * as fs from 'fs';

import {
  VonageVideoClient,
  type Session,
  type Stream,
  type Connection,
  type Subscriber,
  type Publisher,
  type AudioData,
  type SessionSettings,
  type PublisherSettings,
  type SampleRate,
  type NumberOfChannels,
  type VideoFrame,
} from '@vonage/video-connector';

function readSessionInfo(sessionInfoArg: string): { apiKey: string; sessionId: string; token: string } {
  if (fs.existsSync(sessionInfoArg) && fs.statSync(sessionInfoArg).isFile()) {
    return JSON.parse(fs.readFileSync(sessionInfoArg, 'utf8'));
  }
  return JSON.parse(sessionInfoArg);
}

// Scale a YUV420P video frame to the given width and height, preserving aspect ratio,
// centering the result and converting it to grayscale.
// Returns a new VideoFrame object with the scaled data.
function scaleGrayVideoFrame(frame: VideoFrame, width: number, height: number): VideoFrame {
  if (frame.format === 'YUV420P') {
    const yuvData = frame.data;
    const srcWidth = frame.width;
    const srcHeight = frame.height;

    // Compute the largest scaled size that fits within the target while
    // preserving the source aspect ratio (fit-inside / letterbox strategy).
    const srcAspect = srcWidth / srcHeight;
    const dstAspect = width / height;

    let scaledWidth: number;
    let scaledHeight: number;
    if (srcAspect > dstAspect) {
      // Source is wider than destination — fit to width, letterbox top/bottom
      scaledWidth = width;
      scaledHeight = Math.round(width / srcAspect);
    } else {
      // Source is taller than destination — fit to height, pillarbox left/right
      scaledHeight = height;
      scaledWidth = Math.round(height * srcAspect);
    }

    // YUV420P requires even dimensions for correct chroma plane addressing
    scaledWidth  = scaledWidth  & ~1;
    scaledHeight = scaledHeight & ~1;

    // Even-aligned centering offsets so chroma samples stay on 2×2 boundaries
    const xOffset = (Math.floor((width  - scaledWidth)  / 2)) & ~1;
    const yOffset = (Math.floor((height - scaledHeight) / 2)) & ~1;

    // YUV420P layout:
    // Y plane  : width * height bytes
    // U plane  : (width/2) * (height/2) bytes
    // V plane  : (width/2) * (height/2) bytes
    const srcYPlaneSize = srcWidth * srcHeight;
    const dstYPlaneSize = width * height;
    const dstUPlaneSize = (width / 2) * (height / 2);

    // Initialise the output buffer with YUV black:
    const yBlack = 16; // Y = 16 (limited-range black)
    const uvNeutral = 128; // U = V = 128 (grayscale)
    const scaledData = Buffer.alloc(width * height * 3 / 2);
    scaledData.fill(yBlack, 0, dstYPlaneSize);
    scaledData.fill(uvNeutral, dstYPlaneSize, dstYPlaneSize + dstUPlaneSize * 2);

    // Inverse scale factors for nearest-neighbor source lookup
    const xScale = srcWidth / scaledWidth;
    const yScale = srcHeight/ scaledHeight;

    // Scale Y plane into the centred region using nearest-neighbor.
    // U and V are set to 128 (already done above) — grayscale has no chroma.
    for (let y = 0; y < scaledHeight; y++) {
      for (let x = 0; x < scaledWidth; x++) {
        const srcX = Math.floor(x * xScale);
        const srcY = Math.floor(y * yScale);
        scaledData[(y + yOffset) * width + (x + xOffset)] = yuvData[srcY * srcWidth + srcX];
      }
    }

    return {
      data: scaledData,
      width: width,
      height: height,
      format: 'YUV420P',
    };
  } else {
    throw new Error(`Unsupported video format for scaling: ${frame.format}`);
  }
}

class VonageVideoEchoServer {
  static readonly DEFAULT_SAMPLE_RATE: SampleRate = 48000;
  static readonly DEFAULT_NUMBER_OF_CHANNELS: NumberOfChannels = 2;
  static readonly AUDIO_TICK_INTERVAL = 10; // ms
  static readonly VIDEO_PUBLISH_WIDTH =  640; // pixels
  static readonly VIDEO_PUBLISH_HEIGHT =  480; // pixels
  static readonly VIDEO_PUBLISH_FPS =  30; // ms
  static readonly VIDEO_TICK_INTERVAL = 1000 / VonageVideoEchoServer.VIDEO_PUBLISH_FPS; // ms

  private sessionInfo: { apiKey: string; sessionId: string; token: string };
  private client: VonageVideoClient;
  private audioTimer: NodeJS.Timeout | null;
  private audioQueue: AudioData[];
  private videoTimer: NodeJS.Timeout | null;
  private videoQueue: VideoFrame[];
  private isPublishing: boolean;
  private streams: Map<string, Stream>;
  private echoedVideoStreamId: string | null;
  private sessionSettings: SessionSettings;
  private publisherSettings: PublisherSettings;

  constructor(sessionInfo: { apiKey: string; sessionId: string; token: string }) {
    this.sessionInfo = sessionInfo;
    this.client = new VonageVideoClient();

    this.audioTimer = null;
    this.audioQueue = [];
    this.videoTimer = null;
    this.videoQueue = [];
    this.isPublishing = false;
    this.streams = new Map<string, Stream>();
    this.echoedVideoStreamId = null;

    this.sessionSettings = {
      enableMigration: false,
      av: {
        audioPublisher: { sampleRate: VonageVideoEchoServer.DEFAULT_SAMPLE_RATE, channels: VonageVideoEchoServer.DEFAULT_NUMBER_OF_CHANNELS },
        audioSubscribersMix: { sampleRate: VonageVideoEchoServer.DEFAULT_SAMPLE_RATE, channels: VonageVideoEchoServer.DEFAULT_NUMBER_OF_CHANNELS },
        videoPublisher: { 
          width: VonageVideoEchoServer.VIDEO_PUBLISH_WIDTH,
          height: VonageVideoEchoServer.VIDEO_PUBLISH_HEIGHT,
          fps: VonageVideoEchoServer.VIDEO_PUBLISH_FPS,
          format: 'YUV420P'
        },
      },
      logging: { level: 'DEBUG' },
      // logging: { level: 'WARN' },
    };

    this.publisherSettings = {
      name: 'Video Connector Example Echo Server',
      hasAudio: true,
      hasVideo: true,
      audioSettings: {
        enableStereoMode: false,
        enableOpusDtx: true,
      },
    };
  }

  log(level: string, msg: string, ...args: unknown[]): void {
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

  onSessionError(session: Session, errorDescription: string, errorCode: number): void {
    this.log('error', `Session error: session_id=${session?.sessionId} desc=${errorDescription} code=${errorCode}`);
  }

  onReadyForAudio(session: Session): void {
    this.log('info', `Audio system ready, echo server is now active: session_id=${session?.sessionId}`);
    this.isPublishing = true;

    if (!this.audioTimer) {
      this.audioTimer = setInterval(() => this.audioEchoTick(), VonageVideoEchoServer.AUDIO_TICK_INTERVAL);
    }
  }

  onSessionDisconnected(session: Session): void {
    this.log('info', `Session disconnected: session_id=${session?.sessionId}`);
    this.stop();
  }

  async onStreamReceived(session: Session, stream: Stream): Promise<void> {
    this.log('info', `Stream received: session_id=${session?.sessionId} stream_id=${stream?.streamId}`);
    if (!this.videoTimer) {
      this.videoTimer = setInterval(() => this.videoEchoTick(), VonageVideoEchoServer.VIDEO_TICK_INTERVAL);
    }

    this.streams.set(stream?.streamId, stream);
    if(!this.echoedVideoStreamId) {
      this.echoedVideoStreamId = stream?.streamId;
    }

    const subscriber = await this.client.subscribe(
      stream,
      {
        onError: this.onSubscriberError.bind(this),
        onDisconnected: this.onSubscriberDisconnected.bind(this),
        onRenderFrame: (subscriber: Subscriber, frame: VideoFrame) => {
          if( subscriber.stream?.streamId === this.echoedVideoStreamId) {
            this.videoQueue.push(frame);
          }
        },
      }
    );

    if (!subscriber) {
      this.log('error', `Failed to subscribe to stream ${stream?.streamId}`);
    }

    this.log('info', `Subscribed to stream: session_id=${session?.sessionId} stream_id=${stream?.streamId}`);
  }

  onStreamDropped(session: Session, stream: Stream): void {
    this.log('info', `Stream dropped: session_id=${session?.sessionId} stream_id=${stream?.streamId}`);

    this.streams.delete(stream?.streamId);
    if(this.echoedVideoStreamId && stream?.streamId === this.echoedVideoStreamId) {
      this.echoedVideoStreamId = this.streams.size > 0 ? Array.from(this.streams.keys())[0] : null;
    }
    if (this.streams.size === 0) {
      this.log('info', 'No more streams, stopping video echo thread...');
      clearInterval(this.videoTimer);
      this.videoTimer = null;
    }
  }

  onConnectionCreated(session: Session, connection: Connection): void {
    this.log(
      'info',
      `Connection created: session_id=${session?.sessionId} connection_id=${connection?.connectionId} creation_time=${connection?.creationTime}`
    );
  }

  onConnectionDropped(session: Session, connection: Connection): void {
    const ownConnection = this.client.getConnection();
    const isOwn = ownConnection && connection?.connectionId === ownConnection.connectionId;
    const label = isOwn ? 'OWN' : 'OTHER';
    this.log('info', `Connection dropped [${label}]: session_id=${session?.sessionId} connection_id=${connection?.connectionId}`);
  }

  onSessionAudioData(_session: Session, audioData: AudioData): void {
    if (!this.isPublishing) return;
    this.audioQueue.push(audioData);
  }

  onSubscriberError(subscriber: Subscriber, errorDescription: string, errorCode: number): void {
    this.log('error', `Subscriber error: stream_id=${subscriber?.stream?.streamId} desc=${errorDescription} code=${errorCode}`);
  }

  onSubscriberDisconnected(subscriber: Subscriber): void {
    this.log('info', `Subscriber disconnected: stream_id=${subscriber?.stream?.streamId}`);
  }

  onPublisherError(publisher: Publisher, errorDescription: string, errorCode: number): void {
    this.log('error', `Publisher error: stream_id=${publisher?.stream?.streamId} desc=${errorDescription} code=${errorCode}`);
  }

  onStreamDestroyed(publisher: Publisher): void {
    this.log('info', `Publisher stream destroyed: stream_id=${publisher?.stream?.streamId}`);
    this.stop();
  }

  audioEchoTick() {
    if (this.audioQueue.length === 0) return;

    const audioData = this.audioQueue.shift();
    if (audioData === undefined) return;

    try {
      this.client.addAudio(audioData);
    } catch (error) {
      this.log('error', 'Error injecting echo audio', error);
    }
  }

  videoEchoTick() {
    if (this.videoQueue.length === 0) return;

    const videoFrame = this.videoQueue.shift();
    if (videoFrame === undefined) return;

    try {
      // Scale the video frame to the publisher's resolution before sending it back and take out the color information to make it grayscale
      const scaledFrame: VideoFrame = scaleGrayVideoFrame(videoFrame, VonageVideoEchoServer.VIDEO_PUBLISH_WIDTH, VonageVideoEchoServer.VIDEO_PUBLISH_HEIGHT);
      this.client.addVideo(scaledFrame);
    } catch (error) {
      this.log('error', 'Error injecting echo video', error);
    }
  }

  stop() {
    if (this.audioTimer) {
      this.log('info', 'Stopping audio echo thread...');
      clearInterval(this.audioTimer);
      this.audioTimer = null;
    }
    if (this.videoTimer) {
      this.log('info', 'Stopping video echo thread...');
      clearInterval(this.videoTimer);
      this.videoTimer = null;
    }
  }

  async connect() {
    this.log('info', 'Connecting to session...');

    const session = await this.client.connect(
      this.sessionInfo.apiKey,
      this.sessionInfo.sessionId,
      this.sessionInfo.token,
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
    });

    if (!session) {
      this.log('error', 'Failed to connect to session');
      return false;
    }

    const own_connection = this.client.getConnection();
    if (own_connection) {
        this.log('info', `Own connection: id=${own_connection.connectionId} creation_time=${own_connection.creationTime}`);
    }

    this.log('info', `Connected to session: session_id=${session?.sessionId}, echo server will echo back any received audio.`);
    return true;
  }

  async publish() {
    this.log('info', 'Starting publishing...');
    const publisher = await this.client.publish(
      {
        settings: this.publisherSettings,
        onError: this.onPublisherError.bind(this),
        onStreamDestroyed: this.onStreamDestroyed.bind(this),
      }
    );

    if (!publisher) {
      this.log('error', 'Failed to start publishing');
      return false;
    }

    return true;
  } 
  

  cleanup() {
    this.stop();

    this.log('info', 'Stopping publishing...');
    this.client.unpublish().catch(() => {
      this.log('warn', 'Failed to stop publishing');
    });

    const connected = this.client.isConnected();
    if (connected) {
      this.log('info', 'Disconnecting...');
      this.client.disconnect().catch(() => {
        this.log('warn', 'Failed to disconnect');
      });
    }
  }
}

function waitForShutdownSignal(): Promise<NodeJS.Signals> {
  return new Promise<NodeJS.Signals>((resolve) => {
    // Keep the event loop alive while waiting; signal listeners alone do not.
    const keepAlive = setInterval(() => {}, 1 << 30);

    const onSignal = (signal: NodeJS.Signals) => {
      clearInterval(keepAlive);
      resolve(signal);
    };

    process.once('SIGINT', onSignal);
    process.once('SIGTERM', onSignal);
  });
}

async function main() {

  const sessionInfoArg: string = process.argv[2];
  if (!sessionInfoArg) {
    console.error('Usage: node vonage_video_echo_server.js <session.json|json-string>');
    process.exit(1);
  }

  let sessionInfo: { apiKey: string; sessionId: string; token: string };
  try {
    sessionInfo = readSessionInfo(sessionInfoArg);
  } catch (error) {
    console.error('Invalid session info. Provide a JSON file path or a JSON string.', error instanceof Error ? error.message : error);
    process.exit(1);
  }

  const echoServer = new VonageVideoEchoServer(sessionInfo);

  try {
    if (!await echoServer.connect()) {
      process.exit(1);
    }

    if (!await echoServer.publish()) {
      process.exit(1);
    }

    echoServer.log('info', 'Echo server running. Press Ctrl+C to stop...');
    const signal = await waitForShutdownSignal();
    echoServer.log('info', `Received ${signal}, shutting down echo server...`);
  } catch (error) {
    echoServer.log('error', 'Unhandled runtime error', error);
    process.exitCode = 1;
  } finally {
    echoServer.cleanup();
  }
}

main();
