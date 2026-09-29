// Optional HeyGen LiveAvatar adapter.
// Sends Nova Sonic audio to LiveAvatar and receives synchronized audio and video.

import {
  AudioStream,
  Room,
  RoomEvent,
  TrackKind,
  VideoBufferType,
  VideoStream,
  dispose,
  type RemoteTrack,
} from '@livekit/rtc-node';

import { log } from './vonage_video_transport.ts';

const LIVE_AVATAR_API_URL = 'https://api.liveavatar.com';
/** Stock demo avatar, matching the Python sample. Renders at 1280x720. */
const DEFAULT_AVATAR_ID = '513fd1b7-7ef9-466d-9af2-344e51eeb833';

/** A decoded avatar video frame, already in the connector's YUV420P layout. */
interface AvatarVideoFrame {
  data: Buffer;
  width: number;
  height: number;
}

interface HeyGenAvatarHandlers {
  onVideoFrame: (frame: AvatarVideoFrame) => void;
  onAudioFrame: (audio: Buffer) => void;
  /** The avatar session ended on its own. */
  onClosed: () => void;
}

interface LiveAvatarSession {
  session_id: string;
  livekit_url: string;
  livekit_client_token: string;
  ws_url: string;
  max_session_duration: number;
}

export class HeyGenAvatar {
  /** LiveAvatar requires PCM16 at this rate; Nova Sonic already emits it. */
  static readonly REQUIRED_SAMPLE_RATE = 24000;
  /**
   * Batch generated speech before sending. LiveAvatar suggests ~1 s chunks, but
   * that adds a second of latency to every reply, so we trade packet count for
   * responsiveness.
   */
  private static readonly SPEAK_CHUNK_MS = 200;
  private static readonly KEEP_ALIVE_INTERVAL_MS = 30_000;

  private readonly apiKey: string;
  private readonly avatarId: string;
  private readonly eventHandlers: HeyGenAvatarHandlers;

  private sessionToken: string | null = null;
  private room: Room | null = null;
  private controlSocket: WebSocket | null = null;
  private keepAliveTimer: NodeJS.Timeout | null = null;
  private audioFlushTimer: NodeJS.Timeout | null = null;
  private pendingAudio: Buffer = Buffer.alloc(0);
  private readonly audioChunkSizeBytes: number;
  private isStopped = false;

  constructor(eventHandlers: HeyGenAvatarHandlers) {
    const apiKey = process.env.HEYGEN_API_KEY;
    if (!apiKey) throw new Error('HEYGEN_API_KEY is not set');

    this.apiKey = apiKey;
    this.avatarId = process.env.HEYGEN_AVATAR_ID ?? DEFAULT_AVATAR_ID;
    this.eventHandlers = eventHandlers;
    // PCM16 mono: 2 bytes per sample.
    this.audioChunkSizeBytes =
      (HeyGenAvatar.REQUIRED_SAMPLE_RATE * HeyGenAvatar.SPEAK_CHUNK_MS * 2) / 1000;
  }

  /** Create and start the avatar session, then join its LiveKit room. */
  async start(): Promise<void> {
    const session = await this.createSession();
    log(
      'info',
      `LiveAvatar session ${session.session_id} started ` +
      `(max ${session.max_session_duration}s)`,
    );

    await this.joinRoom(session);
    this.openControlSocket(session.ws_url);
  }

  private async createSession(): Promise<LiveAvatarSession> {
    const tokenResponse = await this.postLiveAvatarRequest(
      '/v1/sessions/token',
      { 'X-API-KEY': this.apiKey },
      { avatar_id: this.avatarId, mode: 'LITE' },
    );

    this.sessionToken = tokenResponse.session_token as string;

    // Starting the session authenticates with the freshly minted session token,
    // not the account API key.
    const session = await this.postLiveAvatarRequest(
      '/v1/sessions/start',
      this.authorizationHeaders(),
      {},
    );
    return session as unknown as LiveAvatarSession;
  }

  private authorizationHeaders(): Record<string, string> {
    return { Authorization: `Bearer ${this.sessionToken}` };
  }

  private async postLiveAvatarRequest(
    path: string,
    authorizationHeaders: Record<string, string>,
    body: unknown,
  ): Promise<Record<string, unknown>> {
    const response = await fetch(LIVE_AVATAR_API_URL + path, {
      method: 'POST',
      headers: { ...authorizationHeaders, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    const responseBody = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    if (!response.ok) {
      throw new Error(
        `LiveAvatar ${path} failed [${response.status}]: ` + JSON.stringify(responseBody),
      );
    }
    // Successful calls that carry no payload (stop) return data: null.
    return (responseBody.data ?? {}) as Record<string, unknown>;
  }

  private async joinRoom(session: LiveAvatarSession): Promise<void> {
    const room = new Room();
    this.room = room;

    room.on(RoomEvent.TrackSubscribed, (track: RemoteTrack) => this.handleSubscribedTrack(track));
    room.on(RoomEvent.Disconnected, () => {
      if (!this.isStopped) {
        log('warn', 'LiveAvatar LiveKit room disconnected');
        this.eventHandlers.onClosed();
      }
    });

    await room.connect(session.livekit_url, session.livekit_client_token, {
      autoSubscribe: true,
      dynacast: false,
    });
    log('info', 'Joined the LiveAvatar LiveKit room');
  }

  private handleSubscribedTrack(track: RemoteTrack): void {
    if (track.kind === TrackKind.KIND_VIDEO) {
      void (async () => {
        try {
          for await (const videoEvent of new VideoStream(track)) {
            const i420Frame = videoEvent.frame.convert(VideoBufferType.I420);
            this.eventHandlers.onVideoFrame({
              data: Buffer.from(i420Frame.data),
              width: i420Frame.width,
              height: i420Frame.height,
            });
          }
        } catch (error) {
          if (!this.isStopped) log('error', 'Avatar video stream error', error);
        }
      })();
    } else if (track.kind === TrackKind.KIND_AUDIO) {
      void (async () => {
        try {
          for await (const audioFrame of new AudioStream(track)) {
            const audio = Buffer.from(
              audioFrame.data.buffer,
              audioFrame.data.byteOffset,
              audioFrame.data.byteLength,
            );
            this.eventHandlers.onAudioFrame(audio);
          }
        } catch (error) {
          if (!this.isStopped) log('error', 'Avatar audio stream error', error);
        }
      })();
    }
  }

  private openControlSocket(controlSocketUrl: string): void {
    const controlSocket = new WebSocket(controlSocketUrl);
    this.controlSocket = controlSocket;

    controlSocket.onopen = () => {
      log('info', 'LiveAvatar control socket open');
      this.keepAliveTimer = setInterval(
        () => this.sendControlMessage('session.keep_alive'),
        HeyGenAvatar.KEEP_ALIVE_INTERVAL_MS,
      );
      // Also flush on a timer so the tail of a reply, which is usually shorter
      // than a full chunk, is not held back until the next one.
      this.audioFlushTimer = setInterval(
        () => this.flushPendingAudio(),
        HeyGenAvatar.SPEAK_CHUNK_MS,
      );
    };

    controlSocket.onmessage = (event) => {
      if (typeof event.data !== 'string') return;
      try {
        const message = JSON.parse(event.data) as { type?: string; error?: { message?: string } };
        if (message.type === 'error') {
          log('error', `LiveAvatar control error: ${message.error?.message ?? 'unknown'}`);
        }
      } catch {
        /* ignore frames that are not JSON */
      }
    };

    controlSocket.onerror = () => log('error', 'LiveAvatar control socket error');
    controlSocket.onclose = () => {
      if (!this.isStopped) {
        log('warn', 'LiveAvatar control socket closed');
        this.eventHandlers.onClosed();
      }
    };
  }

  /** Queue generated speech (PCM16 mono at {@link REQUIRED_SAMPLE_RATE}). */
  speak(audio: Buffer): void {
    this.pendingAudio = Buffer.concat([this.pendingAudio, audio]);
    if (this.pendingAudio.length >= this.audioChunkSizeBytes) this.flushPendingAudio();
  }

  /** Drop queued speech and stop the avatar mid-sentence. */
  interrupt(): void {
    this.pendingAudio = Buffer.alloc(0);
    this.sendControlMessage('agent.interrupt');
  }

  /** Send everything queued, split into chunks of at most SPEAK_CHUNK_MS. */
  private flushPendingAudio(): void {
    while (this.pendingAudio.length > 0) {
      const chunkSize = Math.min(this.audioChunkSizeBytes, this.pendingAudio.length);
      const audioChunk = this.pendingAudio.subarray(0, chunkSize);
      this.pendingAudio = this.pendingAudio.subarray(chunkSize);
      this.sendControlMessage('agent.speak', { audio: audioChunk.toString('base64') });
    }
  }

  private sendControlMessage(type: string, fields: Record<string, unknown> = {}): void {
    if (this.controlSocket?.readyState !== WebSocket.OPEN) return;
    try {
      this.controlSocket.send(JSON.stringify({ type, ...fields }));
    } catch (error) {
      log('error', `Failed to send ${type} to LiveAvatar`, error);
    }
  }

  async stop(): Promise<void> {
    if (this.isStopped) return;
    this.isStopped = true;

    for (const timer of [this.keepAliveTimer, this.audioFlushTimer]) {
      if (timer) clearInterval(timer);
    }
    this.keepAliveTimer = null;
    this.audioFlushTimer = null;

    try { this.controlSocket?.close(); } catch { /* already closing */ }

    if (this.room) {
      try { await this.room.disconnect(); } catch { /* already gone */ }
      this.room = null;
    }

    if (this.sessionToken) {
      try {
        await this.postLiveAvatarRequest('/v1/sessions/stop', this.authorizationHeaders(), {});
        log('info', 'LiveAvatar session stopped');
      } catch (error) {
        log('warn', `Failed to stop the LiveAvatar session: ${(error as Error).message}`);
      }
      this.sessionToken = null;
    }

    // Releases the native LiveKit worker threads so the process can exit.
    try { await dispose(); } catch { /* nothing to release */ }
  }
}
