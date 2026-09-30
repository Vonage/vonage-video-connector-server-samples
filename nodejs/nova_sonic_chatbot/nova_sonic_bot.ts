// Wiring between the Vonage Video session and the Nova Sonic conversation:
// participant audio goes up to the model, generated speech comes back down.
//
// Environment variables:
//   AWS_REGION / AWS_DEFAULT_REGION   Region hosting Nova Sonic (default: us-east-1).
//   AWS credentials                   Resolved by the default AWS SDK provider chain
//                                     (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
//                                     AWS_SESSION_TOKEN, shared config, or IAM role).
//   NOVA_SONIC_MODEL_ID               Model id (default: amazon.nova-2-sonic-v1:0).
//   NOVA_SONIC_VOICE_ID               Output voice (default: tiffany).
//   NOVA_SONIC_GREETING               Prompt used to make the bot speak first.
//                                     Set to an empty string to stay silent.
//   NOVA_SONIC_ENDPOINTING_SENSITIVITY
//                                     LOW, MEDIUM or HIGH (default: HIGH). Set to an
//                                     empty string for amazon.nova-sonic-v1:0, which
//                                     does not support this setting.

import type { NumberOfChannels, SampleRate } from '@vonage/video-connector';

import { HeyGenAvatar } from './heygen_avatar.ts';
import { NovaSonicClient, type EndpointingSensitivity } from './nova_sonic_client.ts';
import { VonageVideoTransport, log, type SessionInfo } from './vonage_video_transport.ts';

// Nova Sonic accepts 8, 16 or 24 kHz mono input and produces the same rates on
// output. 16 kHz in keeps the uplink small, 24 kHz out keeps the voice crisp;
// the connector resamples the session audio to match, so this file never does.
const INPUT_SAMPLE_RATE: SampleRate = 16000;
const OUTPUT_SAMPLE_RATE: SampleRate = 24000;
const CHANNELS: NumberOfChannels = 1;
// Batch uplink audio into ~32 ms chunks, the cadence AWS documents for
// audioInput, instead of one event per 10 ms connector callback. Larger chunks
// mean fewer JSON/base64 events per second at the cost of response latency.
const INPUT_CHUNK_BYTES = 1024;

// LiveAvatar outputs 48 kHz mono audio and 1280x720 video.
const AVATAR_AUDIO_SAMPLE_RATE: SampleRate = 48000;
const AVATAR_VIDEO_SETTINGS = { width: 1280, height: 720, fps: 30 };

const SYSTEM_PROMPT =
  'You are a friendly assistant. The user and you will engage in a spoken dialog exchanging ' +
  'the transcripts of a natural real-time conversation. Keep your responses short, generally ' +
  'two or three sentences for chatty scenarios.';

export class NovaSonicBot {
  private readonly transport: VonageVideoTransport;
  private readonly novaSonic: NovaSonicClient;
  private readonly avatar: HeyGenAvatar | null;
  private readonly closed: Promise<void>;
  private markClosed!: () => void;

  private conversationStarted = false;
  private greetingSent = false;

  constructor(sessionInfo: SessionInfo) {
    this.closed = new Promise<void>((resolve) => {
      this.markClosed = resolve;
    });

    const isAvatarEnabled = Boolean(process.env.HEYGEN_API_KEY);

    this.transport = new VonageVideoTransport(
      {
        sessionInfo,
        publisherName: isAvatarEnabled ? 'Nova Sonic avatar bot' : 'Nova Sonic bot',
        inputSampleRate: INPUT_SAMPLE_RATE,
        // With the avatar in the loop the published audio is the avatar's, so the
        // publisher must be configured for what LiveAvatar returns.
        outputSampleRate: isAvatarEnabled ? AVATAR_AUDIO_SAMPLE_RATE : OUTPUT_SAMPLE_RATE,
        channels: CHANNELS,
        inputChunkBytes: INPUT_CHUNK_BYTES,
        videoSettings: isAvatarEnabled ? AVATAR_VIDEO_SETTINGS : undefined,
        logLevel: 'WARN',
      },
      {
        onAudioReceived: (audio: Buffer) => this.novaSonic.sendAudio(audio),
        onReady: () => void this.startConversation(),
        onParticipantJoined: () => this.sendGreeting(),
        onClosed: () => {
          this.stop();
          this.markClosed();
        },
      },
    );

    // Publishing the avatar's own audio alongside its video is what keeps the
    // voice and the lips aligned; Nova Sonic's audio is not published directly.
    this.avatar = isAvatarEnabled
      ? new HeyGenAvatar({
          onVideoFrame: (frame) => this.transport.playVideo(frame),
          onAudioFrame: (audio) => this.transport.playAudio(audio),
          onClosed: () => {
            this.stop();
            this.markClosed();
          },
        })
      : null;

    this.novaSonic = new NovaSonicClient(
      {
        region: process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'us-east-1',
        modelId: process.env.NOVA_SONIC_MODEL_ID ?? 'amazon.nova-2-sonic-v1:0',
        voiceId: process.env.NOVA_SONIC_VOICE_ID ?? 'tiffany',
        systemPrompt: SYSTEM_PROMPT,
        inputSampleRate: INPUT_SAMPLE_RATE,
        outputSampleRate: OUTPUT_SAMPLE_RATE,
        endpointingSensitivity: (process.env.NOVA_SONIC_ENDPOINTING_SENSITIVITY ?? 'HIGH') as
          | EndpointingSensitivity
          | undefined,
      },
      {
        onAudioOutput: (audio: Buffer) =>
          this.avatar ? this.avatar.speak(audio) : this.transport.playAudio(audio),
        onTranscript: (role: string, text: string) => log('info', `${role}: ${text}`),
        onInterrupted: () => {
          log('info', 'Barge-in detected, dropping pending bot audio');
          this.avatar?.interrupt();
          this.transport.clearAudio();
        },
        onError: (error: unknown) => log('error', 'Nova Sonic stream error', error),
      },
    );
  }

  /** Join the session and start publishing. */
  async connect(): Promise<boolean> {
    if (this.avatar) {
      // Fail fast: a broken avatar session would otherwise surface as a silent
      // video-less stream well after the bot has joined.
      await this.avatar.start();
    }
    return (await this.transport.connect()) && (await this.transport.publish());
  }

  /** Resolves when the session ends or the published stream is destroyed. */
  whenClosed(): Promise<void> {
    return this.closed;
  }

  async cleanup(): Promise<void> {
    this.stop();
    if (this.avatar) await this.avatar.stop();
    await this.transport.cleanup();
  }

  private async startConversation(): Promise<void> {
    if (this.conversationStarted) return;

    try {
      await this.novaSonic.start();
      this.conversationStarted = true;
      log('info', 'Nova Sonic conversation started');
      this.sendGreeting();
    } catch (error) {
      log('error', 'Failed to start the Nova Sonic conversation', error);
    }
  }

  /**
   * Make the bot speak first, once the conversation is up and someone is
   * listening. An empty NOVA_SONIC_GREETING keeps the bot silent until a
   * participant speaks.
   */
  private sendGreeting(): void {
    if (this.greetingSent || !this.conversationStarted || this.transport.participantCount === 0) {
      return;
    }

    const greeting = process.env.NOVA_SONIC_GREETING ?? 'Tell me a fun fact!';
    if (!greeting) return;

    this.greetingSent = true;
    this.novaSonic.sendTextTurn('USER', greeting, true);
  }

  private stop(): void {
    this.transport.stop();
    this.novaSonic.stop();
    this.conversationStarted = false;
  }
}
