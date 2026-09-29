// Minimal AWS Nova Sonic speech-to-speech client built on the Bedrock
// bidirectional streaming API (InvokeModelWithBidirectionalStream).
//
// Event protocol reference:
// https://docs.aws.amazon.com/nova/latest/userguide/speech-bidirection.html

import { randomUUID } from 'crypto';

import {
  BedrockRuntimeClient,
  InvokeModelWithBidirectionalStreamCommand,
} from '@aws-sdk/client-bedrock-runtime';
import { NodeHttp2Handler } from '@smithy/node-http-handler';

/** Endpointing sensitivity for Nova Sonic server-side turn detection. */
export type EndpointingSensitivity = 'LOW' | 'MEDIUM' | 'HIGH';

/** Speaker of a conversation turn. */
export type Role = 'USER' | 'ASSISTANT' | 'SYSTEM';

/** Configuration for a Nova Sonic conversation. */
export interface NovaSonicOptions {
  /** AWS region hosting the Nova Sonic model. */
  region: string;
  /** Bedrock model identifier. */
  modelId: string;
  /** Output voice. */
  voiceId: string;
  /** System instruction sent before any audio. */
  systemPrompt: string;
  /** Sample rate of the PCM16 audio pushed with sendAudio(). */
  inputSampleRate: number;
  /** Sample rate requested for the generated PCM16 audio. */
  outputSampleRate: number;
  /**
   * Server-side turn detection sensitivity.
   * Only supported by newer Sonic versions; leave unset otherwise.
   */
  endpointingSensitivity?: EndpointingSensitivity;
  /** Optional inference tuning; the AWS defaults below are used when unset. */
  maxTokens?: number;
  topP?: number;
  temperature?: number;
}

/** Inference defaults, as documented in the AWS bidirectional streaming guide. */
const DEFAULT_MAX_TOKENS = 1024;
const DEFAULT_TOP_P = 0.9;
const DEFAULT_TEMPERATURE = 0.7;

/** Callbacks invoked while the conversation is running. */
export interface NovaSonicHandlers {
  /** Generated speech, PCM16 mono at `outputSampleRate`. */
  onAudioOutput: (pcm: Buffer) => void;
  /** Transcript of a user or assistant turn. */
  onTranscript: (role: string, text: string) => void;
  /** The user started talking over the assistant: drop pending audio. */
  onInterrupted: () => void;
  /** The stream failed or was closed by the service. */
  onError: (error: unknown) => void;
}

/** Async queue of JSON events feeding the request stream. */
class EventQueue {
  private items: string[] = [];
  private closed = false;
  private wake: (() => void) | null = null;

  push(event: string): void {
    if (this.closed) return;
    this.items.push(event);
    this.signal();
  }

  close(): void {
    this.closed = true;
    this.signal();
  }

  private signal(): void {
    const wake = this.wake;
    this.wake = null;
    wake?.();
  }

  async *drain(): AsyncGenerator<string> {
    for (;;) {
      while (this.items.length > 0) {
        yield this.items.shift() as string;
      }
      if (this.closed) return;
      await new Promise<void>((resolve) => {
        this.wake = resolve;
      });
    }
  }
}

export class NovaSonicClient {
  private readonly options: NovaSonicOptions;
  private readonly handlers: NovaSonicHandlers;
  private readonly bedrock: BedrockRuntimeClient;
  private readonly queue = new EventQueue();
  private readonly promptName = randomUUID();
  private readonly audioContentName = randomUUID();
  private readonly encoder = new TextEncoder();

  private started = false;
  /** Role and generation stage of the content block being received. */
  private currentRole = '';
  private isCurrentContentSpeculative = false;

  constructor(options: NovaSonicOptions, handlers: NovaSonicHandlers) {
    this.options = options;
    this.handlers = handlers;
    this.bedrock = new BedrockRuntimeClient({
      region: options.region,
      // Bidirectional streaming requires HTTP/2 with concurrent streams enabled.
      requestHandler: new NodeHttp2Handler({
        requestTimeout: 300_000,
        sessionTimeout: 300_000,
        disableConcurrentStreams: false,
        maxConcurrentStreams: 20,
      }),
    });
  }

  /**
   * Open the bidirectional stream and start the conversation.
   * Resolves once the request is accepted; responses are delivered to the handlers.
   */
  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;

    this.sendSessionStart();
    this.sendPromptStart();
    this.sendTextTurn('SYSTEM', this.options.systemPrompt, false);
    this.sendAudioContentStart();

    const response = await this.bedrock.send(
      new InvokeModelWithBidirectionalStreamCommand({
        modelId: this.options.modelId,
        body: this.requestStream(),
      }),
    );

    void this.readResponses(response.body);
  }

  /** Push PCM16 mono audio at `inputSampleRate` into the conversation. */
  sendAudio(pcm: Buffer): void {
    if (!this.started) return;
    this.queue.push(
      JSON.stringify({
        event: {
          audioInput: {
            promptName: this.promptName,
            contentName: this.audioContentName,
            content: pcm.toString('base64'),
          },
        },
      }),
    );
  }

  /**
   * Send a text turn.
   *
   * @param role - Speaker of the turn.
   * @param text - The text content.
   * @param interactive - True for text injected during the live audio turn
   *                      (e.g. a greeting trigger), false for history and
   *                      system instructions sent before the audio starts.
   */
  sendTextTurn(role: Role, text: string, interactive: boolean): void {
    if (!text) return;

    const contentName = randomUUID();
    this.queue.push(
      JSON.stringify({
        event: {
          contentStart: {
            promptName: this.promptName,
            contentName,
            type: 'TEXT',
            interactive,
            role,
            textInputConfiguration: { mediaType: 'text/plain' },
          },
        },
      }),
    );
    this.queue.push(
      JSON.stringify({
        event: {
          textInput: { promptName: this.promptName, contentName, content: text },
        },
      }),
    );
    this.queue.push(
      JSON.stringify({
        event: { contentEnd: { promptName: this.promptName, contentName } },
      }),
    );
  }

  /** Close the audio turn, the prompt and the session, then end the stream. */
  stop(): void {
    if (!this.started) return;
    this.started = false;

    this.queue.push(
      JSON.stringify({
        event: {
          contentEnd: { promptName: this.promptName, contentName: this.audioContentName },
        },
      }),
    );
    this.queue.push(JSON.stringify({ event: { promptEnd: { promptName: this.promptName } } }));
    this.queue.push(JSON.stringify({ event: { sessionEnd: {} } }));
    this.queue.close();
  }

  private sendSessionStart(): void {
    const inferenceConfiguration = {
      maxTokens: this.options.maxTokens ?? DEFAULT_MAX_TOKENS,
      topP: this.options.topP ?? DEFAULT_TOP_P,
      temperature: this.options.temperature ?? DEFAULT_TEMPERATURE,
    };

    this.queue.push(
      JSON.stringify({
        event: {
          sessionStart: this.options.endpointingSensitivity
            ? {
                inferenceConfiguration,
                turnDetectionConfiguration: {
                  endpointingSensitivity: this.options.endpointingSensitivity,
                },
              }
            : { inferenceConfiguration },
        },
      }),
    );
  }

  private sendPromptStart(): void {
    this.queue.push(
      JSON.stringify({
        event: {
          promptStart: {
            promptName: this.promptName,
            textOutputConfiguration: { mediaType: 'text/plain' },
            audioOutputConfiguration: {
              mediaType: 'audio/lpcm',
              sampleRateHertz: this.options.outputSampleRate,
              sampleSizeBits: 16,
              channelCount: 1,
              voiceId: this.options.voiceId,
              encoding: 'base64',
              audioType: 'SPEECH',
            },
          },
        },
      }),
    );
  }

  private sendAudioContentStart(): void {
    this.queue.push(
      JSON.stringify({
        event: {
          contentStart: {
            promptName: this.promptName,
            contentName: this.audioContentName,
            type: 'AUDIO',
            interactive: true,
            role: 'USER',
            audioInputConfiguration: {
              mediaType: 'audio/lpcm',
              sampleRateHertz: this.options.inputSampleRate,
              sampleSizeBits: 16,
              channelCount: 1,
              audioType: 'SPEECH',
              encoding: 'base64',
            },
          },
        },
      }),
    );
  }

  private async *requestStream(): AsyncGenerator<{ chunk: { bytes: Uint8Array } }> {
    for await (const event of this.queue.drain()) {
      yield { chunk: { bytes: this.encoder.encode(event) } };
    }
  }

  private async readResponses(
    body: AsyncIterable<{ chunk?: { bytes?: Uint8Array } }> | undefined,
  ): Promise<void> {
    if (!body) return;

    try {
      for await (const output of body) {
        const bytes = output.chunk?.bytes;
        if (!bytes) continue;

        const event = JSON.parse(Buffer.from(bytes).toString('utf8')).event;
        if (!event) continue;

        if (event.contentStart) {
          this.currentRole = event.contentStart.role ?? this.currentRole;
          // Each turn is emitted twice: once speculatively while the model is
          // still generating, then again as the final version.
          this.isCurrentContentSpeculative = this.isSpeculative(event.contentStart);
        } else if (event.audioOutput) {
          this.handlers.onAudioOutput(Buffer.from(event.audioOutput.content, 'base64'));
        } else if (event.textOutput) {
          const text: string = event.textOutput.content ?? '';
          // Barge-in is reported as a synthetic text payload, not as a transcript.
          if (text.includes('"interrupted"')) {
            this.handlers.onInterrupted();
          } else if (!this.isCurrentContentSpeculative) {
            this.handlers.onTranscript(event.textOutput.role ?? this.currentRole, text);
          }
        }
      }
    } catch (error) {
      this.handlers.onError(error);
    }
  }

  private isSpeculative(contentStart: { additionalModelFields?: string }): boolean {
    if (!contentStart.additionalModelFields) return false;
    try {
      return JSON.parse(contentStart.additionalModelFields).generationStage === 'SPECULATIVE';
    } catch {
      return false;
    }
  }
}
