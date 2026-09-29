#!/usr/bin/env node

// Vonage Video Connector + AWS Nova Sonic chatbot.
//
// The bot joins a Vonage Video session, streams the mixed audio of the other
// participants to Nova Sonic, and publishes the generated speech back into the
// session.  Turn detection and barge-in are handled by Nova Sonic itself, so no
// local VAD is needed.
//
// Layout:
//   main.ts                    entrypoint: arguments, signals, shutdown
//   nova_sonic_bot.ts          configuration and wiring between the two sides
//   vonage_video_transport.ts  Vonage Video Connector plumbing
//   nova_sonic_client.ts       Bedrock bidirectional streaming protocol
//
// See nova_sonic_bot.ts for the supported environment variables.

import * as fs from 'fs';

import { NovaSonicBot } from './nova_sonic_bot.ts';
import { log, type SessionInfo } from './vonage_video_transport.ts';

function readSessionInfo(sessionInfoArg: string): SessionInfo {
  if (fs.existsSync(sessionInfoArg) && fs.statSync(sessionInfoArg).isFile()) {
    return JSON.parse(fs.readFileSync(sessionInfoArg, 'utf8'));
  }
  return JSON.parse(sessionInfoArg);
}

/** Resolves on the first SIGINT or SIGTERM, so a second one can still kill us. */
function waitForSignal(): Promise<void> {
  return new Promise<void>((resolve) => {
    process.once('SIGINT', () => resolve());
    process.once('SIGTERM', () => resolve());
  });
}

async function main(): Promise<void> {
  const sessionInfoArg: string = process.argv[2];
  if (!sessionInfoArg) {
    console.error('Usage: node main.ts <session.json|json-string>');
    process.exit(1);
  }

  let sessionInfo: SessionInfo;
  try {
    sessionInfo = readSessionInfo(sessionInfoArg);
  } catch (error) {
    console.error(
      'Invalid session info. Provide a JSON file path or a JSON string.',
      error instanceof Error ? error.message : error,
    );
    process.exit(1);
  }

  const bot = new NovaSonicBot(sessionInfo);

  try {
    if (!(await bot.connect())) {
      process.exit(1);
    }

    log('info', 'Nova Sonic bot running. Press Ctrl+C to stop...');
    // Exit on Ctrl+C, or by itself if the session ends.
    await Promise.race([waitForSignal(), bot.whenClosed()]);
    log('info', 'Shutting down...');
  } catch (error) {
    log('error', 'Unhandled runtime error', error);
    process.exitCode = 1;
  } finally {
    await bot.cleanup();
    // The native library keeps background threads running, so exit explicitly.
    process.exit(process.exitCode ?? 0);
  }
}

main();
