# Fluxer Platform Plugin

Connects Hermes to [Fluxer](https://github.com/fluxerapp/fluxer), a free and
open-source (AGPL-3.0), self-hostable Discord-alternative chat platform.

The adapter speaks Fluxer's wire protocol directly with `aiohttp` — REST for
sending, a persistent WebSocket gateway (HELLO → IDENTIFY → HEARTBEAT →
READY, with RESUME support) for receiving.  No SDK required.

## Setup

1. **Create a bot** on your Fluxer instance: open developer/application
   settings, create an application, add a bot, and copy the bot token.
2. **Invite the bot** to your guild(s) with permission to read and send
   messages (and attach files, if you want media delivery).
3. **Configure Hermes** — either run `hermes setup` / `hermes gateway setup`
   and pick Fluxer, or set env vars directly:

```bash
FLUXER_BOT_TOKEN=<your bot token>          # required
FLUXER_API_BASE_URL=https://fluxer.my.org/v1   # optional — only for self-hosted
FLUXER_ALLOWED_USERS=123456789,987654321   # recommended
FLUXER_HOME_CHANNEL=<channel id>           # cron/notification delivery target
```

Or in `config.yaml`:

```yaml
gateway:
  platforms:
    fluxer:
      enabled: true
fluxer:
  api_base_url: https://fluxer.my.org/v1
  require_mention: true
  free_response_channels: [123456789]
  allowed_channels: []
```

## Behavior

- **DMs**: the bot always responds (subject to the user allowlist).
- **Guild channels**: requires an @mention by default
  (`FLUXER_REQUIRE_MENTION=false` or `FLUXER_FREE_RESPONSE_CHANNELS` to
  relax; `FLUXER_ALLOWED_CHANNELS` to whitelist channels).
- **Media**: inbound attachments are downloaded into the local media cache
  for vision/transcription tools; outbound images, documents, voice, and
  video are uploaded natively (multipart `payload_json` + `files[n]`,
  matched by attachment `id` — same convention as Discord).
- **Replies**: `message_reference` round-trips both ways.
- **Message cap**: 4000 characters for bots; longer replies are chunked on
  code-block-safe boundaries.
- **Mentions**: outbound messages send `allowed_mentions: {parse: []}` so
  agent output can never ping `@everyone`/roles.
- **Cron**: `deliver=fluxer` works both with a live gateway and
  out-of-process via the standalone REST sender + `FLUXER_HOME_CHANNEL`.
- **No interactive buttons**: Fluxer's message schema has no components/
  button equivalent (verified against its OpenAPI spec), so approval and
  clarify prompts degrade to the plain-text flow automatically.

## Voice (voice channels + DM calls)

Fluxer's media plane is **LiveKit (WebRTC)**, not Discord's UDP/RTP.  The
adapter signals over the existing gateway websocket (op 4
`VOICE_STATE_UPDATE` → `VOICE_SERVER_UPDATE` with a LiveKit endpoint +
JWT) and then joins the LiveKit room for audio.

- **Guild voice channels**: type `/voice join` in a text channel while you
  are in a voice channel — the bot joins, listens (speech-to-text), and
  speaks replies (TTS, including streaming TTS).  `/voice leave`
  disconnects; `/voice status` shows who's in the channel.
- **DM calls**: ring the bot in a DM and it auto-answers (allowed users
  only; disable with `FLUXER_AUTO_ANSWER_CALLS=false`).
- **Inactivity**: the bot auto-leaves after `FLUXER_VOICE_TIMEOUT` seconds
  (default 300) of silence.
- **Dependencies**: the `livekit` package (lazy-installed on first join, or
  `pip install 'hermes-agent[fluxer-voice]'`) plus `ffmpeg` on PATH.

> ⚠️ **E2EE downgrade**: Fluxer voice channels can be end-to-end encrypted
> between human clients.  The server permits bot joins but **downgrades
> E2EE for the whole channel while a bot is present**
> (`guild_voice_e2ee.erl: join_downgrades_e2ee`) — participants will see
> the E2EE indicator drop.  Don't invite the bot into calls whose privacy
> depends on E2EE.

```bash
FLUXER_VOICE_ENABLED=true        # default
FLUXER_AUTO_ANSWER_CALLS=true    # default; allowed users only
FLUXER_VOICE_TIMEOUT=300         # seconds; 0 disables auto-leave
```

## Protocol notes (derived from Fluxer source)

- Opcodes: Discord-style numbering (`DISPATCH=0`, `HEARTBEAT=1`,
  `IDENTIFY=2`, `RESUME=6`, `RECONNECT=7`, `INVALID_SESSION=9`, `HELLO=10`,
  `HEARTBEAT_ACK=11`) plus Fluxer-specific `GATEWAY_ERROR=12` and ops 14–16
  (`packages/constants/src/GatewayConstants.ts`).
- IDENTIFY has **no `intents` field** — `{token, properties: {os, browser,
  device}, shard?}` (`gateway_handler_identify.erl`); close codes 4013/4014
  do not exist.
- Frames are JSON only — no ETF or zlib (`gateway_codec.erl`).
- IDENTIFY pacing honors `session_start_limit.max_concurrency` from
  `GET /gateway/bot`.
- Rate limiting is Discord-shaped: 429 body `{code, message, retry_after,
  global}`; the adapter sleeps `retry_after` and retries.
