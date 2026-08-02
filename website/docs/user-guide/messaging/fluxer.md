# Fluxer

The Fluxer adapter connects Hermes to [Fluxer](https://github.com/fluxerapp/fluxer), a free and open-source, self-hostable Discord-alternative chat platform. It speaks Fluxer's wire protocol directly over `aiohttp` — REST for sending, a persistent WebSocket gateway for receiving — **no SDK required**. It works with the hosted [fluxer.app](https://fluxer.app) and any self-hosted instance.

Fluxer renders Discord-flavored markdown and supports guilds/channels, DMs, replies, reactions, native file attachments, and **voice** — both guild voice channels and DM calls. Interactive buttons are not part of Fluxer's message schema, so approval and clarify prompts use the plain-text flow.

> Run `hermes gateway setup` and pick **Fluxer** for a guided walk-through.

## Prerequisites

- A bot application on your Fluxer instance (developer settings → create application → add bot → copy the token)
- The bot invited to your guild(s) with permission to read and send messages (plus attach files for media delivery)
- Self-hosted only: your instance's API base URL (e.g. `https://fluxer.example.org/v1`)

## Configure Hermes

### Option A — environment variables

```bash
FLUXER_BOT_TOKEN=<your bot token>              # required
FLUXER_API_BASE_URL=https://fluxer.example.org/v1  # optional — self-hosted only
FLUXER_ALLOWED_USERS=123456789,987654321       # recommended
FLUXER_HOME_CHANNEL=<channel id>               # cron / notification delivery
```

### Option B — config.yaml

```yaml
gateway:
  platforms:
    fluxer:
      enabled: true
fluxer:
  api_base_url: https://fluxer.example.org/v1
  require_mention: true              # @mention needed in guild channels
  free_response_channels: [123456789]
  allowed_channels: []               # empty = all channels
  voice_enabled: true                # voice channels + DM calls
  auto_answer_calls: true            # auto-answer DM rings from allowed users
  voice_timeout: 300                 # seconds of silence before auto-leave
```

Environment variables win over YAML.

## Behavior

- **DMs**: the bot always responds (subject to `FLUXER_ALLOWED_USERS`).
- **Guild channels**: requires an @mention by default. Relax with `FLUXER_REQUIRE_MENTION=false` or per-channel via `FLUXER_FREE_RESPONSE_CHANNELS`; restrict with `FLUXER_ALLOWED_CHANNELS`.
- **Media**: inbound attachments are cached locally for vision/transcription tools; outbound images, documents, voice, and video upload natively.
- **Message cap**: 4000 characters per message for bots — longer replies split on code-block-safe boundaries.
- **Safety**: outbound messages send `allowed_mentions: {parse: []}`, so agent output can never ping `@everyone` or roles.
- **Cron**: `deliver=fluxer` works with a live gateway or out-of-process via the standalone REST sender and `FLUXER_HOME_CHANNEL`.

## Voice

Fluxer's media plane is LiveKit (WebRTC); Hermes joins as a real voice participant — it hears you via speech-to-text and speaks replies via TTS.

- **Voice channels**: join a voice channel yourself, then type `/voice join` in a text channel. The bot joins your channel, transcribes what you say, and speaks its replies. `/voice leave` disconnects, `/voice status` lists participants, `/voice off` keeps it in the channel but replies text-only.
- **DM calls**: ring the bot in a DM — it answers automatically (allowed users only). Set `FLUXER_AUTO_ANSWER_CALLS=false` to disable.
- **Auto-leave**: after `FLUXER_VOICE_TIMEOUT` seconds of inactivity (default 300, `0` disables).
- **Requirements**: the `livekit` Python package (installed automatically on first join, or `pip install 'hermes-agent[fluxer-voice]'`) and `ffmpeg`.

:::warning E2EE downgrade
Fluxer voice can be end-to-end encrypted between human clients. The server allows bots to join but **turns off E2EE for the whole channel while the bot is present** — other participants will see the encryption indicator drop. Don't bring the bot into calls whose privacy depends on E2EE.
:::

## Access control

| Variable | Effect |
|----------|--------|
| `FLUXER_ALLOWED_USERS` | Comma-separated user IDs allowed to talk to the bot |
| `FLUXER_ALLOW_ALL_USERS` | `true` to allow anyone (dev only) |

With neither set, the bot ignores all users.
