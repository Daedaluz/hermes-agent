# Fluxer

The Fluxer adapter connects Hermes to [Fluxer](https://github.com/fluxerapp/fluxer), a free and open-source, self-hostable Discord-alternative chat platform. It speaks Fluxer's wire protocol directly over `aiohttp` — REST for sending, a persistent WebSocket gateway for receiving — **no SDK required**. It works with the hosted [fluxer.app](https://fluxer.app) and any self-hosted instance.

Fluxer renders Discord-flavored markdown and supports guilds/channels, DMs, replies, reactions, and native file attachments. Interactive buttons are not part of Fluxer's message schema, so approval and clarify prompts use the plain-text flow.

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
```

Environment variables win over YAML.

## Behavior

- **DMs**: the bot always responds (subject to `FLUXER_ALLOWED_USERS`).
- **Guild channels**: requires an @mention by default. Relax with `FLUXER_REQUIRE_MENTION=false` or per-channel via `FLUXER_FREE_RESPONSE_CHANNELS`; restrict with `FLUXER_ALLOWED_CHANNELS`.
- **Media**: inbound attachments are cached locally for vision/transcription tools; outbound images, documents, voice, and video upload natively.
- **Message cap**: 4000 characters per message for bots — longer replies split on code-block-safe boundaries.
- **Safety**: outbound messages send `allowed_mentions: {parse: []}`, so agent output can never ping `@everyone` or roles.
- **Cron**: `deliver=fluxer` works with a live gateway or out-of-process via the standalone REST sender and `FLUXER_HOME_CHANNEL`.

## Access control

| Variable | Effect |
|----------|--------|
| `FLUXER_ALLOWED_USERS` | Comma-separated user IDs allowed to talk to the bot |
| `FLUXER_ALLOW_ALL_USERS` | `true` to allow anyone (dev only) |

With neither set, the bot ignores all users.
