"""Fluxer gateway adapter.

Connects to a Fluxer instance (hosted fluxer.app or self-hosted) via its
Discord-shaped REST API + persistent WebSocket gateway.  No SDK required —
raw aiohttp against the wire protocol derived from Fluxer's source
(github.com/fluxerapp/fluxer):

- Opcodes from ``packages/constants/src/GatewayConstants.ts`` — Discord-style
  numbering (DISPATCH=0 … HEARTBEAT_ACK=11) plus Fluxer-specific extras
  (GATEWAY_ERROR=12, LAZY_REQUEST=14, …).  JSON envelopes only
  (``fluxer_gateway/src/gateway/gateway_codec.erl`` — no ETF/zlib).
- IDENTIFY takes ``{token, properties: {os, browser, device}, shard?}`` —
  there is NO ``intents`` field (Fluxer has no intent system; close codes
  4013/4014 don't exist).
- Dispatch event names are the uppercased internal atoms
  (``constants:dispatch_event_atom/1``): READY, RESUMED, MESSAGE_CREATE, …
- Message create supports multipart/form-data with Discord's exact
  ``payload_json`` + ``files[n]`` convention, attachments matched by
  ``id`` == file index (``fluxer_api/.../MessageRequestParser.ts``).
- Bots may send up to 4000 characters per message
  (``MessageContentRequest`` in the OpenAPI spec: "premium users, bots,
  and webhooks can send up to 4000 characters").

Environment variables:
    FLUXER_BOT_TOKEN               Bot token (``Authorization: Bot <token>``)
    FLUXER_API_BASE_URL            API base URL; defaults to the hosted
                                   instance ``https://api.fluxer.app/v1``.
                                   Self-hosters point this at their server.
    FLUXER_ALLOWED_USERS           Comma-separated user IDs
    FLUXER_ALLOW_ALL_USERS         Allow any user (dev only)
    FLUXER_HOME_CHANNEL            Channel ID for cron/notification delivery
    FLUXER_REQUIRE_MENTION         Require @mention in guild channels (default true)
    FLUXER_FREE_RESPONSE_CHANNELS  Channel IDs where no mention is needed
    FLUXER_ALLOWED_CHANNELS        If set, only respond in these guild channels
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    classify_send_error,
)

logger = logging.getLogger(__name__)

DEFAULT_API_BASE_URL = "https://api.fluxer.app/v1"

# Bot accounts may send up to 4000 characters (MessageContentRequest in
# Fluxer's OpenAPI spec; non-premium *users* get 2000, bots get 4000).
MAX_MESSAGE_LENGTH = 4000

# ── Gateway opcodes (packages/constants/src/GatewayConstants.ts) ──────────
OP_DISPATCH = 0
OP_HEARTBEAT = 1
OP_IDENTIFY = 2
OP_RESUME = 6
OP_RECONNECT = 7
OP_INVALID_SESSION = 9
OP_HELLO = 10
OP_HEARTBEAT_ACK = 11
OP_GATEWAY_ERROR = 12

# Close codes that will never succeed on retry (GatewayConstants.ts).
# 4004 AUTHENTICATION_FAILED, 4010 INVALID_SHARD, 4011 SHARDING_REQUIRED,
# 4012 INVALID_API_VERSION.  Note: Fluxer has no 4013/4014 (no intents).
_FATAL_CLOSE_CODES = {4004, 4010, 4011, 4012}
# Close codes after which the session is dead and we must re-IDENTIFY
# rather than RESUME.
_NO_RESUME_CLOSE_CODES = {4007, 4009}

# Channel types (packages/constants/src/ChannelConstants.ts):
# GUILD_TEXT=0, DM=1, GUILD_VOICE=2, GROUP_DM=3, GUILD_CATEGORY=4,
# GUILD_LINK=998, DM_PERSONAL_NOTES=999.
_CHANNEL_TYPE_MAP = {
    0: "channel",
    1: "dm",
    2: "channel",
    3: "group",
    4: "channel",
    998: "channel",
    999: "dm",
}

# Message types worth relaying (MessageResponseSchema type enum: 0-7, 19).
# 0 = DEFAULT, 19 = REPLY; the rest are system messages (joins, pins, calls).
_RELAYED_MESSAGE_TYPES = {0, 19}

# Reconnect parameters (exponential backoff + jitter).
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

# Minimum spacing between IDENTIFY calls per max_concurrency bucket —
# same 5-second rule as Discord's sharding contract.
_IDENTIFY_PACE_SECONDS = 5.0

_MENTION_RE = re.compile(r"<@!?(\d+)>")


def _mask_token(token: str) -> str:
    """Redact a bot token for log output (never log the raw value)."""
    if not token:
        return "<empty>"
    if len(token) <= 8:
        return "***"
    return f"{token[:4]}…{token[-4:]}"


def _resolve_api_base(config: Optional[PlatformConfig] = None) -> str:
    """Resolve the API base URL: config.extra > env > hosted default."""
    extra = getattr(config, "extra", {}) or {}
    base = (
        str(extra.get("api_base_url") or "").strip()
        or os.getenv("FLUXER_API_BASE_URL", "").strip()
        or DEFAULT_API_BASE_URL
    )
    return base.rstrip("/")


def _resolve_token(config: Optional[PlatformConfig] = None) -> str:
    return (getattr(config, "token", None) or os.getenv("FLUXER_BOT_TOKEN", "")).strip()


def check_fluxer_requirements() -> bool:
    """Return True if the Fluxer adapter runtime dependency is available."""
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("Fluxer: aiohttp not installed")
        return False


def validate_fluxer_config(config: PlatformConfig) -> bool:
    """Return True when Fluxer has enough config to connect."""
    if not _resolve_token(config):
        logger.debug("Fluxer: FLUXER_BOT_TOKEN not set")
        return False
    return True


class FluxerAdapter(BasePlatformAdapter):
    """Gateway adapter for Fluxer (hosted or self-hosted)."""

    splits_long_messages = True  # send() chunks via truncate_message()

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("fluxer"))

        self._token: str = _resolve_token(config)
        self._api_base: str = _resolve_api_base(config)

        self._bot_user_id: str = ""
        self._bot_username: str = ""

        # aiohttp session + websocket handle
        self._session: Any = None  # aiohttp.ClientSession
        self._ws: Any = None       # aiohttp.ClientWebSocketResponse
        self._ws_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._closing = False

        # Gateway session state (for RESUME)
        self._gateway_url: str = ""
        self._session_id: Optional[str] = None
        self._resume_gateway_url: Optional[str] = None
        self._seq: Optional[int] = None
        self._heartbeat_interval: float = 41.25  # seconds; overwritten by HELLO
        self._heartbeat_acked = True
        self._last_identify_ts = 0.0
        self._max_concurrency = 1

        # Dedup cache (gateway may redeliver on resume)
        self._dedup = MessageDeduplicator()

    # ------------------------------------------------------------------
    # REST helpers
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bot {self._token}"}

    async def _api_request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        form: Any = None,
        timeout_s: float = 30.0,
        max_attempts: int = 3,
    ) -> Optional[Dict[str, Any]]:
        """Perform a REST call with Fluxer's Discord-shaped 429 handling.

        Returns the decoded JSON body, ``{}`` for empty 2xx responses, or
        ``None`` on failure.  Honors ``retry_after`` from 429 bodies and
        retries transient 5xx errors.
        """
        import aiohttp

        if ".." in path:
            logger.error("Fluxer API path traversal blocked: %s", path)
            return None
        url = f"{self._api_base}/{path.lstrip('/')}"

        for attempt in range(max_attempts):
            try:
                kwargs: Dict[str, Any] = {
                    "headers": self._headers(),
                    "timeout": aiohttp.ClientTimeout(total=timeout_s),
                }
                if form is not None:
                    kwargs["data"] = form
                elif payload is not None:
                    kwargs["json"] = payload
                async with self._session.request(method, url, **kwargs) as resp:
                    if resp.status == 429:
                        retry_after = await self._retry_after_from_response(resp)
                        if attempt < max_attempts - 1:
                            logger.warning(
                                "Fluxer API %s %s rate limited — retrying in %.1fs",
                                method, path, retry_after,
                            )
                            await asyncio.sleep(retry_after)
                            continue
                        logger.error("Fluxer API %s %s rate limited (gave up)", method, path)
                        return None
                    if resp.status >= 500 and attempt < max_attempts - 1:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error(
                            "Fluxer API %s %s → %s: %s",
                            method, path, resp.status, body[:200],
                        )
                        return None
                    if resp.status == 204 or resp.content_length == 0:
                        return {}
                    try:
                        return await resp.json()
                    except (aiohttp.ContentTypeError, ValueError):
                        return {}
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                logger.error("Fluxer API %s %s network error: %s", method, path, exc)
                return None
        return None

    @staticmethod
    async def _retry_after_from_response(resp: Any) -> float:
        """Extract retry_after seconds from a 429 body/headers (Discord shape)."""
        retry_after = 2.0
        try:
            data = await resp.json()
            if isinstance(data, dict) and data.get("retry_after") is not None:
                retry_after = float(data["retry_after"])
        except Exception:
            header = resp.headers.get("X-RateLimit-Reset-After") or resp.headers.get("Retry-After")
            if header:
                try:
                    retry_after = float(header)
                except ValueError:
                    pass
        # Clamp to something sane so a hostile server can't stall us forever.
        return max(0.1, min(retry_after, 60.0))

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Authenticate, discover the gateway URL, and start the WS listener."""
        import aiohttp

        if not self._token:
            logger.error("Fluxer: FLUXER_BOT_TOKEN not configured")
            self._set_fatal_error(
                "config_missing", "FLUXER_BOT_TOKEN must be set", retryable=False
            )
            return False

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self._closing = False

        # Verify credentials and fetch bot identity.
        me = await self._api_request("GET", "users/@me")
        if not me or "id" not in me:
            logger.error(
                "Fluxer: failed to authenticate against %s — check FLUXER_BOT_TOKEN (%s)",
                self._api_base, _mask_token(self._token),
            )
            await self._session.close()
            self._session = None
            return False
        self._bot_user_id = str(me["id"])
        self._bot_username = me.get("username", "")

        # Gateway discovery: GET /gateway/bot → {url, shards, session_start_limit}
        gw = await self._api_request("GET", "gateway/bot")
        if not gw or not gw.get("url"):
            logger.error("Fluxer: GET /gateway/bot failed — cannot open gateway connection")
            await self._session.close()
            self._session = None
            return False
        self._gateway_url = gw["url"]
        limit = gw.get("session_start_limit") or {}
        self._max_concurrency = max(1, int(limit.get("max_concurrency") or 1))
        remaining = limit.get("remaining")
        if remaining is not None and int(remaining) <= 0:
            reset_after_s = int(limit.get("reset_after") or 0) / 1000.0
            logger.error(
                "Fluxer: session start limit exhausted — resets in %.0fs", reset_after_s
            )
            await self._session.close()
            self._session = None
            self._set_fatal_error(
                "session_limit",
                "Fluxer session start limit exhausted; try again later",
                retryable=True,
            )
            return False

        logger.info(
            "Fluxer: authenticated as @%s (%s) on %s",
            self._bot_username, self._bot_user_id, self._api_base,
        )

        # Start WebSocket gateway loop in the background.
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Close the gateway connection and HTTP session."""
        self._closing = True

        for task in (self._heartbeat_task, self._ws_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._heartbeat_task = None
        self._ws_task = None

        if self._ws and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                pass
        self._ws = None

        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

        self._mark_disconnected()
        logger.info("Fluxer: disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message (or multiple chunks) to a channel."""
        if not content:
            return SendResult(success=True)

        formatted = self.format_message(content)
        chunks = self.truncate_message(formatted, MAX_MESSAGE_LENGTH)

        last_id = None
        for idx, chunk in enumerate(chunks):
            payload = self._message_payload(chunk, reply_to if idx == 0 else None)
            data = await self._api_request(
                "POST", f"channels/{chat_id}/messages", payload=payload
            )
            if not data or "id" not in data:
                err = "Failed to create message"
                return SendResult(
                    success=False,
                    error=err,
                    error_kind=classify_send_error(None, err),
                )
            last_id = str(data["id"])

        return SendResult(success=True, message_id=last_id)

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Trigger the typing indicator (POST /channels/{id}/typing)."""
        await self._api_request(
            "POST", f"channels/{chat_id}/typing", payload=None, max_attempts=1
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return channel name and normalized type."""
        data = await self._api_request("GET", f"channels/{chat_id}")
        if not data:
            return {"name": chat_id, "type": "channel", "chat_id": chat_id}

        ch_type = _CHANNEL_TYPE_MAP.get(data.get("type"), "channel")
        name = data.get("name")
        if not name:
            # DMs/group DMs have no name — derive one from recipients.
            recipients = data.get("recipients") or []
            names = [
                r.get("global_name") or r.get("username", "")
                for r in recipients
                if isinstance(r, dict)
            ]
            name = ", ".join(n for n in names if n) or chat_id
        return {"name": name, "type": ch_type, "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        """Edit an existing message (PATCH /channels/{id}/messages/{id})."""
        formatted = self.format_message(content)
        if len(formatted) > MAX_MESSAGE_LENGTH:
            formatted = formatted[:MAX_MESSAGE_LENGTH]
        data = await self._api_request(
            "PATCH",
            f"channels/{chat_id}/messages/{message_id}",
            payload={"content": formatted},
        )
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to edit message")
        return SendResult(success=True, message_id=str(data["id"]))

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download an image URL and attach it natively."""
        blob = await self._download_url(image_url)
        if blob is None:
            # Fall back to sending the URL as text — Fluxer unfurls embeds.
            return await self.send(
                chat_id, f"{caption or ''}\n{image_url}".strip(), reply_to, metadata
            )
        data, content_type = blob
        fname = image_url.rsplit("/", 1)[-1].split("?")[0] or "image.png"
        return await self._send_files(
            chat_id, [(data, fname, content_type)], caption, reply_to
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption, reply_to)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(
            chat_id, file_path, caption, reply_to, file_name=file_name
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, audio_path, caption, reply_to)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption, reply_to)

    async def send_animation(
        self,
        chat_id: str,
        animation_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """GIFs are ordinary attachments on Fluxer — reuse the image path."""
        if animation_url.startswith("file://") or os.path.exists(animation_url):
            path = animation_url[7:] if animation_url.startswith("file://") else animation_url
            return await self._send_local_file(chat_id, path, caption, reply_to)
        return await self.send_image(chat_id, animation_url, caption, reply_to, metadata)

    def format_message(self, content: str) -> str:
        """Fluxer renders Discord-flavored markdown — pass through.

        Convert image markdown to bare URLs so they unfurl inline instead of
        rendering as broken link syntax.
        """
        return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)

    # ------------------------------------------------------------------
    # Outbound payload / attachment helpers
    # ------------------------------------------------------------------

    def _message_payload(
        self, content: str, reply_to: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build a message-create JSON body with safe mention defaults."""
        payload: Dict[str, Any] = {
            "content": content,
            # Never ping @everyone/roles from agent output; explicit user
            # mentions the agent writes (<@id>) stay inert too — replies are
            # the sanctioned notification path.
            "allowed_mentions": {"parse": []},
        }
        if reply_to:
            payload["message_reference"] = {"message_id": str(reply_to)}
            payload["allowed_mentions"]["replied_user"] = True
        return payload

    async def _download_url(self, url: str) -> Optional[Tuple[bytes, str]]:
        """Download a URL with SSRF protection; returns (bytes, content_type)."""
        from tools.url_safety import is_safe_url

        if not is_safe_url(url):
            logger.warning("Fluxer: blocked unsafe URL (SSRF protection)")
            return None

        import aiohttp

        for attempt in range(3):
            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status >= 500 or resp.status == 429:
                        if attempt < 2:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                    if resp.status >= 400:
                        return None
                    return await resp.read(), resp.content_type or "application/octet-stream"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < 2:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                return None
        return None

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        reply_to: Optional[str],
        file_name: Optional[str] = None,
    ) -> SendResult:
        """Upload a local file as a native attachment."""
        import mimetypes

        p = Path(file_path)
        if not p.exists():
            logger.warning("Fluxer: local file not found, skipping: %s", file_path)
            return SendResult(success=True, message_id=None)

        fname = file_name or p.name
        ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
        return await self._send_files(
            chat_id, [(p.read_bytes(), fname, ct)], caption, reply_to
        )

    async def _send_files(
        self,
        chat_id: str,
        files: List[Tuple[bytes, str, str]],
        caption: Optional[str],
        reply_to: Optional[str],
    ) -> SendResult:
        """POST multipart message-create: ``payload_json`` + ``files[n]`` parts.

        Fluxer's parser (fluxer_api MessageRequestParser.ts) matches each
        ``files[n]`` part to the attachments entry whose ``id`` equals ``n``
        — the same convention as Discord.
        """
        import aiohttp

        payload = self._message_payload(caption or "", reply_to)
        payload["attachments"] = [
            {"id": i, "filename": fname} for i, (_, fname, _) in enumerate(files)
        ]

        form = aiohttp.FormData()
        form.add_field("payload_json", json.dumps(payload), content_type="application/json")
        for i, (data, fname, ct) in enumerate(files):
            form.add_field(f"files[{i}]", data, filename=fname, content_type=ct)

        data = await self._api_request(
            "POST", f"channels/{chat_id}/messages", form=form, timeout_s=120.0
        )
        if not data or "id" not in data:
            return SendResult(success=False, error="Failed to send attachment")
        return SendResult(success=True, message_id=str(data["id"]))

    # ------------------------------------------------------------------
    # WebSocket gateway
    # ------------------------------------------------------------------

    def _ws_url(self) -> str:
        """Gateway URL for the next connection attempt (RESUME-aware)."""
        base = (self._resume_gateway_url or self._gateway_url).rstrip("/")
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}v=1&encoding=json"

    async def _ws_loop(self) -> None:
        """Run gateway sessions forever, reconnecting with backoff + jitter."""
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                clean = await self._ws_connect_and_listen()
                if clean:
                    delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except _FatalGatewayError as exc:
                logger.error("Fluxer gateway fatal error: %s — stopping reconnect", exc)
                self._set_fatal_error("gateway_fatal", str(exc), retryable=False)
                await self._notify_fatal_error()
                return
            except Exception as exc:
                if self._closing:
                    return
                logger.warning(
                    "Fluxer gateway error: %s — reconnecting in %.0fs", exc, delay
                )

            if self._closing:
                return
            jitter = delay * _RECONNECT_JITTER * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _ws_connect_and_listen(self) -> bool:
        """One gateway session: HELLO → IDENTIFY/RESUME → dispatch loop.

        Returns True when the session got as far as READY/RESUMED (so the
        caller resets its backoff), False/raises otherwise.
        """
        import aiohttp

        url = self._ws_url()
        logger.info("Fluxer: connecting to gateway %s", url.split("?")[0])
        established = False

        try:
            self._ws = await self._session.ws_connect(url, max_msg_size=16 * 1024 * 1024)
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in {401, 403}:
                raise _FatalGatewayError(f"gateway handshake rejected (HTTP {exc.status})")
            raise

        try:
            # First frame must be HELLO (op 10) with heartbeat_interval (ms).
            hello = await self._recv_envelope(timeout=30.0)
            if not hello or hello.get("op") != OP_HELLO:
                raise ConnectionError("Fluxer gateway did not send HELLO")
            interval_ms = (hello.get("d") or {}).get("heartbeat_interval") or 41250
            self._heartbeat_interval = float(interval_ms) / 1000.0
            self._heartbeat_acked = True
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            if self._session_id and self._seq is not None:
                await self._ws.send_json({
                    "op": OP_RESUME,
                    "d": {
                        "token": self._token,
                        "session_id": self._session_id,
                        "seq": self._seq,
                    },
                })
            else:
                await self._pace_identify()
                await self._ws.send_json({
                    "op": OP_IDENTIFY,
                    "d": {
                        "token": self._token,
                        "properties": {
                            "os": "linux",
                            "browser": "hermes-agent",
                            "device": "hermes-agent",
                        },
                        # Single-shard bot; GET /gateway/bot said shards=N but
                        # a chat-relay bot only needs shard 0 of 1.
                        "shard": [0, 1],
                    },
                })

            async for raw in self._ws:
                if self._closing:
                    return established
                if raw.type == aiohttp.WSMsgType.TEXT:
                    try:
                        envelope = json.loads(raw.data)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    result = await self._handle_envelope(envelope)
                    if result == "established":
                        established = True
                    elif result == "reconnect":
                        return established
                elif raw.type in {
                    aiohttp.WSMsgType.ERROR,
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.CLOSED,
                }:
                    break

            close_code = self._ws.close_code
            if close_code in _FATAL_CLOSE_CODES:
                raise _FatalGatewayError(f"gateway closed with code {close_code}")
            if close_code in _NO_RESUME_CLOSE_CODES:
                self._session_id = None
                self._seq = None
            logger.info("Fluxer: gateway session ended (close code %s)", close_code)
            return established
        finally:
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            self._heartbeat_task = None
            if self._ws and not self._ws.closed:
                try:
                    await self._ws.close()
                except Exception:
                    pass

    async def _recv_envelope(self, timeout: float) -> Optional[Dict[str, Any]]:
        import aiohttp

        raw = await asyncio.wait_for(self._ws.receive(), timeout=timeout)
        if raw.type != aiohttp.WSMsgType.TEXT:
            return None
        try:
            return json.loads(raw.data)
        except (json.JSONDecodeError, TypeError):
            return None

    async def _pace_identify(self) -> None:
        """Respect session_start_limit.max_concurrency IDENTIFY pacing.

        With a single shard this reduces to: at most one IDENTIFY per
        5s / max_concurrency window, mirroring Discord's sharding rules.
        """
        window = _IDENTIFY_PACE_SECONDS / max(1, self._max_concurrency)
        elapsed = time.monotonic() - self._last_identify_ts
        if elapsed < window:
            await asyncio.sleep(window - elapsed)
        self._last_identify_ts = time.monotonic()

    async def _heartbeat_loop(self) -> None:
        """Send op-1 heartbeats; force a reconnect on missed ACK."""
        try:
            # First beat after interval * jitter, per Discord convention.
            await asyncio.sleep(self._heartbeat_interval * random.random())
            while not self._closing and self._ws and not self._ws.closed:
                if not self._heartbeat_acked:
                    logger.warning("Fluxer: heartbeat ACK missed — recycling gateway connection")
                    await self._ws.close(code=4000)
                    return
                self._heartbeat_acked = False
                await self._ws.send_json({"op": OP_HEARTBEAT, "d": self._seq})
                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Fluxer heartbeat loop ended: %s", exc)

    async def _handle_envelope(self, envelope: Dict[str, Any]) -> Optional[str]:
        """Process one gateway envelope.  Returns a control signal or None."""
        op = envelope.get("op")

        if op == OP_DISPATCH:
            seq = envelope.get("s")
            if seq is not None:
                self._seq = seq
            return await self._handle_dispatch(
                envelope.get("t") or "", envelope.get("d") or {}
            )
        if op == OP_HEARTBEAT:
            # Server requested an immediate beat.
            await self._ws.send_json({"op": OP_HEARTBEAT, "d": self._seq})
            return None
        if op == OP_HEARTBEAT_ACK:
            self._heartbeat_acked = True
            return None
        if op == OP_RECONNECT:
            logger.info("Fluxer: server requested reconnect (RESUME will follow)")
            return "reconnect"
        if op == OP_INVALID_SESSION:
            resumable = bool(envelope.get("d"))
            if not resumable:
                self._session_id = None
                self._seq = None
            logger.warning("Fluxer: invalid session (resumable=%s)", resumable)
            # Brief random wait before the new IDENTIFY, per Discord convention.
            await asyncio.sleep(1.0 + random.random() * 4.0)
            return "reconnect"
        if op == OP_GATEWAY_ERROR:
            logger.warning("Fluxer: gateway error frame: %s", str(envelope.get("d"))[:200])
            return None
        return None

    async def _handle_dispatch(self, event: str, d: Dict[str, Any]) -> Optional[str]:
        if event == "READY":
            self._session_id = d.get("session_id")
            self._resume_gateway_url = d.get("resume_gateway_url") or None
            user = d.get("user") or {}
            if user.get("id"):
                self._bot_user_id = str(user["id"])
                self._bot_username = user.get("username", self._bot_username)
            logger.info(
                "Fluxer: gateway READY (session %s…)",
                str(self._session_id or "")[:8],
            )
            return "established"
        if event == "RESUMED":
            logger.info("Fluxer: gateway session resumed")
            return "established"
        if event == "MESSAGE_CREATE":
            try:
                await self._on_message_create(d)
            except Exception:
                logger.warning("Fluxer: error handling MESSAGE_CREATE", exc_info=True)
            return None
        if event in {"MESSAGE_REACTION_ADD", "MESSAGE_REACTION_REMOVE"}:
            await self._on_reaction(event, d)
            return None
        return None

    # ------------------------------------------------------------------
    # Inbound messages
    # ------------------------------------------------------------------

    async def _on_reaction(self, event: str, d: Dict[str, Any]) -> None:
        """Forward emoji reactions to the gateway's reaction fan-out."""
        if not self._reaction_handler:
            return
        if str(d.get("user_id") or "") == self._bot_user_id:
            return
        emoji = d.get("emoji") or {}
        try:
            await self._reaction_handler({
                "platform": "fluxer",
                "event_name": (
                    "reaction:added" if event == "MESSAGE_REACTION_ADD"
                    else "reaction:removed"
                ),
                "reaction": emoji.get("name") or "",
                "user_id": str(d.get("user_id") or ""),
                "item_user_id": None,
                "channel_id": str(d.get("channel_id") or ""),
                "message_ts": str(d.get("message_id") or ""),
                "event_ts": "",
                "raw_event": d,
            })
        except Exception:
            logger.debug("Fluxer: reaction handler raised", exc_info=True)

    async def _on_message_create(self, d: Dict[str, Any]) -> None:
        """Normalize a MESSAGE_CREATE dispatch into a MessageEvent."""
        author = d.get("author") or {}
        author_id = str(author.get("id") or "")

        # Filter self-messages and other bots/webhooks (reply-loop guard).
        if not author_id or author_id == self._bot_user_id:
            return
        if author.get("bot") or author.get("system") or d.get("webhook_id"):
            return

        # Only relay user-visible content messages (0=DEFAULT, 19=REPLY).
        if d.get("type") not in _RELAYED_MESSAGE_TYPES:
            return

        message_id = str(d.get("id") or "")
        if not message_id or self._dedup.is_duplicate(message_id):
            return

        channel_id = str(d.get("channel_id") or "")
        guild_id = str(d.get("guild_id")) if d.get("guild_id") else None
        text = d.get("content") or ""

        is_dm = guild_id is None
        chat_type = "dm" if is_dm else "channel"

        # Mention gating for guild channels (DMs always respond).
        if not is_dm:
            if not self._channel_allowed(channel_id):
                logger.debug("Fluxer: ignoring message in non-allowed channel %s", channel_id)
                return

            mentioned = self._is_bot_mentioned(text, d)
            if self._require_mention() and not self._is_free_channel(channel_id) and not mentioned:
                return
            if mentioned:
                text = self._strip_bot_mentions(text)

        # Reply context.
        ref = d.get("referenced_message") or {}
        ref_author = (ref.get("author") or {}) if isinstance(ref, dict) else {}
        reply_to_id = str(ref["id"]) if isinstance(ref, dict) and ref.get("id") else None

        # Attachments → local cache for downstream tools.
        media_urls, media_types = await self._cache_attachments(d.get("attachments") or [])

        msg_type = MessageType.TEXT
        if text.startswith("/"):
            msg_type = MessageType.COMMAND
        elif media_types:
            if any(m.startswith("image/") for m in media_types):
                msg_type = MessageType.PHOTO
            elif any(m.startswith("audio/") for m in media_types):
                msg_type = MessageType.VOICE
            else:
                msg_type = MessageType.DOCUMENT

        source = self.build_source(
            chat_id=channel_id,
            chat_type=chat_type,
            user_id=author_id,
            user_name=author.get("global_name") or author.get("username") or author_id,
            guild_id=guild_id,
            message_id=message_id,
        )

        from gateway.platforms.base import resolve_channel_prompt
        channel_prompt = resolve_channel_prompt(self.config.extra, channel_id, None)

        event = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            raw_message=d,
            message_id=message_id,
            media_urls=media_urls or None,
            media_types=media_types or None,
            reply_to_message_id=reply_to_id,
            reply_to_text=(ref.get("content") if isinstance(ref, dict) else None),
            reply_to_author_id=(str(ref_author["id"]) if ref_author.get("id") else None),
            reply_to_author_name=ref_author.get("global_name") or ref_author.get("username"),
            reply_to_is_own_message=(
                str(ref_author.get("id") or "") == self._bot_user_id
            ),
            channel_prompt=channel_prompt,
        )
        await self.handle_message(event)

    # -- inbound helpers -------------------------------------------------

    def _require_mention(self) -> bool:
        return os.getenv("FLUXER_REQUIRE_MENTION", "true").lower() not in {
            "false", "0", "no",
        }

    @staticmethod
    def _csv_env(name: str) -> set:
        return {c.strip() for c in os.getenv(name, "").split(",") if c.strip()}

    def _is_free_channel(self, channel_id: str) -> bool:
        return channel_id in self._csv_env("FLUXER_FREE_RESPONSE_CHANNELS")

    def _channel_allowed(self, channel_id: str) -> bool:
        allowed_raw = (self.config.extra or {}).get("allowed_channels")
        if allowed_raw is None:
            allowed = self._csv_env("FLUXER_ALLOWED_CHANNELS")
        elif isinstance(allowed_raw, list):
            allowed = {str(c).strip() for c in allowed_raw if str(c).strip()}
        else:
            allowed = {c.strip() for c in str(allowed_raw).split(",") if c.strip()}
        return not allowed or channel_id in allowed

    def _is_bot_mentioned(self, text: str, d: Dict[str, Any]) -> bool:
        for m in d.get("mentions") or []:
            if isinstance(m, dict) and str(m.get("id") or "") == self._bot_user_id:
                return True
        return any(uid == self._bot_user_id for uid in _MENTION_RE.findall(text))

    def _strip_bot_mentions(self, text: str) -> str:
        def _sub(match: re.Match) -> str:
            return "" if match.group(1) == self._bot_user_id else match.group(0)
        return _MENTION_RE.sub(_sub, text).strip()

    async def _cache_attachments(
        self, attachments: List[Dict[str, Any]]
    ) -> Tuple[List[str], List[str]]:
        """Download message attachments into the local media caches."""
        media_urls: List[str] = []
        media_types: List[str] = []
        for att in attachments:
            if not isinstance(att, dict):
                continue
            url = att.get("url") or att.get("proxy_url")
            if not url:
                continue
            fname = att.get("filename") or "attachment"
            mime = att.get("content_type") or "application/octet-stream"
            blob = await self._download_url(url)
            if blob is None:
                logger.warning("Fluxer: failed to download attachment %s", fname)
                continue
            data, resp_ct = blob
            mime = mime if mime != "application/octet-stream" else resp_ct
            ext = Path(fname).suffix
            try:
                from gateway.platforms.base import (
                    cache_audio_from_bytes,
                    cache_document_from_bytes,
                    cache_image_from_bytes,
                )
                if mime.startswith("image/"):
                    local = cache_image_from_bytes(data, ext or ".png")
                elif mime.startswith("audio/"):
                    local = cache_audio_from_bytes(data, ext or ".ogg")
                else:
                    local = cache_document_from_bytes(data, fname)
                media_urls.append(local)
                media_types.append(mime)
            except Exception:
                logger.warning("Fluxer: error caching attachment %s", fname, exc_info=True)
        return media_urls, media_types


class _FatalGatewayError(RuntimeError):
    """Gateway condition that will never succeed on retry (bad token, etc.)."""


# ---------------------------------------------------------------------------
# Plugin standalone-send (out-of-process cron delivery via Fluxer REST)
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send via the Fluxer REST API without a live gateway adapter.

    Used by ``tools/send_message_tool._send_via_adapter`` when cron runs in a
    separate process from the gateway.  Opens an ephemeral aiohttp session,
    optionally uploads media as multipart ``files[n]`` parts, posts to
    ``/channels/{chat_id}/messages``, and closes.

    ``thread_id`` is accepted for signature parity; Fluxer has no separate
    thread primitive, so it is treated as a reply target when provided.
    ``force_document`` is accepted but unused — Fluxer stores all uploads as
    generic attachments.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    token = _resolve_token(pconfig)
    api_base = _resolve_api_base(pconfig)
    if not token:
        return {"error": "Fluxer standalone send: FLUXER_BOT_TOKEN must be set"}

    payload: Dict[str, Any] = {
        "content": message[:MAX_MESSAGE_LENGTH],
        "allowed_mentions": {"parse": []},
    }
    if thread_id:
        payload["message_reference"] = {"message_id": str(thread_id)}

    headers = {"Authorization": f"Bot {token}"}
    media_files = media_files or []

    try:
        from gateway.platforms.base import proxy_kwargs_for_aiohttp, resolve_proxy_url
        _proxy = resolve_proxy_url(platform_env_var="FLUXER_PROXY")
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(_proxy)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120), **_sess_kw
        ) as session:
            url = f"{api_base}/channels/{chat_id}/messages"

            existing = [
                m.get("path") if isinstance(m, dict) else m
                for m in media_files
            ]
            existing = [p for p in existing if p and os.path.exists(p)]

            if existing:
                import mimetypes

                payload["attachments"] = [
                    {"id": i, "filename": os.path.basename(p)}
                    for i, p in enumerate(existing)
                ]
                form = aiohttp.FormData()
                form.add_field(
                    "payload_json", json.dumps(payload), content_type="application/json"
                )
                for i, p in enumerate(existing):
                    ct = mimetypes.guess_type(p)[0] or "application/octet-stream"
                    with open(p, "rb") as fh:
                        form.add_field(
                            f"files[{i}]", fh.read(),
                            filename=os.path.basename(p), content_type=ct,
                        )
                req_kwargs: Dict[str, Any] = {"data": form}
            else:
                req_kwargs = {"json": payload}

            async with session.post(url, headers=headers, **req_kwargs, **_req_kw) as resp:
                if resp.status == 429:
                    try:
                        body = await resp.json()
                        retry_after = float(body.get("retry_after") or 2.0)
                    except Exception:
                        retry_after = 2.0
                    await asyncio.sleep(min(retry_after, 30.0))
                    async with session.post(
                        url, headers=headers, **req_kwargs, **_req_kw
                    ) as retry_resp:
                        if retry_resp.status >= 400:
                            body_text = await retry_resp.text()
                            return {
                                "error": (
                                    f"Fluxer API error ({retry_resp.status}): "
                                    f"{body_text[:400]}"
                                )
                            }
                        data = await retry_resp.json()
                elif resp.status >= 400:
                    body_text = await resp.text()
                    return {"error": f"Fluxer API error ({resp.status}): {body_text[:400]}"}
                else:
                    data = await resp.json()

            return {
                "success": True,
                "platform": "fluxer",
                "chat_id": chat_id,
                "message_id": data.get("id"),
            }
    except aiohttp.ClientError as exc:
        return {"error": f"Fluxer send failed (network): {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Fluxer send failed: {exc}"}


# ---------------------------------------------------------------------------
# Env-driven auto-configuration (env_enablement_fn)
# ---------------------------------------------------------------------------


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    Called BEFORE adapter construction so ``hermes gateway status`` and
    ``get_connected_platforms()`` reflect env-only configuration without
    instantiating the adapter.  Returns ``None`` when Fluxer isn't minimally
    configured (no token).
    """
    if not os.getenv("FLUXER_BOT_TOKEN", "").strip():
        return None
    seed: dict = {}
    base = os.getenv("FLUXER_API_BASE_URL", "").strip()
    if base:
        seed["api_base_url"] = base.rstrip("/")
    home = os.getenv("FLUXER_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("FLUXER_HOME_CHANNEL_NAME", home),
        }
    return seed


# ---------------------------------------------------------------------------
# YAML → env config bridge (apply_yaml_config_fn)
# ---------------------------------------------------------------------------


def _apply_yaml_config(yaml_cfg: dict, fluxer_cfg: dict) -> Optional[dict]:
    """Translate ``config.yaml`` ``fluxer:`` keys into env vars.

    Lets self-hosters put ``fluxer.api_base_url`` in config.yaml instead of
    exporting ``FLUXER_API_BASE_URL`` by hand.  Env vars win over YAML —
    every assignment is guarded by ``not os.getenv(...)``.  The base URL is
    also seeded into ``PlatformConfig.extra`` so the adapter and status
    display can read it without another env round-trip.
    """
    seeded: dict = {}
    base = fluxer_cfg.get("api_base_url")
    if base:
        base = str(base).rstrip("/")
        if not os.getenv("FLUXER_API_BASE_URL"):
            os.environ["FLUXER_API_BASE_URL"] = base
        seeded["api_base_url"] = base
    if "require_mention" in fluxer_cfg and not os.getenv("FLUXER_REQUIRE_MENTION"):
        os.environ["FLUXER_REQUIRE_MENTION"] = str(fluxer_cfg["require_mention"]).lower()
    for yaml_key, env_key in (
        ("free_response_channels", "FLUXER_FREE_RESPONSE_CHANNELS"),
        ("allowed_channels", "FLUXER_ALLOWED_CHANNELS"),
    ):
        val = fluxer_cfg.get(yaml_key)
        if val is not None and not os.getenv(env_key):
            if isinstance(val, list):
                val = ",".join(str(v) for v in val)
            os.environ[env_key] = str(val)
    return seeded or None


# ---------------------------------------------------------------------------
# Interactive setup wizard
# ---------------------------------------------------------------------------


def interactive_setup() -> None:
    """Guide the user through Fluxer bot setup (mirrors Mattermost's shape)."""
    from hermes_cli.config import get_env_value, remove_env_value, save_env_value
    from hermes_cli.cli_output import (
        print_header,
        print_info,
        print_success,
        prompt,
        prompt_yes_no,
    )

    print_header("Fluxer")
    existing = get_env_value("FLUXER_BOT_TOKEN")
    if existing:
        print_info("Fluxer: already configured")
        if not prompt_yes_no("Reconfigure Fluxer?", False):
            return

    print_info("Works with the hosted fluxer.app or any self-hosted Fluxer instance.")
    print_info("   1. Create an application/bot in Fluxer's developer settings")
    print_info("   2. Copy the bot token")
    print()
    base_url = prompt(
        "API base URL (empty for hosted https://api.fluxer.app/v1)"
    ).strip()
    if base_url:
        save_env_value("FLUXER_API_BASE_URL", base_url.rstrip("/"))
    elif get_env_value("FLUXER_API_BASE_URL"):
        remove_env_value("FLUXER_API_BASE_URL")

    token = prompt("Fluxer bot token", password=True)
    if not token:
        return
    save_env_value("FLUXER_BOT_TOKEN", token)
    print_success("Fluxer token saved")

    print()
    print_info("🔒 Security: Restrict who can use your bot")
    print_info("   User IDs are snowflakes — right-click a user → Copy ID")
    allowed_users = prompt(
        "Allowed user IDs (comma-separated, leave empty for open access)"
    )
    if allowed_users:
        save_env_value("FLUXER_ALLOWED_USERS", allowed_users.replace(" ", ""))
        print_success("Fluxer allowlist configured")
    else:
        print_info("⚠️  No allowlist set - anyone who can message the bot can use it!")

    print()
    print_info("📬 Home Channel: where Hermes delivers cron job results and notifications.")
    home_channel = prompt("Home channel ID (leave empty to set later with /set-home)").strip()
    if home_channel:
        save_env_value("FLUXER_HOME_CHANNEL", home_channel)
    else:
        if remove_env_value("FLUXER_HOME_CHANNEL"):
            print_info("Home channel cleared.")
    print_info("   Open config in your editor:  hermes config edit")


# ---------------------------------------------------------------------------
# is_connected probe
# ---------------------------------------------------------------------------


def _is_connected(config) -> bool:
    """Fluxer is considered connected when FLUXER_BOT_TOKEN is set.

    Looks up via ``hermes_cli.gateway.get_env_value`` at call time (not via
    the plugin's own bound import) so tests that patch
    ``gateway_mod.get_env_value`` can suppress ambient env vars.
    """
    import hermes_cli.gateway as gateway_mod
    return bool((gateway_mod.get_env_value("FLUXER_BOT_TOKEN") or "").strip())


# ---------------------------------------------------------------------------
# Plugin registration entry point
# ---------------------------------------------------------------------------


def _build_adapter(config):
    """Factory wrapper that constructs FluxerAdapter from a PlatformConfig."""
    return FluxerAdapter(config)


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="fluxer",
        label="Fluxer",
        adapter_factory=_build_adapter,
        check_fn=check_fluxer_requirements,
        validate_config=validate_fluxer_config,
        is_connected=_is_connected,
        required_env=["FLUXER_BOT_TOKEN"],
        install_hint="pip install aiohttp",
        setup_fn=interactive_setup,
        # Env-driven auto-configuration: seeds api_base_url + home_channel
        # so env-only setups show up in gateway status without
        # instantiating the adapter.
        env_enablement_fn=_env_enablement,
        # YAML→env config bridge — owns the translation of ``config.yaml``
        # ``fluxer:`` keys (api_base_url, require_mention,
        # free_response_channels, allowed_channels) into ``FLUXER_*`` env
        # vars that the adapter reads via ``os.getenv()``.
        apply_yaml_config_fn=_apply_yaml_config,
        # Auth env vars for _is_user_authorized() integration.
        allowed_users_env="FLUXER_ALLOWED_USERS",
        allow_all_env="FLUXER_ALLOW_ALL_USERS",
        # Cron home-channel delivery.
        cron_deliver_env_var="FLUXER_HOME_CHANNEL",
        # Out-of-process cron delivery via the Fluxer REST API.  Without
        # this hook, ``deliver=fluxer`` cron jobs fail with "No live
        # adapter" when cron runs separately from the gateway.
        standalone_sender_fn=_standalone_send,
        # Bots/webhooks may send up to 4000 chars (OpenAPI
        # MessageContentRequest).
        max_message_length=MAX_MESSAGE_LENGTH,
        # Display
        emoji="🌀",
        allow_update_command=True,
        # LLM guidance
        platform_hint=(
            "You are on Fluxer, a Discord-like chat platform. Messages render "
            "Discord-flavored markdown (bold, italics, code blocks, links). "
            "Messages are limited to 4000 characters (long replies are "
            "automatically split). You can attach images and files natively."
        ),
    )
