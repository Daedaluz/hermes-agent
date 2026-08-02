"""Fluxer voice-channel / call support: gateway signaling + LiveKit media.

Fluxer's voice stack is *not* Discord's UDP/RTP transport.  Signaling is
Discord-shaped — the client sends gateway opcode 4 (``VOICE_STATE_UPDATE``)
and receives a ``VOICE_SERVER_UPDATE`` dispatch — but the payload carries a
LiveKit endpoint (``wss://…``) plus a per-participant LiveKit JWT instead of
a Discord voice-gateway address.  Media then flows over LiveKit/WebRTC via
the ``livekit`` Python SDK.  Protocol details derived from the Fluxer source
(github.com/fluxerapp/fluxer):

- op 4 request shape: ``{guild_id?, channel_id, self_mute, self_deaf,
  self_video, self_stream, e2ee_capable, bot}``
  (``guild_voice_connection_util.erl`` / ``dm_voice_connect.erl``)
- ``VOICE_SERVER_UPDATE`` dispatch: ``{token, endpoint, channel_id,
  connection_id, e2ee_key?}`` (``dm_voice_token.erl``, ``call_voice.erl``)
- Join tokens expire after 600 s (``VOICE_TOKEN_TTL_SECONDS`` in
  ``LiveKitService.ts``) — reconnects re-run the op 4 handshake for a
  fresh token rather than reusing a stale JWT.
- op 4 is rate-limited to 2/s per session (``gateway_handler_voice.erl``);
  we never send bursts, so no client-side pacing is needed.
- Bots are explicitly modeled: ``check_join_allowed_guild(_, _, Bot=true,
  _) -> ok`` — but a bot join **downgrades an E2EE-active channel**
  (``guild_voice_e2ee.erl: join_downgrades_e2ee``).  We honestly send
  ``bot: true`` / ``e2ee_capable: false`` and document the downgrade.
- DM/group calls ring via ``CALL_CREATE`` dispatches (``call_ringing.erl``,
  ring timeout 30 s); answering is the same op 4 join against the DM
  channel id (no ``guild_id``).

Sessions are keyed by ``int(guild_id)`` for guild voice channels and
``int(dm_channel_id)`` for DM calls — the same integer key the gateway
runner uses for ``_voice_text_channels`` / ``_voice_input_callback``, so
the whole Discord-era voice pipeline (STT → agent → TTS playback) works
unchanged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
import wave
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

OP_VOICE_STATE_UPDATE = 4

# LiveKit publish/playback format (WebRTC native).
SAMPLE_RATE = 48000
CHANNELS = 1
FRAME_MS = 20
FRAME_SAMPLES = SAMPLE_RATE * FRAME_MS // 1000  # 960
FRAME_BYTES = FRAME_SAMPLES * 2  # int16 mono

# Utterance segmentation — same thresholds as the Discord VoiceReceiver so
# the two platforms feel identical to speak to.
SILENCE_THRESHOLD = 1.5     # seconds of silence → end of utterance
MIN_SPEECH_DURATION = 0.5   # minimum seconds of speech to run STT
MAX_UTTERANCE_SECONDS = 45  # hard cap so a hot mic can't grow unbounded
# Peak-amplitude gate (int16) above which a frame counts as speech.
# WebRTC noise suppression upstream keeps the floor low; 500/32767 ≈ -36 dBFS.
SPEECH_AMPLITUDE_THRESHOLD = 500

# How long to wait for the server's VOICE_SERVER_UPDATE after sending op 4.
HANDSHAKE_TIMEOUT = 15.0

# Default inactivity timeout (seconds) before auto-leaving; overridable via
# FLUXER_VOICE_TIMEOUT.  0 disables the timer.
DEFAULT_VOICE_TIMEOUT = 300


def _find_ffmpeg() -> str:
    """ffmpeg discovery: explicit override, repo-wide helper, PATH."""
    explicit = os.getenv("FFMPEG_PATH")
    if explicit and explicit.strip():
        return os.path.expandvars(os.path.expanduser(explicit.strip()))
    try:
        from tools.transcription_tools import _find_ffmpeg_binary
        found = _find_ffmpeg_binary()
        if found:
            return found
    except ImportError:
        pass
    return shutil.which("ffmpeg") or "ffmpeg"


def _require_rtc():
    """Import the LiveKit SDK, lazy-installing it when permitted."""
    try:
        from livekit import rtc  # type: ignore
        return rtc
    except ImportError:
        pass
    try:
        from tools.lazy_deps import ensure
        ensure("platform.fluxer_voice")
        from livekit import rtc  # type: ignore
        return rtc
    except Exception as e:
        raise RuntimeError(
            "Fluxer voice requires the LiveKit SDK. "
            "Install with: pip install 'livekit'"
        ) from e


def peak_amplitude(pcm: bytes) -> int:
    """Return the peak absolute int16 amplitude of little-endian PCM."""
    if len(pcm) < 2:
        return 0
    samples = memoryview(pcm).cast("h")
    # Sample every 4th value — plenty for a speech gate at 48 kHz and keeps
    # the pure-Python loop cheap (no numpy in the base install).
    step = 4 if len(samples) >= 64 else 1
    peak = 0
    for i in range(0, len(samples), step):
        v = samples[i]
        if v < 0:
            v = -v
        if v > peak:
            peak = v
    return peak


def downmix_to_mono(pcm: bytes, num_channels: int) -> bytes:
    """Average interleaved int16 channels down to mono."""
    if num_channels <= 1:
        return pcm
    samples = memoryview(pcm).cast("h")
    frames = len(samples) // num_channels
    out = bytearray(frames * 2)
    out_view = memoryview(out).cast("h")
    for i in range(frames):
        base = i * num_channels
        acc = 0
        for c in range(num_channels):
            acc += samples[base + c]
        out_view[i] = acc // num_channels
    return bytes(out)


def pcm_to_wav(pcm: bytes, wav_path: str, sample_rate: int = SAMPLE_RATE) -> None:
    """Write 16-bit mono PCM out as a WAV file for the STT pipeline."""
    with wave.open(wav_path, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm)


class FluxerVoiceChannel:
    """Duck-typed stand-in for a discord.py VoiceChannel.

    ``gateway/run.py`` only touches ``.name`` (join success message) and the
    adapter's ``join_voice_channel`` reads ``.id`` / ``.guild.id`` — mirror
    exactly that surface.  ``guild.id`` is ``None`` for DM calls.
    """

    def __init__(self, channel_id: str, name: str, guild_id: Optional[str]):
        self.id = str(channel_id)
        self.name = name or f"voice-{channel_id}"
        self.guild = SimpleNamespace(id=guild_id)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<FluxerVoiceChannel id={self.id} name={self.name!r} guild={self.guild.id}>"


@dataclass
class _Capture:
    """Per-participant utterance accumulator."""
    user_id: int
    buffer: bytearray = field(default_factory=bytearray)
    sample_rate: int = SAMPLE_RATE
    last_voice_time: float = 0.0
    speaking: bool = False


@dataclass
class _VoiceSession:
    """One live voice connection (guild channel or DM call)."""
    key: int                      # int(guild_id) or int(dm_channel_id)
    channel_id: str
    guild_id: Optional[str]       # None for DM calls
    channel_name: str
    connection_id: Optional[str] = None
    room: Any = None              # rtc.Room
    audio_source: Any = None      # rtc.AudioSource (48k mono)
    local_track: Any = None
    connected: bool = False
    leaving: bool = False
    captures: Dict[int, _Capture] = field(default_factory=dict)
    stream_tasks: List[asyncio.Task] = field(default_factory=list)
    check_task: Optional[asyncio.Task] = None
    timeout_task: Optional[asyncio.Task] = None
    playback_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_activity: float = field(default_factory=time.monotonic)
    reconnect_attempts: int = 0


class _StreamingHandle:
    """Opaque handle for the streaming-TTS adapter contract."""

    def __init__(
        self, session: _VoiceSession, resampler: Any, channels: int, input_rate: int
    ):
        self.session = session
        self.resampler = resampler   # rtc.AudioResampler or None
        self.channels = channels
        self.input_rate = input_rate
        self.remainder = b""         # sub-sample carry between chunks
        self.closed = False


class FluxerVoiceManager:
    """Owns all voice sessions for one FluxerAdapter.

    The adapter forwards ``VOICE_STATE_UPDATE`` / ``VOICE_SERVER_UPDATE`` /
    ``CALL_CREATE`` dispatches here and delegates the run.py duck-typed
    voice API (join/leave/play/info) to this manager.
    """

    def __init__(self, adapter):
        self._adapter = adapter
        self._sessions: Dict[int, _VoiceSession] = {}
        # channel_id -> Future resolved with the VOICE_SERVER_UPDATE payload
        self._server_update_waiters: Dict[str, asyncio.Future] = {}
        self._join_locks: Dict[int, asyncio.Lock] = {}

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @staticmethod
    def _voice_enabled() -> bool:
        return os.getenv("FLUXER_VOICE_ENABLED", "true").strip().lower() not in {
            "false", "0", "no", "off",
        }

    @staticmethod
    def _auto_answer_enabled() -> bool:
        return os.getenv("FLUXER_AUTO_ANSWER_CALLS", "true").strip().lower() not in {
            "false", "0", "no", "off",
        }

    @staticmethod
    def _voice_timeout_seconds() -> int:
        raw = os.getenv("FLUXER_VOICE_TIMEOUT", "").strip()
        if not raw:
            return DEFAULT_VOICE_TIMEOUT
        try:
            return max(0, int(raw))
        except ValueError:
            return DEFAULT_VOICE_TIMEOUT

    # ------------------------------------------------------------------
    # Signaling (gateway op 4 → VOICE_SERVER_UPDATE)
    # ------------------------------------------------------------------

    async def _send_voice_state(
        self,
        guild_id: Optional[str],
        channel_id: Optional[str],
        connection_id: Optional[str] = None,
    ) -> None:
        """Send gateway op 4.  ``channel_id=None`` disconnects."""
        ws = self._adapter._ws
        if ws is None or getattr(ws, "closed", False):
            raise RuntimeError("Fluxer gateway websocket is not connected")
        d: Dict[str, Any] = {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "self_mute": False,
            "self_deaf": False,
            "self_video": False,
            "self_stream": False,
            # Honest bot identity.  The server allows bot joins outright
            # (guild_voice_e2ee.erl) but flags the E2EE downgrade to other
            # participants; claiming e2ee_capable would break their audio.
            "e2ee_capable": False,
            "bot": True,
        }
        if connection_id:
            d["connection_id"] = connection_id
        await ws.send_json({"op": OP_VOICE_STATE_UPDATE, "d": d})

    def on_voice_server_update(self, d: Dict[str, Any]) -> None:
        """Resolve a pending join handshake with token+endpoint."""
        channel_id = str(d.get("channel_id") or "")
        fut = self._server_update_waiters.get(channel_id)
        if fut is None and len(self._server_update_waiters) == 1:
            # Guild-path VOICE_SERVER_UPDATE payloads may omit channel_id;
            # with exactly one pending join the match is unambiguous.
            fut = next(iter(self._server_update_waiters.values()))
        if fut is not None and not fut.done():
            fut.set_result(d)
            return
        # Unsolicited update (e.g. region migration): reconnect the live
        # session for that channel with the fresh endpoint/token.
        for session in self._sessions.values():
            if session.channel_id == channel_id and session.connected:
                asyncio.ensure_future(self._reconnect_media(session, d))
                return

    def on_call_create(self, d: Dict[str, Any]) -> None:
        """Auto-answer a DM/group call when the bot is being rung."""
        if not (self._voice_enabled() and self._auto_answer_enabled()):
            return
        ringing = {str(u) for u in (d.get("ringing") or [])}
        bot_id = str(getattr(self._adapter, "_bot_user_id", "") or "")
        if not bot_id or bot_id not in ringing:
            return
        channel_id = str(d.get("channel_id") or "")
        if not channel_id:
            return
        asyncio.ensure_future(self._answer_call(channel_id))

    async def _answer_call(self, channel_id: str) -> None:
        try:
            key = int(channel_id)
        except ValueError:
            return
        if key in self._sessions:
            return  # already in this call
        # Only answer calls from allowed users: resolve the DM recipients
        # and require at least one non-bot recipient to pass the allowlist.
        # Raw GET /channels/{id} — the DM payload carries a ``recipients``
        # array of user objects (verified live); the adapter's summarized
        # get_chat_info drops it.  NOTE: GET /channels/{id}/call is
        # ACCESS_DENIED for bot tokens, so eligibility can't be pre-checked.
        try:
            info = await self._adapter._api_request("GET", f"/channels/{channel_id}")
        except Exception:
            info = None
        info = info if isinstance(info, dict) else {}
        if not self._call_participants_allowed(info):
            logger.info(
                "Fluxer: ignoring incoming call in %s (no allowed recipient)",
                channel_id,
            )
            return
        recipients = info.get("recipients") or []
        name = info.get("name") or ", ".join(
            str(r.get("username") or r.get("id") or "?")
            for r in recipients if isinstance(r, dict)
        ) or "call"
        channel = FluxerVoiceChannel(channel_id, name, guild_id=None)
        try:
            ok = await self.join(channel)
        except Exception as e:
            logger.warning("Fluxer: failed to answer call in %s: %s", channel_id, e)
            return
        if ok:
            # Bind the DM channel to itself so transcripts/replies route home.
            self._adapter._voice_text_channels[key] = key
            try:
                source = self._adapter.build_source(
                    chat_id=channel_id,
                    chat_type="dm",
                    user_id=str(self._adapter._bot_user_id or ""),
                    user_name=getattr(self._adapter, "_bot_username", "") or "hermes",
                )
                self._adapter._voice_sources[key] = source.to_dict()
            except Exception:
                pass
            try:
                await self._adapter.send(
                    chat_id=channel_id,
                    content="📞 Joined the call. Talk to me — /voice leave hangs up.",
                )
            except Exception:
                pass

    def _call_participants_allowed(self, info: Any) -> bool:
        """True when any non-bot DM recipient passes the user allowlist."""
        allow_all = os.getenv("FLUXER_ALLOW_ALL_USERS", "").strip().lower() in {
            "true", "1", "yes",
        }
        if allow_all:
            return True
        allowed = {
            u.strip()
            for u in os.getenv("FLUXER_ALLOWED_USERS", "").split(",")
            if u.strip()
        }
        if not allowed:
            return False
        recipients = []
        if isinstance(info, dict):
            recipients = info.get("recipients") or info.get("recipient_ids") or []
        for r in recipients:
            rid = str(r.get("id")) if isinstance(r, dict) else str(r)
            if rid in allowed:
                return True
        return False

    # ------------------------------------------------------------------
    # Join / leave
    # ------------------------------------------------------------------

    async def join(self, channel: FluxerVoiceChannel) -> bool:
        """Full join: op 4 handshake → LiveKit connect → publish + listen."""
        if not self._voice_enabled():
            raise RuntimeError("Fluxer voice is disabled (FLUXER_VOICE_ENABLED=false)")
        rtc = _require_rtc()

        guild_id = channel.guild.id
        key = int(guild_id) if guild_id else int(channel.id)
        lock = self._join_locks.setdefault(key, asyncio.Lock())
        async with lock:
            existing = self._sessions.get(key)
            if existing and existing.connected:
                if existing.channel_id == str(channel.id):
                    self._touch(existing)
                    return True
                await self._teardown_session(existing, send_disconnect=True)

            session = _VoiceSession(
                key=key,
                channel_id=str(channel.id),
                guild_id=str(guild_id) if guild_id else None,
                channel_name=channel.name,
            )
            d = await self._handshake(session)
            await self._connect_media(session, d, rtc)
            self._sessions[key] = session
            self._start_timeout_timer(session)
            logger.info(
                "Fluxer: joined voice channel %s (%s) via %s",
                session.channel_id, session.channel_name,
                str(d.get("endpoint", ""))[:64],
            )
            return True

    async def _handshake(self, session: _VoiceSession) -> Dict[str, Any]:
        """Send op 4 and await the VOICE_SERVER_UPDATE dispatch."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._server_update_waiters[session.channel_id] = fut
        try:
            await self._send_voice_state(
                session.guild_id, session.channel_id, session.connection_id
            )
            d = await asyncio.wait_for(fut, timeout=HANDSHAKE_TIMEOUT)
        except asyncio.TimeoutError:
            raise RuntimeError(
                "Fluxer voice: no VOICE_SERVER_UPDATE from gateway "
                f"within {HANDSHAKE_TIMEOUT:.0f}s"
            )
        finally:
            self._server_update_waiters.pop(session.channel_id, None)
        token = d.get("token")
        endpoint = d.get("endpoint")
        if not token or not endpoint:
            raise RuntimeError(f"Fluxer voice: malformed VOICE_SERVER_UPDATE: {d}")
        session.connection_id = str(d.get("connection_id") or "") or session.connection_id
        return d

    async def _connect_media(self, session: _VoiceSession, d: Dict[str, Any], rtc) -> None:
        """Connect the LiveKit room, publish our track, subscribe to others."""
        endpoint = str(d["endpoint"])
        if not endpoint.startswith(("ws://", "wss://")):
            endpoint = "wss://" + endpoint

        room = rtc.Room()
        manager = self

        def _on_track_subscribed(track, publication, participant):
            try:
                if getattr(track, "kind", None) == rtc.TrackKind.KIND_AUDIO:
                    task = asyncio.ensure_future(
                        manager._consume_audio_track(session, track, participant, rtc)
                    )
                    session.stream_tasks.append(task)
            except Exception:
                logger.debug("Fluxer voice: track_subscribed handler failed", exc_info=True)

        def _on_participant_disconnected(participant):
            uid = manager._participant_user_id(participant)
            if uid is not None:
                capture = session.captures.get(uid)
                if capture:
                    capture.last_voice_time = 0.0  # force finalize on next tick

        def _on_disconnected(*args):
            if not session.leaving:
                asyncio.ensure_future(manager._handle_unexpected_disconnect(session))

        room.on("track_subscribed", _on_track_subscribed)
        room.on("participant_disconnected", _on_participant_disconnected)
        room.on("disconnected", _on_disconnected)

        try:
            options = rtc.RoomOptions(auto_subscribe=True)
        except Exception:
            options = None
        if options is not None:
            await room.connect(endpoint, str(d["token"]), options=options)
        else:  # very old SDK signature
            await room.connect(endpoint, str(d["token"]))

        source = rtc.AudioSource(SAMPLE_RATE, CHANNELS)
        track = rtc.LocalAudioTrack.create_audio_track("hermes-voice", source)
        publish_options = None
        try:
            publish_options = rtc.TrackPublishOptions(
                source=rtc.TrackSource.SOURCE_MICROPHONE
            )
        except Exception:
            pass
        if publish_options is not None:
            await room.local_participant.publish_track(track, publish_options)
        else:
            await room.local_participant.publish_track(track)

        session.room = room
        session.audio_source = source
        session.local_track = track
        session.connected = True
        session.reconnect_attempts = 0
        self._touch(session)

        if session.check_task is None or session.check_task.done():
            session.check_task = asyncio.ensure_future(self._check_silence_loop(session))

    async def _reconnect_media(self, session: _VoiceSession, d: Dict[str, Any]) -> None:
        """Region migration / token refresh: reconnect to the new endpoint."""
        rtc = _require_rtc()
        try:
            await self._close_room(session)
            await self._connect_media(session, d, rtc)
            logger.info("Fluxer: voice session %s migrated to new endpoint", session.key)
        except Exception as e:
            logger.warning("Fluxer: voice migration failed for %s: %s", session.key, e)
            await self._teardown_session(session, send_disconnect=True, notify=True)

    async def _handle_unexpected_disconnect(self, session: _VoiceSession) -> None:
        """Media dropped without us leaving — redo the op 4 handshake.

        Join tokens are only valid for 600 s (LiveKitService.ts), so a
        reconnect must fetch a fresh token instead of reusing the old JWT.
        """
        if session.leaving or not session.connected:
            return
        session.connected = False
        if session.reconnect_attempts >= 3:
            logger.warning("Fluxer: voice session %s gave up reconnecting", session.key)
            await self._teardown_session(session, send_disconnect=True, notify=True)
            return
        session.reconnect_attempts += 1
        delay = 2.0 * session.reconnect_attempts
        logger.info(
            "Fluxer: voice media dropped for %s — reconnecting in %.0fs (attempt %d)",
            session.key, delay, session.reconnect_attempts,
        )
        await asyncio.sleep(delay)
        try:
            rtc = _require_rtc()
            d = await self._handshake(session)
            await self._close_room(session)
            await self._connect_media(session, d, rtc)
        except Exception as e:
            logger.warning("Fluxer: voice reconnect failed for %s: %s", session.key, e)
            await self._handle_unexpected_disconnect(session)

    async def leave(self, key: int) -> None:
        session = self._sessions.get(key)
        if not session:
            return
        await self._teardown_session(session, send_disconnect=True)

    async def shutdown(self) -> None:
        """Disconnect every live voice session (adapter.disconnect path)."""
        for session in list(self._sessions.values()):
            try:
                await self._teardown_session(session, send_disconnect=False)
            except Exception:
                logger.debug("Fluxer: voice shutdown error", exc_info=True)

    async def _teardown_session(
        self, session: _VoiceSession, *, send_disconnect: bool, notify: bool = False
    ) -> None:
        session.leaving = True
        session.connected = False
        self._sessions.pop(session.key, None)

        for task in (session.check_task, session.timeout_task):
            if task and not task.done():
                task.cancel()
        for task in session.stream_tasks:
            if not task.done():
                task.cancel()
        session.stream_tasks.clear()

        await self._close_room(session)

        if send_disconnect:
            try:
                await self._send_voice_state(session.guild_id, None)
            except Exception:
                logger.debug("Fluxer: voice disconnect op4 failed", exc_info=True)

        text_ch = self._adapter._voice_text_channels.pop(session.key, None)
        self._adapter._voice_sources.pop(session.key, None)

        if notify and text_ch:
            on_disconnect = getattr(self._adapter, "_on_voice_disconnect", None)
            if on_disconnect:
                try:
                    on_disconnect(str(text_ch), self._adapter.platform)
                except TypeError:
                    on_disconnect(str(text_ch))
                except Exception:
                    pass
            try:
                await self._adapter.send(
                    chat_id=str(text_ch),
                    content="Left voice channel (inactivity timeout).",
                )
            except Exception:
                pass

    async def _close_room(self, session: _VoiceSession) -> None:
        room, session.room = session.room, None
        session.audio_source = None
        session.local_track = None
        if room is not None:
            try:
                await room.disconnect()
            except Exception:
                logger.debug("Fluxer: room.disconnect failed", exc_info=True)

    # ------------------------------------------------------------------
    # Inbound audio → STT
    # ------------------------------------------------------------------

    def _participant_user_id(self, participant) -> Optional[int]:
        """Map a LiveKit participant to a Fluxer user id.

        The server mints tokens with JSON metadata containing ``user_id``
        (LiveKitService.ts); the identity string is
        ``user_<user_id>_<connection_id>`` (verified against a live
        instance).  Prefer metadata, fall back to the first digit run in
        the identity.
        """
        meta = getattr(participant, "metadata", None)
        if meta:
            try:
                uid = json.loads(meta).get("user_id")
                if uid is not None:
                    return int(uid)
            except (ValueError, TypeError, AttributeError):
                pass
        ident = str(getattr(participant, "identity", "") or "")
        match = re.search(r"\d+", ident)
        return int(match.group(0)) if match else None

    def _user_allowed_for_stt(self, user_id: int) -> bool:
        """Cheap pre-STT allowlist gate (final auth happens in run.py)."""
        allow_all = os.getenv("FLUXER_ALLOW_ALL_USERS", "").strip().lower() in {
            "true", "1", "yes",
        }
        if allow_all:
            return True
        allowed = {
            u.strip()
            for u in os.getenv("FLUXER_ALLOWED_USERS", "").split(",")
            if u.strip()
        }
        return str(user_id) in allowed

    async def _consume_audio_track(self, session, track, participant, rtc) -> None:
        """Feed one remote audio track into the participant's capture buffer."""
        user_id = self._participant_user_id(participant)
        if user_id is None:
            logger.debug(
                "Fluxer voice: cannot resolve user for participant %r — skipping",
                getattr(participant, "identity", "?"),
            )
            return
        bot_id = str(getattr(self._adapter, "_bot_user_id", "") or "")
        if bot_id and str(user_id) == bot_id:
            return
        if not self._user_allowed_for_stt(user_id):
            logger.debug("Fluxer voice: user %s not allowlisted, not capturing", user_id)
            return

        try:
            stream = rtc.AudioStream(track)
        except Exception:
            logger.warning("Fluxer voice: AudioStream open failed", exc_info=True)
            return

        capture = session.captures.setdefault(user_id, _Capture(user_id=user_id))
        try:
            async for event in stream:
                frame = getattr(event, "frame", event)
                data = bytes(frame.data)
                num_channels = int(getattr(frame, "num_channels", 1) or 1)
                capture.sample_rate = int(
                    getattr(frame, "sample_rate", SAMPLE_RATE) or SAMPLE_RATE
                )
                mono = downmix_to_mono(data, num_channels)
                now = time.monotonic()
                if peak_amplitude(mono) >= SPEECH_AMPLITUDE_THRESHOLD:
                    capture.last_voice_time = now
                    capture.speaking = True
                    self._touch(session)
                if capture.speaking:
                    capture.buffer.extend(mono)
                    max_bytes = capture.sample_rate * 2 * MAX_UTTERANCE_SECONDS
                    if len(capture.buffer) > max_bytes:
                        del capture.buffer[:-max_bytes]
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("Fluxer voice: audio stream ended with error", exc_info=True)
        finally:
            try:
                aclose = getattr(stream, "aclose", None)
                if aclose:
                    await aclose()
            except Exception:
                pass

    async def _check_silence_loop(self, session: _VoiceSession) -> None:
        """Poll captures every 200 ms and dispatch completed utterances."""
        try:
            while session.connected or not session.leaving:
                await asyncio.sleep(0.2)
                now = time.monotonic()
                for capture in list(session.captures.values()):
                    if not capture.speaking or not capture.buffer:
                        continue
                    silence = now - capture.last_voice_time
                    if silence < SILENCE_THRESHOLD:
                        continue
                    pcm = bytes(capture.buffer)
                    capture.buffer.clear()
                    capture.speaking = False
                    duration = len(pcm) / 2 / max(capture.sample_rate, 1)
                    if duration < MIN_SPEECH_DURATION:
                        continue
                    asyncio.ensure_future(
                        self._process_utterance(
                            session, capture.user_id, pcm, capture.sample_rate
                        )
                    )
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.error("Fluxer voice: silence loop crashed", exc_info=True)

    async def _process_utterance(
        self, session: _VoiceSession, user_id: int, pcm: bytes, sample_rate: int
    ) -> None:
        """PCM → WAV → STT → runner callback (mirrors Discord's flow)."""
        callback = getattr(self._adapter, "_voice_input_callback", None)
        if callback is None:
            return
        tmp = tempfile.NamedTemporaryFile(
            suffix=".wav", prefix="fluxer_vc_", delete=False
        )
        wav_path = tmp.name
        tmp.close()
        try:
            await asyncio.to_thread(pcm_to_wav, pcm, wav_path, sample_rate)

            from tools.transcription_tools import transcribe_audio
            result = await asyncio.to_thread(transcribe_audio, wav_path)
            if not result.get("success"):
                return
            transcript = (result.get("transcript") or "").strip()
            if not transcript:
                return
            from tools.voice_mode import is_whisper_hallucination
            if is_whisper_hallucination(transcript):
                return

            logger.info("Fluxer voice input from %s: %s", user_id, transcript[:100])
            self._touch(session)
            await callback(
                guild_id=session.key,
                user_id=user_id,
                transcript=transcript,
                platform=self._adapter.platform,
            )
        except Exception:
            logger.warning("Fluxer voice: utterance processing failed", exc_info=True)
        finally:
            try:
                os.unlink(wav_path)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Outbound audio (whole-file playback)
    # ------------------------------------------------------------------

    async def _decode_to_pcm(self, audio_path: str) -> bytes:
        """Decode any audio file to 48 kHz mono s16le PCM via ffmpeg."""
        ffmpeg = _find_ffmpeg()
        proc = await asyncio.create_subprocess_exec(
            ffmpeg, "-v", "error", "-i", audio_path,
            "-f", "s16le", "-acodec", "pcm_s16le",
            "-ar", str(SAMPLE_RATE), "-ac", str(CHANNELS), "pipe:1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        pcm, err = await proc.communicate()
        if proc.returncode != 0 or not pcm:
            raise RuntimeError(
                f"ffmpeg decode failed ({proc.returncode}): "
                f"{(err or b'').decode(errors='replace')[:200]}"
            )
        return pcm

    async def play_file(self, key: int, audio_path: str) -> bool:
        """Play an audio file into the voice channel; waits for playout."""
        session = self._sessions.get(key)
        if not session or not session.connected or session.audio_source is None:
            return False
        rtc = _require_rtc()
        try:
            pcm = await self._decode_to_pcm(audio_path)
        except Exception as e:
            logger.warning("Fluxer voice: decode failed for %s: %s", audio_path, e)
            return False

        async with session.playback_lock:
            if not session.connected or session.audio_source is None:
                return False
            self._touch(session)
            try:
                for off in range(0, len(pcm), FRAME_BYTES):
                    chunk = pcm[off:off + FRAME_BYTES]
                    if len(chunk) < FRAME_BYTES:
                        chunk = chunk + b"\x00" * (FRAME_BYTES - len(chunk))
                    frame = rtc.AudioFrame(
                        data=chunk,
                        sample_rate=SAMPLE_RATE,
                        num_channels=CHANNELS,
                        samples_per_channel=FRAME_SAMPLES,
                    )
                    await session.audio_source.capture_frame(frame)
                waiter = getattr(session.audio_source, "wait_for_playout", None)
                if waiter:
                    await waiter()
            except Exception as e:
                logger.warning("Fluxer voice: playback failed: %s", e)
                return False
            self._touch(session)
            return True

    # ------------------------------------------------------------------
    # Streaming TTS (gateway/platforms/base.py adapter contract, #60671)
    # ------------------------------------------------------------------

    def session_for_chat(self, chat_id: str) -> Optional[_VoiceSession]:
        """Resolve the session whose bound text channel is ``chat_id``."""
        for key, text_ch in self._adapter._voice_text_channels.items():
            if str(text_ch) == str(chat_id):
                session = self._sessions.get(key)
                if session and session.connected:
                    return session
        return None

    async def begin_streaming(self, chat_id: str, audio_format) -> Optional[_StreamingHandle]:
        session = self.session_for_chat(chat_id)
        if session is None or session.audio_source is None:
            return None
        rtc = _require_rtc()
        resampler = None
        in_rate = int(getattr(audio_format, "sample_rate", SAMPLE_RATE))
        channels = int(getattr(audio_format, "channels", 1) or 1)
        if int(getattr(audio_format, "sample_width", 2)) != 2:
            return None  # only int16 PCM is supported
        if in_rate != SAMPLE_RATE:
            try:
                resampler = rtc.AudioResampler(
                    input_rate=in_rate, output_rate=SAMPLE_RATE, num_channels=1
                )
            except Exception:
                logger.debug("Fluxer voice: no AudioResampler; declining stream", exc_info=True)
                return None
        await session.playback_lock.acquire()
        if not session.connected or session.audio_source is None:
            session.playback_lock.release()
            return None
        self._touch(session)
        return _StreamingHandle(session, resampler, channels, in_rate)

    async def write_streaming(self, handle: _StreamingHandle, chunk: bytes) -> None:
        if handle.closed or not chunk:
            return
        session = handle.session
        if not session.connected or session.audio_source is None:
            return
        rtc = _require_rtc()
        data = handle.remainder + chunk
        unit = 2 * handle.channels
        usable = len(data) - (len(data) % unit)
        handle.remainder = data[usable:]
        data = data[:usable]
        if not data:
            return
        mono = downmix_to_mono(data, handle.channels)
        frame = rtc.AudioFrame(
            data=mono,
            sample_rate=handle.input_rate,
            num_channels=1,
            samples_per_channel=len(mono) // 2,
        )
        frames = [frame]
        if handle.resampler is not None:
            frames = list(handle.resampler.push(frame))
        for f in frames:
            await session.audio_source.capture_frame(f)
        self._touch(session)

    async def finish_streaming(self, handle: _StreamingHandle, *, interrupted: bool = False) -> None:
        if handle.closed:
            return
        handle.closed = True
        session = handle.session
        try:
            if not interrupted and session.connected and session.audio_source is not None:
                if handle.resampler is not None:
                    for f in handle.resampler.flush():
                        await session.audio_source.capture_frame(f)
                waiter = getattr(session.audio_source, "wait_for_playout", None)
                if waiter:
                    await waiter()
        except Exception:
            logger.debug("Fluxer voice: finish_streaming flush failed", exc_info=True)
        finally:
            self._touch(session)
            if session.playback_lock.locked():
                session.playback_lock.release()

    async def abort_streaming(self, handle: _StreamingHandle) -> None:
        if handle.closed:
            return
        handle.closed = True
        session = handle.session
        try:
            clear = getattr(session.audio_source, "clear_queue", None)
            if clear:
                clear()
        except Exception:
            pass
        if session.playback_lock.locked():
            session.playback_lock.release()

    # ------------------------------------------------------------------
    # Status / awareness
    # ------------------------------------------------------------------

    def is_connected(self, key: int) -> bool:
        session = self._sessions.get(key)
        return bool(session and session.connected)

    def channel_info(self, key: int) -> Optional[Dict[str, Any]]:
        """Voice-channel awareness dict (same shape as the Discord adapter)."""
        session = self._sessions.get(key)
        if not session or not session.connected or session.room is None:
            return None
        now = time.monotonic()
        members: List[Dict[str, Any]] = []
        speaking = 0
        participants = getattr(session.room, "remote_participants", {}) or {}
        values = participants.values() if isinstance(participants, dict) else participants
        for p in values:
            uid = self._participant_user_id(p)
            if uid is None:
                continue
            capture = session.captures.get(uid)
            is_speaking = bool(
                (capture and now - capture.last_voice_time < 2.0)
                or getattr(p, "is_speaking", False)
            )
            if is_speaking:
                speaking += 1
            members.append({
                "user_id": uid,
                "display_name": getattr(p, "name", "") or str(uid),
                "is_bot": False,
                "is_speaking": is_speaking,
            })
        return {
            "channel_name": session.channel_name,
            "member_count": len(members),
            "members": members,
            "speaking_count": speaking,
        }

    def channel_context(self, key: int) -> str:
        info = self.channel_info(key)
        if not info:
            return ""
        parts = [
            f"[Voice channel: #{info['channel_name']} — "
            f"{info['member_count']} participant(s)]"
        ]
        for m in info["members"]:
            status = " (speaking)" if m["is_speaking"] else ""
            parts.append(f"  - {m['display_name']}{status}")
        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Inactivity timeout
    # ------------------------------------------------------------------

    def _touch(self, session: _VoiceSession) -> None:
        session.last_activity = time.monotonic()

    def _start_timeout_timer(self, session: _VoiceSession) -> None:
        if session.timeout_task and not session.timeout_task.done():
            session.timeout_task.cancel()
        timeout = self._voice_timeout_seconds()
        if timeout <= 0:
            return
        session.timeout_task = asyncio.ensure_future(
            self._timeout_loop(session, timeout)
        )

    async def _timeout_loop(self, session: _VoiceSession, timeout: int) -> None:
        try:
            while not session.leaving:
                await asyncio.sleep(min(30, timeout))
                idle = time.monotonic() - session.last_activity
                if idle < timeout:
                    continue
                # Honor a deliberate text-only session (/voice off) — same
                # rule as the Discord adapter's inactivity timer.
                mode_getter = getattr(self._adapter, "_voice_mode_getter", None)
                text_ch = self._adapter._voice_text_channels.get(session.key)
                if mode_getter is not None and text_ch is not None:
                    try:
                        if mode_getter(str(text_ch)) == "off":
                            self._touch(session)
                            continue
                    except Exception:
                        pass
                logger.info(
                    "Fluxer: voice session %s idle for %.0fs — leaving",
                    session.key, idle,
                )
                await self._teardown_session(
                    session, send_disconnect=True, notify=True
                )
                return
        except asyncio.CancelledError:
            pass
