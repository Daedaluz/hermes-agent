"""Tests for Fluxer voice support (op 4 signaling + LiveKit media).

The LiveKit SDK is fully mocked — these tests exercise the gateway
signaling handshake, the voice-state cache, the run.py duck-typed contract,
call auto-answer gating, playback framing, and the streaming-TTS surface
without any native dependency.
"""

import asyncio
import os
import wave
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, MagicMock

from gateway.config import Platform, PlatformConfig
from tests.gateway._plugin_adapter_loader import load_plugin_adapter

_fluxer_mod = load_plugin_adapter("fluxer")
FluxerAdapter = _fluxer_mod.FluxerAdapter
voice_mod = _fluxer_mod._voice_module()

TEST_TOKEN = "flx_abcdefghijklmnopqrstuvwxyz123456"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeWS:
    def __init__(self):
        self.closed = False
        self.sent = []

    async def send_json(self, obj):
        self.sent.append(obj)


class FakeAudioSource:
    def __init__(self, sample_rate, num_channels):
        self.sample_rate = sample_rate
        self.num_channels = num_channels
        self.frames = []
        self.cleared = False

    async def capture_frame(self, frame):
        self.frames.append(frame)

    async def wait_for_playout(self):
        pass

    def clear_queue(self):
        self.cleared = True


class FakeAudioFrame:
    def __init__(self, data, sample_rate, num_channels, samples_per_channel):
        self.data = data
        self.sample_rate = sample_rate
        self.num_channels = num_channels
        self.samples_per_channel = samples_per_channel


class FakeResampler:
    def __init__(self, input_rate, output_rate, num_channels=1):
        self.input_rate = input_rate
        self.output_rate = output_rate
        self.pushed = []

    def push(self, frame):
        self.pushed.append(frame)
        return [frame]

    def flush(self):
        return []


class FakeLocalParticipant:
    def __init__(self):
        self.published = []

    async def publish_track(self, track, options=None):
        self.published.append((track, options))


class FakeRoom:
    def __init__(self):
        self.handlers = {}
        self.connect_args = None
        self.disconnected = False
        self.local_participant = FakeLocalParticipant()
        self.remote_participants = {}

    def on(self, event, cb):
        self.handlers[event] = cb

    async def connect(self, url, token, options=None):
        self.connect_args = (url, token)

    async def disconnect(self):
        self.disconnected = True


def make_fake_rtc():
    rooms = []

    def _room():
        room = FakeRoom()
        rooms.append(room)
        return room

    rtc = SimpleNamespace(
        Room=_room,
        AudioSource=FakeAudioSource,
        AudioFrame=FakeAudioFrame,
        AudioResampler=FakeResampler,
        LocalAudioTrack=SimpleNamespace(
            create_audio_track=lambda name, source: SimpleNamespace(
                name=name, source=source
            )
        ),
        TrackPublishOptions=lambda **kw: SimpleNamespace(**kw),
        TrackSource=SimpleNamespace(SOURCE_MICROPHONE="mic"),
        TrackKind=SimpleNamespace(KIND_AUDIO="audio"),
        RoomOptions=lambda **kw: SimpleNamespace(**kw),
        AudioStream=MagicMock(),
    )
    rtc._rooms = rooms
    return rtc


def _make_adapter(monkeypatch=None):
    config = PlatformConfig(enabled=True, token=TEST_TOKEN, extra={})
    adapter = FluxerAdapter(config)
    adapter._bot_user_id = "999"
    adapter._ws = FakeWS()
    return adapter


async def _join(adapter, guild_id="11", channel_id="22", name="General",
                server_update=None):
    """Run a join, resolving the VOICE_SERVER_UPDATE handshake."""
    channel = voice_mod.FluxerVoiceChannel(channel_id, name, guild_id)
    task = asyncio.ensure_future(adapter._voice.join(channel))
    # Wait for op4 + waiter registration.
    for _ in range(100):
        await asyncio.sleep(0.01)
        if channel_id in adapter._voice._server_update_waiters:
            break
    d = server_update or {
        "channel_id": channel_id,
        "connection_id": "conn-1",
        "token": "jwt-token",
        "endpoint": "wss://lk.example.org",
    }
    adapter._voice.on_voice_server_update(d)
    return await task


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestAudioHelpers:
    def test_peak_amplitude_silence(self):
        assert voice_mod.peak_amplitude(b"\x00\x00" * 480) == 0

    def test_peak_amplitude_signal(self):
        import struct
        pcm = struct.pack("<4h", 100, -20000, 5, 0)
        assert voice_mod.peak_amplitude(pcm) == 20000

    def test_downmix_stereo(self):
        import struct
        stereo = struct.pack("<4h", 100, 200, -100, -200)
        mono = voice_mod.downmix_to_mono(stereo, 2)
        assert struct.unpack("<2h", mono) == (150, -150)

    def test_downmix_mono_passthrough(self):
        pcm = b"\x01\x02\x03\x04"
        assert voice_mod.downmix_to_mono(pcm, 1) is pcm

    def test_pcm_to_wav(self, tmp_path):
        path = str(tmp_path / "out.wav")
        voice_mod.pcm_to_wav(b"\x00\x01" * 4800, path, sample_rate=48000)
        with wave.open(path, "rb") as wf:
            assert wf.getnchannels() == 1
            assert wf.getsampwidth() == 2
            assert wf.getframerate() == 48000
            assert wf.getnframes() == 4800


# ---------------------------------------------------------------------------
# Voice-state cache
# ---------------------------------------------------------------------------


class TestVoiceStateCache:
    def test_update_and_clear(self):
        adapter = _make_adapter()
        adapter._on_voice_state_update(
            {"guild_id": "11", "user_id": "42", "channel_id": "22"}
        )
        assert adapter._voice_states["11"]["42"] == "22"
        adapter._on_voice_state_update(
            {"guild_id": "11", "user_id": "42", "channel_id": None}
        )
        assert "42" not in adapter._voice_states["11"]

    def test_guild_create_seeds_states_and_names(self):
        adapter = _make_adapter()
        adapter._ingest_guild_voice_states({
            "id": "11",
            "voice_states": [
                {"user_id": "42", "channel_id": "22"},
                {"user_id": "43", "channel_id": None},
            ],
            "channels": [{"id": "22", "name": "General", "type": 2}],
        })
        assert adapter._voice_states["11"] == {"42": "22"}
        assert adapter._channel_names["22"] == "General"

    @pytest.mark.asyncio
    async def test_get_user_voice_channel(self):
        adapter = _make_adapter()
        adapter._on_voice_state_update(
            {"guild_id": "11", "user_id": "42", "channel_id": "22"}
        )
        adapter._channel_names["22"] = "General"
        channel = await adapter.get_user_voice_channel(11, "42")
        assert channel.id == "22"
        assert channel.name == "General"
        assert channel.guild.id == "11"

    @pytest.mark.asyncio
    async def test_get_user_voice_channel_absent(self):
        adapter = _make_adapter()
        assert await adapter.get_user_voice_channel(11, "42") is None


# ---------------------------------------------------------------------------
# Join / leave signaling
# ---------------------------------------------------------------------------


class TestJoinLeave:
    @pytest.mark.asyncio
    async def test_join_sends_op4_and_connects(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()

        ok = await _join(adapter)
        assert ok is True

        op4 = adapter._ws.sent[0]
        assert op4["op"] == voice_mod.OP_VOICE_STATE_UPDATE
        d = op4["d"]
        assert d["guild_id"] == "11"
        assert d["channel_id"] == "22"
        assert d["bot"] is True
        assert d["e2ee_capable"] is False
        assert d["self_mute"] is False and d["self_deaf"] is False

        room = fake_rtc._rooms[0]
        assert room.connect_args == ("wss://lk.example.org", "jwt-token")
        assert len(room.local_participant.published) == 1
        assert adapter.is_in_voice_channel(11)

    @pytest.mark.asyncio
    async def test_join_prepends_wss_scheme(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        await _join(adapter, server_update={
            "channel_id": "22", "token": "t", "endpoint": "lk.example.org",
        })
        assert fake_rtc._rooms[0].connect_args[0] == "wss://lk.example.org"

    @pytest.mark.asyncio
    async def test_join_handshake_timeout(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        monkeypatch.setattr(voice_mod, "HANDSHAKE_TIMEOUT", 0.05)
        adapter = _make_adapter()
        channel = voice_mod.FluxerVoiceChannel("22", "General", "11")
        with pytest.raises(RuntimeError, match="VOICE_SERVER_UPDATE"):
            await adapter._voice.join(channel)
        assert not adapter.is_in_voice_channel(11)
        # Waiter must not leak after the failed handshake.
        assert "22" not in adapter._voice._server_update_waiters

    @pytest.mark.asyncio
    async def test_join_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("FLUXER_VOICE_ENABLED", "false")
        adapter = _make_adapter()
        channel = voice_mod.FluxerVoiceChannel("22", "General", "11")
        with pytest.raises(RuntimeError, match="disabled"):
            await adapter._voice.join(channel)

    @pytest.mark.asyncio
    async def test_leave_sends_disconnect_and_cleans_bindings(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        await _join(adapter)
        adapter._voice_text_channels[11] = 33
        adapter._voice_sources[11] = {"chat_id": "33"}

        await adapter.leave_voice_channel(11)

        assert not adapter.is_in_voice_channel(11)
        assert fake_rtc._rooms[0].disconnected
        # Disconnect op4: channel_id null
        op4s = [m for m in adapter._ws.sent if m["op"] == voice_mod.OP_VOICE_STATE_UPDATE]
        assert op4s[-1]["d"]["channel_id"] is None
        assert 11 not in adapter._voice_text_channels
        assert 11 not in adapter._voice_sources

    @pytest.mark.asyncio
    async def test_join_voice_channel_binds_text_channel(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        channel = voice_mod.FluxerVoiceChannel("22", "General", "11")

        task = asyncio.ensure_future(
            adapter.join_voice_channel(
                channel, text_channel_id=33, source={"chat_id": "33"}
            )
        )
        for _ in range(100):
            await asyncio.sleep(0.01)
            if "22" in adapter._voice._server_update_waiters:
                break
        adapter._voice.on_voice_server_update(
            {"channel_id": "22", "token": "t", "endpoint": "wss://x"}
        )
        assert await task is True
        assert adapter._voice_text_channels[11] == 33
        assert adapter._voice_sources[11] == {"chat_id": "33"}


# ---------------------------------------------------------------------------
# Dispatch routing
# ---------------------------------------------------------------------------


class TestDispatchRouting:
    @pytest.mark.asyncio
    async def test_voice_events_routed(self, monkeypatch):
        adapter = _make_adapter()
        adapter._voice.on_voice_server_update = MagicMock()
        adapter._voice.on_call_create = MagicMock()

        await adapter._handle_dispatch("VOICE_SERVER_UPDATE", {"token": "t"})
        adapter._voice.on_voice_server_update.assert_called_once()

        await adapter._handle_dispatch("CALL_CREATE", {"channel_id": "5"})
        adapter._voice.on_call_create.assert_called_once()

        await adapter._handle_dispatch(
            "VOICE_STATE_UPDATE",
            {"guild_id": "11", "user_id": "42", "channel_id": "22"},
        )
        assert adapter._voice_states["11"]["42"] == "22"

        await adapter._handle_dispatch(
            "GUILD_CREATE",
            {"id": "12", "voice_states": [{"user_id": "7", "channel_id": "8"}]},
        )
        assert adapter._voice_states["12"]["7"] == "8"

    @pytest.mark.asyncio
    async def test_ready_ingests_guild_voice_states(self):
        adapter = _make_adapter()
        result = await adapter._handle_dispatch("READY", {
            "session_id": "s1",
            "user": {"id": "999", "username": "hermes"},
            "guilds": [
                {"id": "11", "voice_states": [{"user_id": "42", "channel_id": "22"}]},
            ],
        })
        assert result == "established"
        assert adapter._voice_states["11"]["42"] == "22"


# ---------------------------------------------------------------------------
# DM call auto-answer
# ---------------------------------------------------------------------------


class TestCallAutoAnswer:
    def _rung_payload(self, channel_id="55", ringing=("999",)):
        return {"channel_id": channel_id, "ringing": list(ringing),
                "message_id": "1", "region": None}

    @pytest.mark.asyncio
    async def test_answers_when_rung_by_allowed_user(self, monkeypatch):
        monkeypatch.setenv("FLUXER_ALLOWED_USERS", "42")
        monkeypatch.delenv("FLUXER_ALLOW_ALL_USERS", raising=False)
        adapter = _make_adapter()
        # Raw GET /channels/{id} shape verified against a live instance:
        # DM channels carry ``recipients`` (user objects) and no ``name``.
        adapter._api_request = AsyncMock(return_value={
            "id": "55", "type": 1,
            "recipients": [{"id": "42", "username": "tux"}],
        })
        adapter.send = AsyncMock()
        adapter._voice.join = AsyncMock(return_value=True)

        await adapter._voice._answer_call("55")

        adapter._voice.join.assert_awaited_once()
        joined_channel = adapter._voice.join.await_args.args[0]
        assert joined_channel.id == "55"
        assert joined_channel.guild.id is None  # DM call: no guild
        assert adapter._voice_text_channels[55] == 55

    @pytest.mark.asyncio
    async def test_ignores_call_without_allowed_recipient(self, monkeypatch):
        monkeypatch.setenv("FLUXER_ALLOWED_USERS", "42")
        monkeypatch.delenv("FLUXER_ALLOW_ALL_USERS", raising=False)
        adapter = _make_adapter()
        adapter._api_request = AsyncMock(return_value={
            "id": "55", "type": 1,
            "recipients": [{"id": "666", "username": "stranger"}],
        })
        adapter._voice.join = AsyncMock(return_value=True)

        await adapter._voice._answer_call("55")

        adapter._voice.join.assert_not_awaited()

    def test_not_rung_means_no_answer(self, monkeypatch):
        adapter = _make_adapter()
        adapter._voice._answer_call = MagicMock()
        # Bot id 999 not in ringing list.
        adapter._voice.on_call_create(self._rung_payload(ringing=("42",)))
        adapter._voice._answer_call.assert_not_called()

    def test_auto_answer_disabled(self, monkeypatch):
        monkeypatch.setenv("FLUXER_AUTO_ANSWER_CALLS", "false")
        adapter = _make_adapter()
        adapter._voice._answer_call = MagicMock()
        adapter._voice.on_call_create(self._rung_payload())
        adapter._voice._answer_call.assert_not_called()


# ---------------------------------------------------------------------------
# Playback
# ---------------------------------------------------------------------------


class TestPlayback:
    @pytest.mark.asyncio
    async def test_play_file_frames_and_pads(self, monkeypatch, tmp_path):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        await _join(adapter)

        # 1.5 frames of PCM → two frames, second zero-padded.
        pcm = b"\x01\x00" * voice_mod.FRAME_SAMPLES + b"\x01\x00" * (
            voice_mod.FRAME_SAMPLES // 2
        )
        monkeypatch.setattr(
            adapter._voice, "_decode_to_pcm", AsyncMock(return_value=pcm)
        )
        ok = await adapter.play_in_voice_channel(11, str(tmp_path / "x.mp3"))
        assert ok is True

        source = adapter._voice._sessions[11].audio_source
        assert len(source.frames) == 2
        assert all(len(f.data) == voice_mod.FRAME_BYTES for f in source.frames)
        tail = source.frames[1].data[-2:]
        assert tail == b"\x00\x00"

    @pytest.mark.asyncio
    async def test_play_file_not_connected(self):
        adapter = _make_adapter()
        assert await adapter.play_in_voice_channel(11, "/tmp/x.mp3") is False


# ---------------------------------------------------------------------------
# Streaming TTS contract
# ---------------------------------------------------------------------------


class TestStreamingTTS:
    @pytest.mark.asyncio
    async def test_full_stream_cycle_with_resample(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        await _join(adapter)
        adapter._voice_text_channels[11] = 33

        fmt = SimpleNamespace(sample_rate=24000, channels=1, sample_width=2)
        assert adapter.supports_streaming_tts("33", fmt) is True
        assert adapter.supports_streaming_tts("99", fmt) is False

        handle = await adapter.begin_streaming_tts("33", fmt)
        assert handle is not None
        assert handle.resampler.input_rate == 24000

        # Odd-length chunk: last byte carried in the remainder.
        await adapter.write_streaming_tts(handle, b"\x01\x00\x02\x00\x03")
        assert handle.remainder == b"\x03"
        source = adapter._voice._sessions[11].audio_source
        assert len(source.frames) == 1
        assert source.frames[0].sample_rate == 24000  # resampler input

        await adapter.finish_streaming_tts(handle)
        assert handle.closed is True
        # Lock released → a second stream can begin.
        handle2 = await adapter.begin_streaming_tts("33", fmt)
        assert handle2 is not None
        await adapter.abort_streaming_tts(handle2)
        assert handle2.closed is True

    @pytest.mark.asyncio
    async def test_declines_non_int16(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        await _join(adapter)
        adapter._voice_text_channels[11] = 33
        fmt = SimpleNamespace(sample_rate=24000, channels=1, sample_width=4)
        assert adapter.supports_streaming_tts("33", fmt) is False


# ---------------------------------------------------------------------------
# Teardown / timeout notify
# ---------------------------------------------------------------------------


class TestTeardownNotify:
    @pytest.mark.asyncio
    async def test_notify_calls_on_voice_disconnect_with_platform(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        adapter.send = AsyncMock()
        await _join(adapter)
        adapter._voice_text_channels[11] = 33

        calls = []
        adapter._on_voice_disconnect = lambda chat_id, platform=None: calls.append(
            (chat_id, platform)
        )
        session = adapter._voice._sessions[11]
        await adapter._voice._teardown_session(
            session, send_disconnect=True, notify=True
        )
        assert calls == [("33", Platform("fluxer"))]
        adapter.send.assert_awaited()  # timeout notice to the text channel


# ---------------------------------------------------------------------------
# Utterance processing
# ---------------------------------------------------------------------------


class TestUtteranceProcessing:
    @pytest.mark.asyncio
    async def test_utterance_reaches_callback_with_platform(self, monkeypatch):
        adapter = _make_adapter()
        session = voice_mod._VoiceSession(
            key=11, channel_id="22", guild_id="11", channel_name="General"
        )

        import tools.transcription_tools as tt
        import tools.voice_mode as vm
        monkeypatch.setattr(
            tt, "transcribe_audio",
            lambda path: {"success": True, "transcript": "hello there"},
        )
        monkeypatch.setattr(vm, "is_whisper_hallucination", lambda t: False)

        received = []

        async def callback(**kwargs):
            received.append(kwargs)

        adapter._voice_input_callback = callback
        await adapter._voice._process_utterance(session, 42, b"\x00\x01" * 48000, 48000)

        assert len(received) == 1
        assert received[0]["guild_id"] == 11
        assert received[0]["user_id"] == 42
        assert received[0]["transcript"] == "hello there"
        assert received[0]["platform"] == Platform("fluxer")

    @pytest.mark.asyncio
    async def test_hallucination_filtered(self, monkeypatch):
        adapter = _make_adapter()
        session = voice_mod._VoiceSession(
            key=11, channel_id="22", guild_id="11", channel_name="General"
        )
        import tools.transcription_tools as tt
        import tools.voice_mode as vm
        monkeypatch.setattr(
            tt, "transcribe_audio",
            lambda path: {"success": True, "transcript": "Thanks for watching!"},
        )
        monkeypatch.setattr(vm, "is_whisper_hallucination", lambda t: True)
        adapter._voice_input_callback = AsyncMock()
        await adapter._voice._process_utterance(session, 42, b"\x00\x01" * 48000, 48000)
        adapter._voice_input_callback.assert_not_awaited()


# ---------------------------------------------------------------------------
# STT allowlist gate
# ---------------------------------------------------------------------------


class TestSttAllowlist:
    def test_allowed_user(self, monkeypatch):
        monkeypatch.setenv("FLUXER_ALLOWED_USERS", "42, 43")
        monkeypatch.delenv("FLUXER_ALLOW_ALL_USERS", raising=False)
        adapter = _make_adapter()
        assert adapter._voice._user_allowed_for_stt(42) is True
        assert adapter._voice._user_allowed_for_stt(666) is False

    def test_allow_all(self, monkeypatch):
        monkeypatch.setenv("FLUXER_ALLOW_ALL_USERS", "true")
        adapter = _make_adapter()
        assert adapter._voice._user_allowed_for_stt(666) is True

    def test_participant_user_id_from_metadata(self):
        adapter = _make_adapter()
        p = SimpleNamespace(metadata='{"user_id": "42"}', identity="42:conn-1")
        assert adapter._voice._participant_user_id(p) == 42

    def test_participant_user_id_from_identity(self):
        # Live-verified identity format: user_<user_id>_<connection_id>
        adapter = _make_adapter()
        p = SimpleNamespace(metadata="", identity="user_42_angora-walleye")
        assert adapter._voice._participant_user_id(p) == 42

    def test_participant_user_id_unresolvable(self):
        adapter = _make_adapter()
        p = SimpleNamespace(metadata="", identity="agent-x")
        assert adapter._voice._participant_user_id(p) is None


# ---------------------------------------------------------------------------
# Voice-channel awareness
# ---------------------------------------------------------------------------


class TestChannelAwareness:
    @pytest.mark.asyncio
    async def test_info_and_context(self, monkeypatch):
        fake_rtc = make_fake_rtc()
        monkeypatch.setattr(voice_mod, "_require_rtc", lambda: fake_rtc)
        adapter = _make_adapter()
        await _join(adapter)

        room = fake_rtc._rooms[0]
        room.remote_participants = {
            "sid1": SimpleNamespace(
                metadata='{"user_id": "42"}', identity="42:c", name="Alice",
                is_speaking=False,
            ),
        }
        info = adapter.get_voice_channel_info(11)
        assert info["channel_name"] == "General"
        assert info["member_count"] == 1
        assert info["members"][0]["display_name"] == "Alice"

        ctx = adapter.get_voice_channel_context(11)
        assert "#General" in ctx and "Alice" in ctx

    def test_info_when_not_connected(self):
        adapter = _make_adapter()
        assert adapter.get_voice_channel_info(11) is None
        assert adapter.get_voice_channel_context(11) == ""


# ---------------------------------------------------------------------------
# run.py generalization
# ---------------------------------------------------------------------------


class TestRunPyGeneralization:
    def test_get_guild_id_from_dict_raw_message(self):
        from gateway.run import GatewayRunner
        event = SimpleNamespace(raw_message={"guild_id": "11", "channel_id": "22"})
        assert GatewayRunner._get_guild_id(event) == 11

    def test_get_guild_id_dict_without_guild(self):
        from gateway.run import GatewayRunner
        event = SimpleNamespace(raw_message={"channel_id": "22"})
        assert GatewayRunner._get_guild_id(event) is None

    def test_get_guild_id_namespace_still_works(self):
        from gateway.run import GatewayRunner
        event = SimpleNamespace(raw_message=SimpleNamespace(guild_id=11, guild=None))
        assert GatewayRunner._get_guild_id(event) == 11
