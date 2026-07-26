"""Tests for the Fluxer platform adapter plugin."""

import asyncio
import json
import logging
import os

import pytest
from unittest.mock import AsyncMock, MagicMock

from gateway.config import Platform, PlatformConfig
from gateway.session import SessionSource
from tests.gateway._plugin_adapter_loader import load_plugin_adapter

# Load plugins/platforms/fluxer/adapter.py under a unique module name
# (plugin_adapter_fluxer) so it cannot collide with other plugin adapters
# loaded by sibling tests in the same xdist worker.
_fluxer_mod = load_plugin_adapter("fluxer")

FluxerAdapter = _fluxer_mod.FluxerAdapter
check_fluxer_requirements = _fluxer_mod.check_fluxer_requirements
validate_fluxer_config = _fluxer_mod.validate_fluxer_config
register = _fluxer_mod.register
_mask_token = _fluxer_mod._mask_token
_resolve_api_base = _fluxer_mod._resolve_api_base
_env_enablement = _fluxer_mod._env_enablement
_apply_yaml_config = _fluxer_mod._apply_yaml_config
DEFAULT_API_BASE_URL = _fluxer_mod.DEFAULT_API_BASE_URL
MAX_MESSAGE_LENGTH = _fluxer_mod.MAX_MESSAGE_LENGTH

TEST_TOKEN = "flx_abcdefghijklmnopqrstuvwxyz123456"


def _make_adapter(extra=None, token=TEST_TOKEN):
    config = PlatformConfig(enabled=True, token=token, extra=extra or {})
    return FluxerAdapter(config)


def _clear_fluxer_env(monkeypatch):
    for var in (
        "FLUXER_BOT_TOKEN",
        "FLUXER_API_BASE_URL",
        "FLUXER_REQUIRE_MENTION",
        "FLUXER_FREE_RESPONSE_CHANNELS",
        "FLUXER_ALLOWED_CHANNELS",
        "FLUXER_HOME_CHANNEL",
        "FLUXER_HOME_CHANNEL_NAME",
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# Helpers / config resolution
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_mask_token_hides_middle(self):
        masked = _mask_token(TEST_TOKEN)
        assert TEST_TOKEN not in masked
        assert TEST_TOKEN[6:20] not in masked
        assert masked.startswith(TEST_TOKEN[:4])

    def test_mask_token_short_and_empty(self):
        assert _mask_token("") == "<empty>"
        assert "short" not in _mask_token("short")

    def test_api_base_default(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        assert _resolve_api_base(None) == DEFAULT_API_BASE_URL

    def test_api_base_env_override(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_API_BASE_URL", "https://fluxer.my.org/v1/")
        assert _resolve_api_base(None) == "https://fluxer.my.org/v1"

    def test_api_base_extra_wins_over_env(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_API_BASE_URL", "https://env.example/v1")
        config = PlatformConfig(extra={"api_base_url": "https://yaml.example/v1"})
        assert _resolve_api_base(config) == "https://yaml.example/v1"

    def test_check_requirements(self):
        # aiohttp is a hard Hermes dependency, so this is always True in CI.
        assert check_fluxer_requirements() is True

    def test_validate_config(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        assert validate_fluxer_config(PlatformConfig(token=TEST_TOKEN)) is True
        assert validate_fluxer_config(PlatformConfig(token=None)) is False

    def test_validate_config_env_fallback(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_BOT_TOKEN", TEST_TOKEN)
        assert validate_fluxer_config(PlatformConfig(token=None)) is True


# ---------------------------------------------------------------------------
# Adapter init
# ---------------------------------------------------------------------------


class TestAdapterInit:
    def test_init_basic(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter()
        assert adapter.platform is Platform("fluxer")
        assert adapter._token == TEST_TOKEN
        assert adapter._api_base == DEFAULT_API_BASE_URL
        assert adapter.is_connected is False

    def test_init_custom_base_url(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter(extra={"api_base_url": "https://fluxer.my.org/v1"})
        assert adapter._api_base == "https://fluxer.my.org/v1"

    def test_init_token_from_env(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_BOT_TOKEN", "env-token")
        adapter = _make_adapter(token=None)
        assert adapter._token == "env-token"

    def test_max_message_length_is_bot_limit(self):
        # Bots/webhooks get 4000 chars (OpenAPI MessageContentRequest).
        assert MAX_MESSAGE_LENGTH == 4000


# ---------------------------------------------------------------------------
# Connect / disconnect lifecycle
# ---------------------------------------------------------------------------


class TestConnectLifecycle:
    @pytest.mark.asyncio
    async def test_connect_success(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter()

        async def fake_api(method, path, **kwargs):
            if path == "users/@me":
                return {"id": "111", "username": "hermes"}
            if path == "gateway/bot":
                return {
                    "url": "wss://gateway.fluxer.app",
                    "shards": 1,
                    "session_start_limit": {
                        "total": 1000, "remaining": 999,
                        "reset_after": 0, "max_concurrency": 1,
                    },
                }
            raise AssertionError(f"unexpected path {path}")

        monkeypatch.setattr(adapter, "_api_request", fake_api)
        monkeypatch.setattr(adapter, "_ws_loop", AsyncMock())

        ok = await adapter.connect()
        assert ok is True
        assert adapter.is_connected is True
        assert adapter._bot_user_id == "111"
        assert adapter._bot_username == "hermes"
        assert adapter._gateway_url == "wss://gateway.fluxer.app"
        assert adapter._max_concurrency == 1

        await adapter.disconnect()
        assert adapter.is_connected is False
        assert adapter._session is None

    @pytest.mark.asyncio
    async def test_connect_fails_without_token(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter(token=None)
        ok = await adapter.connect()
        assert ok is False
        assert adapter.has_fatal_error

    @pytest.mark.asyncio
    async def test_connect_fails_on_bad_auth(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter()
        monkeypatch.setattr(
            adapter, "_api_request", AsyncMock(return_value=None)
        )
        ok = await adapter.connect()
        assert ok is False
        assert adapter.is_connected is False
        assert adapter._session is None  # session cleaned up

    @pytest.mark.asyncio
    async def test_connect_fails_when_session_limit_exhausted(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter()

        async def fake_api(method, path, **kwargs):
            if path == "users/@me":
                return {"id": "111", "username": "hermes"}
            return {
                "url": "wss://gw",
                "shards": 1,
                "session_start_limit": {"remaining": 0, "reset_after": 60000},
            }

        monkeypatch.setattr(adapter, "_api_request", fake_api)
        ok = await adapter.connect()
        assert ok is False
        assert adapter.has_fatal_error


# ---------------------------------------------------------------------------
# Gateway envelope handling
# ---------------------------------------------------------------------------


class TestEnvelopeHandling:
    def setup_method(self):
        self.adapter = _make_adapter()
        self.adapter._ws = MagicMock()
        self.adapter._ws.send_json = AsyncMock()

    @pytest.mark.asyncio
    async def test_dispatch_ready_establishes_session(self):
        result = await self.adapter._handle_envelope({
            "op": 0, "s": 1, "t": "READY",
            "d": {
                "session_id": "sess-1",
                "resume_gateway_url": "wss://resume.fluxer.app",
                "user": {"id": "111", "username": "hermes"},
            },
        })
        assert result == "established"
        assert self.adapter._session_id == "sess-1"
        assert self.adapter._resume_gateway_url == "wss://resume.fluxer.app"
        assert self.adapter._bot_user_id == "111"
        assert self.adapter._seq == 1

    @pytest.mark.asyncio
    async def test_heartbeat_ack_sets_flag(self):
        self.adapter._heartbeat_acked = False
        result = await self.adapter._handle_envelope({"op": 11})
        assert result is None
        assert self.adapter._heartbeat_acked is True

    @pytest.mark.asyncio
    async def test_server_heartbeat_request_answers_immediately(self):
        self.adapter._seq = 42
        await self.adapter._handle_envelope({"op": 1})
        self.adapter._ws.send_json.assert_awaited_once_with({"op": 1, "d": 42})

    @pytest.mark.asyncio
    async def test_reconnect_op_signals_reconnect(self):
        result = await self.adapter._handle_envelope({"op": 7})
        assert result == "reconnect"

    @pytest.mark.asyncio
    async def test_invalid_session_not_resumable_clears_state(self, monkeypatch):
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        self.adapter._session_id = "sess-1"
        self.adapter._seq = 10
        result = await self.adapter._handle_envelope({"op": 9, "d": False})
        assert result == "reconnect"
        assert self.adapter._session_id is None
        assert self.adapter._seq is None

    @pytest.mark.asyncio
    async def test_invalid_session_resumable_keeps_state(self, monkeypatch):
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        self.adapter._session_id = "sess-1"
        self.adapter._seq = 10
        result = await self.adapter._handle_envelope({"op": 9, "d": True})
        assert result == "reconnect"
        assert self.adapter._session_id == "sess-1"
        assert self.adapter._seq == 10

    def test_ws_url_prefers_resume_url(self):
        self.adapter._gateway_url = "wss://gateway.fluxer.app"
        assert self.adapter._ws_url().startswith("wss://gateway.fluxer.app?")
        self.adapter._resume_gateway_url = "wss://resume.fluxer.app"
        assert self.adapter._ws_url().startswith("wss://resume.fluxer.app?")
        assert "encoding=json" in self.adapter._ws_url()


# ---------------------------------------------------------------------------
# Inbound MESSAGE_CREATE → MessageEvent
# ---------------------------------------------------------------------------


def _dm_message(**overrides):
    msg = {
        "id": "9001",
        "channel_id": "555",
        "type": 0,
        "content": "hello agent",
        "author": {"id": "222", "username": "alice", "global_name": "Alice"},
        "attachments": [],
    }
    msg.update(overrides)
    return msg


class TestInboundMessages:
    def setup_method(self):
        self.adapter = _make_adapter()
        self.adapter._bot_user_id = "111"
        self.adapter._bot_username = "hermes"
        self.adapter.handle_message = AsyncMock()

    @pytest.mark.asyncio
    async def test_dm_message_dispatched(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(_dm_message())

        self.adapter.handle_message.assert_awaited_once()
        event = self.adapter.handle_message.await_args.args[0]
        assert event.text == "hello agent"
        assert event.message_id == "9001"
        assert event.source.chat_id == "555"
        assert event.source.chat_type == "dm"
        assert event.source.user_id == "222"
        assert event.source.user_name == "Alice"
        assert event.source.platform is Platform("fluxer")

    @pytest.mark.asyncio
    async def test_own_message_filtered(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(
            _dm_message(author={"id": "111", "username": "hermes"})
        )
        self.adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bot_and_webhook_messages_filtered(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(
            _dm_message(author={"id": "333", "username": "otherbot", "bot": True})
        )
        await self.adapter._on_message_create(_dm_message(webhook_id="777"))
        self.adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_system_message_types_filtered(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(_dm_message(type=6))  # pin notice
        self.adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_message_filtered(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(_dm_message())
        await self.adapter._on_message_create(_dm_message())
        assert self.adapter.handle_message.await_count == 1

    @pytest.mark.asyncio
    async def test_guild_message_without_mention_ignored(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(
            _dm_message(guild_id="42", content="no mention here")
        )
        self.adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_guild_message_with_mention_dispatched_and_stripped(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(
            _dm_message(
                guild_id="42",
                content="<@111> do the thing",
                mentions=[{"id": "111", "username": "hermes"}],
            )
        )
        self.adapter.handle_message.assert_awaited_once()
        event = self.adapter.handle_message.await_args.args[0]
        assert event.text == "do the thing"
        assert event.source.chat_type == "channel"
        assert event.source.guild_id == "42"

    @pytest.mark.asyncio
    async def test_guild_free_response_channel_skips_mention(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_FREE_RESPONSE_CHANNELS", "555")
        await self.adapter._on_message_create(
            _dm_message(guild_id="42", content="no mention needed")
        )
        self.adapter.handle_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_guild_allowed_channels_whitelist(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_ALLOWED_CHANNELS", "999")
        monkeypatch.setenv("FLUXER_REQUIRE_MENTION", "false")
        await self.adapter._on_message_create(
            _dm_message(guild_id="42", content="hi")
        )
        self.adapter.handle_message.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reply_context_mapped(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        await self.adapter._on_message_create(
            _dm_message(
                type=19,
                referenced_message={
                    "id": "8000",
                    "content": "earlier text",
                    "author": {"id": "111", "username": "hermes"},
                },
            )
        )
        event = self.adapter.handle_message.await_args.args[0]
        assert event.reply_to_message_id == "8000"
        assert event.reply_to_text == "earlier text"
        assert event.reply_to_is_own_message is True

    @pytest.mark.asyncio
    async def test_slash_command_type(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        from gateway.platforms.base import MessageType
        await self.adapter._on_message_create(_dm_message(content="/new"))
        event = self.adapter.handle_message.await_args.args[0]
        assert event.message_type == MessageType.COMMAND


# ---------------------------------------------------------------------------
# Outbound send
# ---------------------------------------------------------------------------


class TestSend:
    def setup_method(self):
        self.adapter = _make_adapter()
        self.calls = []

        async def fake_api(method, path, payload=None, form=None, **kwargs):
            self.calls.append((method, path, payload, form))
            return {"id": f"msg-{len(self.calls)}"}

        self.adapter._api_request = fake_api

    @pytest.mark.asyncio
    async def test_send_posts_to_channel_messages(self):
        result = await self.adapter.send("555", "Hello!")
        assert result.success is True
        assert result.message_id == "msg-1"
        method, path, payload, _ = self.calls[0]
        assert method == "POST"
        assert path == "channels/555/messages"
        assert payload["content"] == "Hello!"

    @pytest.mark.asyncio
    async def test_send_disables_mass_mentions(self):
        await self.adapter.send("555", "@everyone hi")
        payload = self.calls[0][2]
        assert payload["allowed_mentions"] == {"parse": []}

    @pytest.mark.asyncio
    async def test_send_reply_reference_on_first_chunk_only(self):
        long_text = "word " * 1500  # > 4000 chars → 2+ chunks
        result = await self.adapter.send("555", long_text, reply_to="8000")
        assert result.success is True
        assert len(self.calls) >= 2
        first_payload = self.calls[0][2]
        assert first_payload["message_reference"] == {"message_id": "8000"}
        for _, _, payload, _ in self.calls[1:]:
            assert "message_reference" not in payload

    @pytest.mark.asyncio
    async def test_send_chunks_within_limit(self):
        long_text = "x" * 9000
        await self.adapter.send("555", long_text)
        assert len(self.calls) >= 3
        for _, _, payload, _ in self.calls:
            assert len(payload["content"]) <= MAX_MESSAGE_LENGTH

    @pytest.mark.asyncio
    async def test_send_empty_is_noop(self):
        result = await self.adapter.send("555", "")
        assert result.success is True
        assert not self.calls

    @pytest.mark.asyncio
    async def test_send_failure_result(self):
        async def failing_api(*args, **kwargs):
            return None

        self.adapter._api_request = failing_api
        result = await self.adapter.send("555", "hello")
        assert result.success is False
        assert result.error

    @pytest.mark.asyncio
    async def test_send_typing(self):
        await self.adapter.send_typing("555")
        method, path, _, _ = self.calls[0]
        assert method == "POST"
        assert path == "channels/555/typing"

    @pytest.mark.asyncio
    async def test_edit_message(self):
        result = await self.adapter.edit_message("555", "9001", "updated")
        assert result.success is True
        method, path, payload, _ = self.calls[0]
        assert method == "PATCH"
        assert path == "channels/555/messages/9001"
        assert payload == {"content": "updated"}


# ---------------------------------------------------------------------------
# Attachments (multipart payload_json + files[n])
# ---------------------------------------------------------------------------


class TestAttachments:
    def setup_method(self):
        self.adapter = _make_adapter()

    @pytest.mark.asyncio
    async def test_send_files_multipart_convention(self, monkeypatch):
        captured = {}

        async def fake_api(method, path, payload=None, form=None, **kwargs):
            captured["method"] = method
            captured["path"] = path
            captured["form"] = form
            return {"id": "msg-77"}

        self.adapter._api_request = fake_api

        # Capture the FormData fields as they are added.
        fields = []
        import aiohttp

        real_add_field = aiohttp.FormData.add_field

        def spy_add_field(self, name, value, **kw):
            fields.append((name, value, kw))
            return real_add_field(self, name, value, **kw)

        monkeypatch.setattr(aiohttp.FormData, "add_field", spy_add_field)

        result = await self.adapter._send_files(
            "555", [(b"png-bytes", "chart.png", "image/png")], "caption", "8000"
        )
        assert result.success is True
        assert result.message_id == "msg-77"
        assert captured["path"] == "channels/555/messages"

        names = [f[0] for f in fields]
        assert "payload_json" in names
        assert "files[0]" in names

        payload = json.loads(next(f[1] for f in fields if f[0] == "payload_json"))
        assert payload["content"] == "caption"
        assert payload["attachments"] == [{"id": 0, "filename": "chart.png"}]
        assert payload["message_reference"] == {"message_id": "8000"}

    @pytest.mark.asyncio
    async def test_send_local_missing_file_skips(self):
        result = await self.adapter._send_local_file(
            "555", "/nonexistent/file.pdf", None, None
        )
        assert result.success is True
        assert result.message_id is None

    @pytest.mark.asyncio
    async def test_send_image_falls_back_to_url_text(self, monkeypatch):
        monkeypatch.setattr(
            self.adapter, "_download_url", AsyncMock(return_value=None)
        )
        sent = {}

        async def fake_send(chat_id, content, reply_to=None, metadata=None):
            sent["content"] = content
            from gateway.platforms.base import SendResult
            return SendResult(success=True, message_id="m1")

        monkeypatch.setattr(self.adapter, "send", fake_send)
        result = await self.adapter.send_image(
            "555", "https://img.example.com/x.png", caption="look"
        )
        assert result.success is True
        assert "https://img.example.com/x.png" in sent["content"]
        assert "look" in sent["content"]


# ---------------------------------------------------------------------------
# get_chat_info
# ---------------------------------------------------------------------------


class TestGetChatInfo:
    def setup_method(self):
        self.adapter = _make_adapter()

    @pytest.mark.asyncio
    async def test_guild_text_channel(self):
        self.adapter._api_request = AsyncMock(
            return_value={"id": "555", "type": 0, "name": "general"}
        )
        info = await self.adapter.get_chat_info("555")
        assert info == {"name": "general", "type": "channel", "chat_id": "555"}

    @pytest.mark.asyncio
    async def test_dm_channel_uses_recipient_names(self):
        self.adapter._api_request = AsyncMock(return_value={
            "id": "556", "type": 1,
            "recipients": [{"username": "alice", "global_name": "Alice"}],
        })
        info = await self.adapter.get_chat_info("556")
        assert info["type"] == "dm"
        assert info["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_group_dm(self):
        self.adapter._api_request = AsyncMock(
            return_value={"id": "557", "type": 3, "name": "the crew"}
        )
        info = await self.adapter.get_chat_info("557")
        assert info["type"] == "group"

    @pytest.mark.asyncio
    async def test_api_failure_falls_back(self):
        self.adapter._api_request = AsyncMock(return_value=None)
        info = await self.adapter.get_chat_info("558")
        assert info == {"name": "558", "type": "channel", "chat_id": "558"}


# ---------------------------------------------------------------------------
# Session source round-trip
# ---------------------------------------------------------------------------


class TestSessionSourceRoundTrip:
    def test_to_dict_from_dict(self):
        adapter = _make_adapter()
        source = adapter.build_source(
            chat_id="555",
            chat_type="channel",
            user_id="222",
            user_name="Alice",
            guild_id="42",
            message_id="9001",
        )
        restored = SessionSource.from_dict(source.to_dict())
        assert restored.platform is Platform("fluxer")
        assert restored.chat_id == "555"
        assert restored.chat_type == "channel"
        assert restored.user_id == "222"
        assert restored.user_name == "Alice"
        assert restored.guild_id == "42"


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


class TestRedaction:
    @pytest.mark.asyncio
    async def test_api_error_logs_never_contain_token(self, caplog, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter()

        import aiohttp

        session = MagicMock()
        session.request = MagicMock(side_effect=aiohttp.ClientError("boom"))
        adapter._session = session

        with caplog.at_level(logging.DEBUG):
            result = await adapter._api_request(
                "GET", "users/@me", max_attempts=1
            )
        assert result is None
        assert TEST_TOKEN not in caplog.text

    @pytest.mark.asyncio
    async def test_connect_failure_log_masks_token(self, caplog, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        adapter = _make_adapter()
        monkeypatch.setattr(adapter, "_api_request", AsyncMock(return_value=None))
        with caplog.at_level(logging.DEBUG):
            ok = await adapter.connect()
        assert ok is False
        assert TEST_TOKEN not in caplog.text


# ---------------------------------------------------------------------------
# Registration + plugin hooks
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_wiring(self):
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args.kwargs

        assert kwargs["name"] == "fluxer"
        assert kwargs["label"] == "Fluxer"
        assert kwargs["required_env"] == ["FLUXER_BOT_TOKEN"]
        assert kwargs["allowed_users_env"] == "FLUXER_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "FLUXER_ALLOW_ALL_USERS"
        assert kwargs["cron_deliver_env_var"] == "FLUXER_HOME_CHANNEL"
        assert kwargs["max_message_length"] == MAX_MESSAGE_LENGTH
        assert callable(kwargs["adapter_factory"])
        assert callable(kwargs["check_fn"])
        assert callable(kwargs["standalone_sender_fn"])
        assert callable(kwargs["env_enablement_fn"])
        assert callable(kwargs["apply_yaml_config_fn"])
        assert callable(kwargs["setup_fn"])
        assert kwargs["platform_hint"]

    def test_adapter_factory_builds_adapter(self):
        ctx = MagicMock()
        register(ctx)
        factory = ctx.register_platform.call_args.kwargs["adapter_factory"]
        adapter = factory(PlatformConfig(enabled=True, token=TEST_TOKEN))
        assert isinstance(adapter, FluxerAdapter)


class TestEnvEnablement:
    def test_none_without_token(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        assert _env_enablement() is None

    def test_seeds_base_url_and_home_channel(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_BOT_TOKEN", TEST_TOKEN)
        monkeypatch.setenv("FLUXER_API_BASE_URL", "https://fluxer.my.org/v1/")
        monkeypatch.setenv("FLUXER_HOME_CHANNEL", "777")
        seed = _env_enablement()
        assert seed["api_base_url"] == "https://fluxer.my.org/v1"
        assert seed["home_channel"] == {"chat_id": "777", "name": "777"}


class TestApplyYamlConfig:
    @pytest.fixture(autouse=True)
    def _restore_env(self):
        # _apply_yaml_config mutates os.environ directly (that's its
        # contract) — snapshot and restore so nothing leaks across tests.
        keys = (
            "FLUXER_API_BASE_URL",
            "FLUXER_REQUIRE_MENTION",
            "FLUXER_FREE_RESPONSE_CHANNELS",
            "FLUXER_ALLOWED_CHANNELS",
        )
        saved = {k: os.environ.get(k) for k in keys}
        yield
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_translates_keys_to_env(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        seeded = _apply_yaml_config({}, {
            "api_base_url": "https://fluxer.my.org/v1/",
            "require_mention": False,
            "free_response_channels": [1, 2],
            "allowed_channels": "3,4",
        })
        assert os.environ["FLUXER_API_BASE_URL"] == "https://fluxer.my.org/v1"
        assert os.environ["FLUXER_REQUIRE_MENTION"] == "false"
        assert os.environ["FLUXER_FREE_RESPONSE_CHANNELS"] == "1,2"
        assert os.environ["FLUXER_ALLOWED_CHANNELS"] == "3,4"
        assert seeded == {"api_base_url": "https://fluxer.my.org/v1"}

    def test_env_wins_over_yaml(self, monkeypatch):
        _clear_fluxer_env(monkeypatch)
        monkeypatch.setenv("FLUXER_REQUIRE_MENTION", "true")
        _apply_yaml_config({}, {"require_mention": False})
        assert os.environ["FLUXER_REQUIRE_MENTION"] == "true"
