"""Read-only checks against an available local host checkout, without starting it."""
import importlib.util
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_mcbot_bridge_plugin import _fresh, _make_service


def host_module(relative, name):
    root = Path(os.environ.get("NEKO_HOST_SOURCE", Path(__file__).resolve().parents[2] / "N.E.K.O"))
    source = root / relative
    if not source.is_file():
        pytest.skip("Set NEKO_HOST_SOURCE to check the real host SDK")
    spec = importlib.util.spec_from_file_location(name, source)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_actual_host_tool_metadata_exposes_controls_without_a_second_task_dispatcher(plugin_module, monkeypatch):
    sdk = host_module("plugin/sdk/plugin/llm_tool.py", "_mcbot_host_tools")
    tool_names = ("ask", "answer", "cancel")
    for suffix in tool_names:
        method = getattr(plugin_module.McbotBridgePlugin, "mcbot_" + suffix)
        monkeypatch.setattr(method, sdk.LLM_TOOL_META_ATTR, None, raising=False)
        sdk.llm_tool(
            name="mcbot_" + suffix,
            description=getattr(plugin_module, "MCBOT_" + suffix.upper() + "_DESCRIPTION"),
            parameters=getattr(plugin_module, "MCBOT_" + suffix.upper() + "_SCHEMA"),
            timeout=30,
        )(method)
    instance = plugin_module.McbotBridgePlugin.__new__(plugin_module.McbotBridgePlugin)
    tools = sdk.collect_llm_tool_methods(instance)
    assert {meta.name for meta, _ in tools} == {"mcbot_" + suffix for suffix in tool_names}
    assert getattr(plugin_module.McbotBridgePlugin.mcbot_task, sdk.LLM_TOOL_META_ATTR, None) is None
    for meta, _ in tools:
        payload = meta.to_ipc_payload(plugin_id="mcbot_bridge")
        assert payload["parameters"]["type"] == "object"
        assert payload["type"] == "LLM_TOOL_REGISTER"


async def test_actual_host_message_translator_accepts_targeted_question_and_done():
    sdk = host_module("plugin/sdk/shared/core/push_message_schema.py", "_mcbot_host_messages")
    canonical = []

    def submit(**payload):
        canonical.append(sdk.translate_push_message(**payload))
        return {"submitted": True}

    service, _ = _make_service()
    service._session_id = "test-session"
    service._push_message = submit
    service._task_targets[7] = "cat-A"
    await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="exact-route-marker"))
    await service.handle_event(_fresh("done", task_id=7, status="failed", text="actual-result-marker"))
    assert len(canonical) == 2
    assert all(item["target_lanlan"] == "cat-A" for item in canonical)
    assert all(item["ai_behavior"] == "respond" for item in canonical)
    assert all(item["visibility"] == [] for item in canonical)
    assert all(item["_legacy_call"] is False for item in canonical)
    assert "exact-route-marker" in canonical[0]["parts"][0]["text"]
    assert canonical[0]["coalesce_key"] != canonical[1]["coalesce_key"]


def test_actual_host_uses_original_context_for_recipient(plugin_module):
    # The public SDK facade has no current_lanlan property in this host.
    # NekoPluginBase keeps the original context on _host_ctx.
    host_module("plugin/sdk/plugin/llm_tool.py", "_mcbot_host_context_probe")
    original = SimpleNamespace(current_lanlan="cat-A")
    wrapped = SimpleNamespace()
    plugin = SimpleNamespace(_host_ctx=original, ctx=wrapped)
    assert plugin_module._recipient(plugin) == "cat-A"
