"""Reproduce the analyzer mistaking a config reload for gathering completion."""
import ast
import os
from pathlib import Path
from types import MethodType, SimpleNamespace
from typing import Dict, Optional
from unittest.mock import AsyncMock

import pytest
from mcbot_bridge_plugin import prompts
from test_host_contract import host_module
from test_mcbot_bridge_plugin import _FakeClient, _fresh, _make_service


def host_result_contract():
    root = Path(os.environ.get("NEKO_HOST_SOURCE", Path(__file__).resolve().parents[2] / "N.E.K.O"))
    source = root / "app/agent_server/channels/user_plugin.py"
    if not source.is_file():
        pytest.skip("Set NEKO_HOST_SOURCE to verify the host's real analyzer contract")
    # Load just these pure production functions, avoiding application imports
    # that would initialize the host's user storage, logging or services.
    parsed = ast.parse(source.read_text(encoding="utf-8"))
    names = {"_resolve_delivery_mode", "_resolve_plugin_result_contract"}
    functions = [node for node in parsed.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(functions) == len(names)
    namespace = {
        "Dict": Dict, "Optional": Optional,
        "_lookup_plugin_entry": lambda *_: {},
        "_positive_finite_seconds": lambda _: None,
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
    return namespace


async def make_facade(plugin_module):
    service, pushes = _make_service()
    service._client = _FakeClient()
    pending = await service.try_claim_pending("去采集两个橡木原木", target_lanlan="cat-A")
    await service.dispatch_claimed(pending)
    # Configuration I/O is controlled; the facade, service slot, SSE handling,
    # SDK envelope and host delivery decisions remain production code.
    service.reload_config_live = AsyncMock(return_value=False)
    finish_sdk = host_module("plugin/sdk/shared/core/finish.py", "_mcbot_control_finish")

    async def finish(**kwargs):
        return finish_sdk.build_finish_envelope(**kwargs)

    plugin = SimpleNamespace(
        _service=service, _lang="zh", _cfg={},
        _host_ctx=SimpleNamespace(current_lanlan="cat-A"),
        _ensure_service_started=AsyncMock(return_value=True),
        config=SimpleNamespace(dump=AsyncMock(return_value={"mcbot": {}})),
        finish=finish,
    )
    plugin._control_result = MethodType(plugin_module.McbotBridgePlugin._control_result, plugin)
    return plugin, service, pending, pushes


@pytest.mark.parametrize("entry", ["mcbot_status", "mcbot_reload_config", "mcbot_stop"])
async def test_manager_success_never_announces_game_goal_completion(plugin_module, entry):
    plugin, service, pending, pushes = await make_facade(plugin_module)
    try:
        result = await getattr(plugin_module.McbotBridgePlugin, entry)(plugin)
        contract = host_result_contract()
        # The old Ok(status) payload had no finish metadata: proactive task_result.
        assert contract["_resolve_delivery_mode"]({"summary": "config reloaded"}) == "proactive"
        assert result["success"] is True
        assert contract["_resolve_delivery_mode"](result) == "silent"
        assert contract["_resolve_plugin_result_contract"]("mcbot_bridge", entry, result)[0] == "event"
        assert not pushes
        assert result["data"]["summary"]
        assert len([call for call in service._client.calls if call[0] == "post_task"]) == 1
        if entry != "mcbot_stop":
            assert service._pending is pending
            assert not service.get_status(target_lanlan="cat-A")["recent_results"]
    finally:
        await service.stop()


async def test_reload_during_gather_does_not_replace_the_actual_result(plugin_module):
    plugin, service, pending, pushes = await make_facade(plugin_module)
    try:
        await plugin_module.McbotBridgePlugin.mcbot_reload_config(plugin)
        await service.handle_event(_fresh(
            "progress", task_id=7, tool="gather", ok=True,
            text="实际进度=2，尚缺=0；局部步骤，等待最终核验",
        ))
        assert not [push for push in pushes if push["ai_behavior"] == "respond"]
        await service.handle_event(_fresh(
            "done", task_id=7, status="completed",
            text="已采集到2个橡木原木，背包基线0，当前2。",
        ))
        reports = [push for push in pushes if push["ai_behavior"] == "respond"]
        assert len(reports) == 1
        text = reports[0]["parts"][0]["text"]
        assert "去采集两个橡木原木" in text
        assert "背包基线0，当前2" in text
        assert "config reloaded" not in text
        assert reports[0]["target_lanlan"] == "cat-A"
        assert pending.result["status"] == "completed"
    finally:
        await service.stop()


@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_original_request_does_not_turn_partial_work_into_success(status):
    service, pushes = _make_service()
    service._client = _FakeClient()
    pending = await service.try_claim_pending("collect 2 oak logs")
    try:
        await service.dispatch_claimed(pending)
        await service.handle_event(_fresh(
            "done", task_id=7, status=status, text="Only 1 collected; 1 missing. Route blocked.",
        ))
        report = pushes[-1]["parts"][0]["text"]
        assert "collect 2 oak logs" in report
        assert "Only 1 collected; 1 missing. Route blocked." in report
        assert pending.result["text"] == "Only 1 collected; 1 missing. Route blocked."
        assert pending.result["status"] == status
    finally:
        await service.stop()


@pytest.mark.parametrize("lang", prompts.SUPPORTED_LANGS)
def test_result_context_keeps_request_and_evidence_in_every_locale(lang):
    text = prompts.t("RESULT_CONTEXT", lang=lang, directive="request-marker", text="evidence-marker")
    assert "request-marker" in text and "evidence-marker" in text
