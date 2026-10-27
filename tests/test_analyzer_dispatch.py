"""Real analyzer visibility, task acceptance, and execution receipts without a host process."""
import ast
import logging
import os
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from mcbot_bridge_plugin.client import McbotBridgeError
from test_control_reporting import host_result_contract
from test_host_contract import host_module
from test_mcbot_bridge_plugin import _FakeClient, _fresh, _make_service


def facade(plugin_module, service, *, role="cat-A"):
    sdk = host_module("plugin/sdk/shared/core/finish.py", "_mcbot_dispatch_finish")

    async def finish(**kwargs):
        return sdk.build_finish_envelope(**kwargs)

    plugin = SimpleNamespace(
        _service=service, _lang="zh", _host_ctx=SimpleNamespace(current_lanlan=role),
        _ensure_service_started=AsyncMock(return_value=True), finish=finish,
    )
    plugin._control_result = MethodType(plugin_module.McbotBridgePlugin._control_result, plugin)
    return plugin


@pytest.mark.parametrize("instruction", [
    "去收集材料，做一把钻石镐", "跟着我，有敌人时保护我", "清理地基，然后建一栋房子", "过来我这边",
])
async def test_visible_task_entry_dispatches_any_original_request_and_keeps_the_role(plugin_module, instruction):
    service, pushes = _make_service()
    service._client = _FakeClient()
    plugin = facade(plugin_module, service)
    try:
        receipt = await plugin_module.McbotBridgePlugin.mcbot_task(
            plugin, text="analyzer paraphrase must not replace the original",
            _ctx={"latest_user_request": instruction},
        )
        assert receipt["success"] is True
        assert receipt["data"]["accepted"] is True
        assert receipt["data"]["task_id"] == 7
        assert receipt["data"]["goal_satisfied"] is False
        contract = host_result_contract()
        assert contract["_resolve_delivery_mode"](receipt) == "silent"
        assert contract["_resolve_plugin_result_contract"]("mcbot_bridge", "mcbot_task", receipt)[0] == "event"
        assert service._client.calls.count(("post_task", instruction, 0)) == 1
        assert service._pending.target_lanlan == "cat-A"
        assert not pushes
        await service.handle_event(_fresh("done", task_id=7, status="failed", text="Verified partial result."))
        assert len(pushes) == 1
        assert pushes[0]["target_lanlan"] == "cat-A"
        assert instruction in pushes[0]["parts"][0]["text"]
    finally:
        await service.stop()


async def test_another_entry_call_is_refused_while_the_original_task_runs(plugin_module):
    service, _ = _make_service()
    service._client = _FakeClient()
    plugin = facade(plugin_module, service)
    try:
        first = await plugin_module.McbotBridgePlugin.mcbot_task(plugin, text="采集两根原木")
        duplicate = await plugin_module.McbotBridgePlugin.mcbot_task(plugin, text="采集两根原木")
        assert first["data"]["accepted"] is True
        assert not duplicate.is_ok()
        assert len([call for call in service._client.calls if call[0] == "post_task"]) == 1
        assert service._pending.task_id == 7
    finally:
        await service.stop()


@pytest.mark.parametrize("text", [None, "", 17, "mcbot_task"])
async def test_invalid_entry_arguments_do_not_dispatch_or_claim_acceptance(plugin_module, text):
    service, _ = _make_service()
    service._client = _FakeClient()
    try:
        result = await plugin_module.McbotBridgePlugin.mcbot_task(facade(plugin_module, service), text=text)
        assert not result.is_ok()
        assert not service._client.calls
    finally:
        await service.stop()


async def test_unknown_post_result_is_not_reported_as_accepted_and_is_never_resent(plugin_module):
    service, _ = _make_service()
    service._client = _FakeClient(raise_on={"post_task": McbotBridgeError("synthetic", code="INTERNAL")})
    plugin = facade(plugin_module, service)
    try:
        result = await plugin_module.McbotBridgePlugin.mcbot_task(plugin, text="采集两根原木")
        assert not result.is_ok()
        assert service._pending.state == "unknown"
        again = await plugin_module.McbotBridgePlugin.mcbot_task(plugin, text="采集两根原木")
        assert not again.is_ok()
        assert len([call for call in service._client.calls if call[0] == "post_task"]) == 1
    finally:
        await service.stop()


async def test_actual_host_analyzer_filter_keeps_the_plain_task_entry_and_hides_dialog_tools(plugin_module):
    root = Path(os.environ.get("NEKO_HOST_SOURCE", Path(__file__).resolve().parents[2] / "N.E.K.O"))
    source = root / "app/agent_server/api_runtime.py"
    if not source.is_file():
        pytest.skip("Set NEKO_HOST_SOURCE to test the real analyzer filter")
    tree = ast.parse(source.read_text(encoding="utf-8"))
    provider = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.AsyncFunctionDef) and node.name == "_http_plugin_provider")
    plugin_source = ast.parse((Path(__file__).resolve().parents[1] / "__init__.py").read_text(encoding="utf-8"))
    declared = {}
    for method in ast.walk(plugin_source):
        if not isinstance(method, ast.AsyncFunctionDef):
            continue
        for decorator in method.decorator_list:
            if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name)
                    and decorator.func.id in ("plugin_entry", "llm_tool")):
                key = "id" if decorator.func.id == "plugin_entry" else "name"
                name = next(ast.literal_eval(k.value) for k in decorator.keywords if k.arg == key)
                ident = name if key == "id" else "__llm_tool__" + name
                declared[ident] = {"id": ident}
    assert "mcbot_task" in declared
    assert "__llm_tool__mcbot_task" not in declared
    assert "text" in plugin_module.MCBOT_TASK_SCHEMA["required"]

    def transport(_):
        return httpx.Response(200, json={"plugins": [
            {"id": "mcbot_bridge", "status": "running", "entries": list(declared.values())},
        ]})

    namespace = {
        "USER_PLUGIN_SERVER_PORT": 48916, "logger": logging.getLogger("offline-analyzer"),
        "httpx": SimpleNamespace(AsyncClient=lambda **kwargs: httpx.AsyncClient(
            **kwargs, transport=httpx.MockTransport(transport),
        )),
    }
    exec(compile(ast.Module(body=[provider], type_ignores=[]), str(source), "exec"), namespace)
    visible = await namespace["_http_plugin_provider"]()
    assert {entry["id"] for entry in visible[0]["entries"]} == {
        "mcbot_task", "mcbot_answer", "mcbot_status", "mcbot_stop", "mcbot_reload_config",
    }


async def test_background_answer_returns_to_the_original_task_without_redispatch(plugin_module):
    service, _ = _make_service()
    service._client = _FakeClient()
    plugin = facade(plugin_module, service)
    try:
        await plugin_module.McbotBridgePlugin.mcbot_task(plugin, text="采集材料")
        await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="是否确认路线？"))
        result = await plugin_module.McbotBridgePlugin.mcbot_answer_entry(
            plugin, text="paraphrase", _ctx={"latest_user_request": "确认"},
        )
        assert result["data"]["answer_submitted"] is True
        assert result["data"]["goal_satisfied"] is False
        assert host_result_contract()["_resolve_delivery_mode"](result) == "silent"
        assert ("answer", "q1", "确认") in service._client.calls
        assert len([call for call in service._client.calls if call[0] == "post_task"]) == 1
        assert service._pending.task_id == 7
    finally:
        await service.stop()


async def test_background_answer_cannot_approve_another_characters_question(plugin_module):
    service, _ = _make_service()
    service._client = _FakeClient()
    owner = facade(plugin_module, service, role="cat-A")
    other = facade(plugin_module, service, role="cat-B")
    try:
        await plugin_module.McbotBridgePlugin.mcbot_task(owner, text="采集材料")
        await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="是否确认路线？"))
        result = await plugin_module.McbotBridgePlugin.mcbot_answer_entry(other, text="确认", question_id="q1")
        assert not result.is_ok()
        assert not [call for call in service._client.calls if call[0] == "answer"]
        assert "q1" in service._pending_questions
    finally:
        await service.stop()
