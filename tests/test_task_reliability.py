"""Task transport regressions: no world, token files or model endpoints."""
import asyncio
import json
import time

import httpx
import pytest
from mcbot_bridge_plugin import prompts
from mcbot_bridge_plugin.client import McbotBridgeError
from mcbot_bridge_plugin.service import COMMAND_DIRECTIVES, normalise_base_url
from test_mcbot_bridge_plugin import _FakeClient, _fresh, _make_service


async def running_service(*, task_id=7, target=None):
    service, pushes = _make_service()
    service._client = _FakeClient(task_id=task_id)
    pending = await service.try_claim_pending("mine one diamond", target_lanlan=target)
    await service.dispatch_claimed(pending)
    return service, pending, pushes


async def test_timeout_still_allows_stop_and_never_resubmits():
    service, pending, _ = await running_service()
    service._task_wait_timeout = 0
    await service._watch_pending(pending)
    assert pending.state == "unknown"
    assert service._pending is pending
    assert await service.try_claim_pending("mine another diamond") is None
    await service.cancel()
    assert ("cancel_task", None) in service._client.calls
    assert not service.has_pending_task()
    assert len([c for c in service._client.calls if c[0] == "post_task"]) == 1
    await service.stop()


async def test_late_done_after_timeout_settles_original_task_once():
    service, pending, pushes = await running_service()
    service._task_wait_timeout = 0
    await service._watch_pending(pending)
    await service.handle_event(_fresh("done", task_id=7, status="completed", text="Verified one diamond in inventory."))
    await service.handle_event(_fresh("done", task_id=7, status="completed", text="duplicate"))
    assert pending.result["status"] == "completed"
    assert not service.has_pending_task()
    assert len([p for p in pushes if p["coalesce_key"].startswith("mcbot_done:")]) == 1
    assert len(service.get_status()["recent_results"]) == 1
    await service.stop()


@pytest.mark.parametrize("code", ["BRIDGE_TIMEOUT", "BRIDGE_TRANSPORT", "BRIDGE_BAD_PAYLOAD", "INTERNAL"])
async def test_unknown_acceptance_blocks_new_work_and_unrelated_done(code):
    service, _ = _make_service()
    service._client = _FakeClient(raise_on={"post_task": McbotBridgeError("synthetic", code=code)})
    pending = await service.try_claim_pending("mine one diamond")
    result = await service.dispatch_claimed(pending)
    assert result["is_error"]
    assert pending.state == "unknown"
    assert pending.task_id is None
    assert await service.try_claim_pending("repeat") is None
    pending.started_at -= 10
    assert await service.try_claim_pending("repeat", overwrite=True) is None
    await service.handle_event(_fresh("done", task_id=99, status="completed", text="other work"))
    assert service._pending is pending
    await service.cancel()
    assert ("cancel_task", None) in service._client.calls
    await service.stop()


@pytest.mark.parametrize("status", [400, 401, 413])
async def test_definitive_submission_rejection_releases_slot(status):
    service, _ = _make_service()
    service._client = _FakeClient(raise_on={"post_task": McbotBridgeError("synthetic", status=status)})
    pending = await service.try_claim_pending("mine one diamond")
    await service.dispatch_claimed(pending)
    assert service._pending is None
    await service.stop()


@pytest.mark.parametrize("task_id", [None, 0, -1, True, "7"])
async def test_malformed_acceptance_never_defaults_to_an_unattributed_task(task_id):
    service, _ = _make_service()
    service._client = _FakeClient(task_id=task_id)
    pending = await service.try_claim_pending("mine one diamond")
    result = await service.dispatch_claimed(pending)
    assert result["error"] == "SUBMISSION_UNKNOWN"
    assert service._pending is pending
    assert pending.state == "unknown"
    await service.stop()


async def test_stop_cannot_overtake_the_task_post():
    entered, release = asyncio.Event(), asyncio.Event()
    service, _ = _make_service()

    class DelayedClient(_FakeClient):
        async def post_task(self, text, *, wait_s=0):
            entered.set()
            await release.wait()
            return await super().post_task(text, wait_s=wait_s)

    client = DelayedClient()
    service._client = client
    submit = asyncio.create_task(service.submit("mine one diamond"))
    await entered.wait()
    stop = asyncio.create_task(service.cancel())
    await asyncio.sleep(0)
    assert not any(c[0] == "cancel_task" for c in client.calls)
    release.set()
    await asyncio.gather(submit, stop)
    mutations = [c[0] for c in client.calls if c[0] in ("post_task", "cancel_task")]
    assert mutations == ["post_task", "cancel_task"]
    assert not service.has_pending_task()
    await service.stop()


async def test_cancelled_dispatch_keeps_acceptance_unknown():
    entered = asyncio.Event()
    service, _ = _make_service()

    class DelayedClient(_FakeClient):
        async def post_task(self, text, *, wait_s=0):
            entered.set()
            await asyncio.Future()

    service._client = DelayedClient()
    task = asyncio.create_task(service.submit("mine one diamond"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service._pending.state == "unknown"
    assert await service.try_claim_pending("repeat") is None
    await service.stop()


async def test_status_query_reads_progress_without_posting_or_interrupting():
    service, pending, _ = await running_service()
    await service.handle_event(_fresh("progress", task_id=7, tool="collect", ok=True, text="one stone collected"))
    result = await service.ask("How much diamond do you have?")
    observation = result["output"]["observation"]
    assert observation["pending_task_id"] == 7
    assert observation["latest_progress"]["text"] == "one stone collected"
    assert observation["latest_progress"]["observed_at"] <= time.time()
    assert "diamond" not in json.dumps(observation["latest_progress"])
    assert service._pending is pending
    assert not [c for c in service._client.calls if c[0] == "ask"]
    assert len([c for c in service._client.calls if c[0] == "post_task"]) == 1
    await service.stop()


@pytest.mark.parametrize("directive", COMMAND_DIRECTIVES)
async def test_all_current_recovery_aliases_are_receipts(directive):
    service, pushes = _make_service()
    service._client = _FakeClient()
    pending = await service.try_claim_pending(directive)
    await service.dispatch_claimed(pending)
    await service.handle_event(_fresh("done", task_id=7, status="completed", text="Recovery accepted."))
    cue = pushes[-1]["parts"][0]["text"]
    assert "Command receipt" in cue
    assert "I finished" not in cue
    await service.stop()


async def test_recovered_execution_is_observed_and_stoppable_after_command_done():
    service, _ = _make_service()
    service._client = _FakeClient(task_id=7)
    pending = await service.try_claim_pending("继续建造")
    await service.dispatch_claimed(pending)
    await service.handle_event(_fresh("state", task_id=8, status="running"))
    await service.handle_event(_fresh("done", task_id=7, status="completed", text="Recovery accepted."))
    assert service.get_status()["executor"]["current_task"] == 8
    service._client._status_payload = {"in_game": True, "current_task": 8}
    result = await service.submit("new unrelated work")
    assert result["error"] == "REMOTE_BUSY"
    await service.cancel()
    assert ("cancel_task", None) in service._client.calls
    await service.stop()


@pytest.mark.parametrize("failure", ["rejected", "exception", "no_receipt"])
async def test_rejected_done_notification_is_retried_without_duplicate_task_settlement(failure):
    service, pending, pushes = await running_service()
    attempts = []

    def unavailable(**payload):
        attempts.append(payload)
        if failure == "exception":
            raise RuntimeError("synthetic")
        return {"submitted": False, "reason": "transport_unavailable"} if failure == "rejected" else None

    service._push_message = unavailable
    event = _fresh("done", task_id=7, status="completed", text="Only partial work was possible.")
    await service.handle_event(event)
    assert pending.event.is_set()
    assert len(service._outbox) == 1
    assert len(service._history) == 1
    assert len(attempts) == 1

    def accepted(**payload):
        pushes.append(payload)
        return {"submitted": True}

    service._push_message = accepted
    await service.handle_event(event)
    await service.handle_event(event)
    assert not service._outbox
    assert len(service._history) == 1
    assert len(pushes) == 1
    await service.stop()


async def test_rejected_notification_and_unknown_active_task_survive_plugin_reload(tmp_path):
    service, pending, _ = await running_service(target="cat-A")
    service._cursor_path = tmp_path / "cursor.json"
    service._session_id = "s1"
    service._client.last_event_id = "12"
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    await service.handle_event(_fresh("done", task_id=99, status="failed", text="Other task failed."))
    await service.stop()
    assert pending.result["status"] == "unknown"

    restored, pushes = _make_service()
    restored._cursor_path = tmp_path / "cursor.json"
    restored._client = _FakeClient()
    await restored._sync_session("s1")
    assert restored._pending.task_id == 7
    assert restored._pending.state == "unknown"
    assert restored._pending.target_lanlan == "cat-A"
    assert restored._client.restored_cursors == ["12"]
    restored._retry_notifications()
    assert len(pushes) == 1
    assert not restored._outbox
    assert not [c for c in restored._client.calls if c[0] == "post_task"]
    await restored.stop()


async def test_confirmed_completion_and_notification_receipt_survive_restart(tmp_path):
    service, _, _ = await running_service()
    service._cursor_path = tmp_path / "cursor.json"
    service._session_id = "s1"
    event = _fresh("done", task_id=7, status="completed", text="Verified result.")
    await service.handle_event(event)
    await service.stop()
    restored, pushes = _make_service()
    restored._cursor_path = tmp_path / "cursor.json"
    restored._client = _FakeClient()
    await restored._sync_session("s1")
    await restored.handle_event(event)
    assert not pushes
    assert not restored._pending
    await restored.stop()


async def test_new_world_does_not_restore_active_task_or_approval(tmp_path):
    service, _, _ = await running_service()
    service._cursor_path = tmp_path / "cursor.json"
    service._session_id = "s1"
    await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="Approve this exact route?"))
    await service.stop()
    restored, _ = _make_service()
    restored._cursor_path = tmp_path / "cursor.json"
    restored._client = _FakeClient()
    await restored._sync_session("s2")
    assert restored._pending is None
    assert not restored._pending_questions
    assert not restored._task_directives
    assert restored.get_status()["recent_results"][-1]["status"] == "unknown"
    await restored.stop()


async def test_expired_or_settled_question_is_not_retried():
    service, _, _ = await running_service()
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="Approve this route?"))
    assert service._outbox
    service._pending_questions["q1"]["asked_at"] -= 121
    service._retry_notifications()
    assert not service._outbox
    assert not service._pending_questions
    await service.stop()


async def test_role_cannot_read_or_answer_another_roles_task_question():
    service, pending, pushes = await running_service(target="cat-A")
    await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="Private route?"))
    await service.handle_event(_fresh("progress", task_id=7, text="Private progress"))
    own = service.get_status(target_lanlan="cat-A")
    other = service.get_status(target_lanlan="cat-B")
    assert own["pending_question_items"]
    assert not other["pending_question_items"]
    assert not other["pending_task"]
    assert not other["latest_progress"]
    assert pushes[0]["target_lanlan"] == "cat-A"
    result = await service.answer("q1", "yes", target_lanlan="cat-B")
    assert result["error"] == "QUESTION_OWNER_MISMATCH"
    assert not [c for c in service._client.calls if c[0] == "answer"]
    await service.answer("", "yes", target_lanlan="cat-A")
    assert ("answer", "q1", "yes") in service._client.calls
    assert service._pending is pending
    await service.stop()


async def test_question_before_post_reply_keeps_original_recipient():
    service, pushes = _make_service()

    class FastQuestion(_FakeClient):
        async def post_task(self, text, *, wait_s=0):
            await service.handle_event(_fresh("question", task_id=7, question_id="q1", text="Approve?"))
            assert not pushes
            return await super().post_task(text, wait_s=wait_s)

    service._client = FastQuestion()
    pending = await service.try_claim_pending("mine one diamond", target_lanlan="cat-A")
    await service.dispatch_claimed(pending)
    assert pushes[0]["target_lanlan"] == "cat-A"
    await service.stop()


async def test_unknown_done_does_not_hide_later_valid_terminal():
    service, pending, pushes = await running_service()
    await service.handle_event(_fresh("done", task_id=7, text="Status missing"))
    assert service._pending is pending
    assert "Unverified result" in pushes[-1]["parts"][0]["text"]
    await service.handle_event(_fresh("done", task_id=7, status="failed", text="Failure confirmed"))
    assert pending.result["status"] == "failed"
    assert not service._pending
    await service.stop()


async def test_gap_marks_active_observation_unknown_without_resubmission():
    service, pending, _ = await running_service()
    await service.mark_event_gap()
    assert pending.state == "unknown"
    assert service.get_status()["event_gap"] is True
    assert len([c for c in service._client.calls if c[0] == "post_task"]) == 1
    await service.stop()


@pytest.mark.parametrize("field", ["in_game", "brain_enabled", "body_available"])
async def test_not_ready_executor_does_not_receive_work(field):
    service, _ = _make_service()
    service._client = _FakeClient(status_payload={field: False})
    result = await service.submit("mine one diamond")
    assert result["error"] == "EXECUTOR_UNAVAILABLE"
    assert not [c for c in service._client.calls if c[0] == "post_task"]
    await service.stop()


@pytest.mark.parametrize("url", [
    "http://example.com", "http://127.0.0.1/token?secret=1",
    "http://user:password@127.0.0.1", "https://127.0.0.1", "http://0.0.0.0",
])
def test_bridge_credentials_are_only_sent_to_configured_loopback_endpoint(url):
    with pytest.raises(ValueError):
        normalise_base_url(url)


async def test_actual_transport_only_observes_on_ask():
    service, _ = _make_service()
    from mcbot_bridge_plugin.client import McbotClient
    requests = []

    async def respond(request):
        requests.append((request.method, request.url.path))
        return httpx.Response(200, json={
            "contract_version": "1.0", "session_id": "s1",
            "in_game": True, "current_task": 42, "brain_enabled": True,
        })

    client = McbotClient("http://127.0.0.1:17897", "test-only-token")
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    service._client = client
    result = await service.ask("Are you done?")
    assert requests == [("GET", "/v1/status")]
    assert result["output"]["observation"]["executor"]["current_task"] == 42
    assert "token" not in json.dumps(result)
    await service.stop()

async def test_real_http_and_sse_task_question_answer_result_flow():
    from mcbot_bridge_plugin.client import McbotClient

    service, pushes = _make_service()
    posted, early_processed, answered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    class Events(httpx.AsyncByteStream):
        async def __aiter__(self):
            await posted.wait()
            payload = {"session_id": "s1", "ev": "question", "task_id": 7, "question_id": "q1", "text": "Approve this route?"}
            yield f"id: 1\nevent: question\ndata: {json.dumps(payload)}\n\n".encode()
            early_processed.set()
            await answered.wait()
            payload = {"session_id": "s1", "ev": "done", "task_id": 7, "status": "completed", "text": "Verified one diamond collected."}
            yield f"id: 2\nevent: done\ndata: {json.dumps(payload)}\n\n".encode()

    async def transport(request):
        calls.append((request.method, request.url.path))
        if request.url.path == "/v1/events":
            return httpx.Response(200, stream=Events(), headers={"content-type": "text/event-stream"})
        if request.url.path == "/v1/status":
            return httpx.Response(200, json={
                "session_id": "s1", "contract_version": "1.0", "in_game": True,
                "brain_enabled": True, "body_available": True, "current_task": 7 if posted.is_set() else 0,
                "queued_tasks": 0,
            })
        if request.url.path == "/v1/task":
            assert json.loads(request.content)["wait_s"] == 0
            posted.set()
            await early_processed.wait()
            return httpx.Response(200, json={"task_id": 7, "done": False, "fragments": []})
        if request.url.path == "/v1/answer":
            assert json.loads(request.content) == {"question_id": "q1", "text": "yes"}
            answered.set()
            return httpx.Response(200, json={"ok": True})
        raise AssertionError(request.url.path)

    client = McbotClient("http://127.0.0.1:17897", "test-only")
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(transport))
    service._client = client
    stream = asyncio.create_task(client.pump_events(service.handle_event))
    try:
        await asyncio.wait_for(service.submit("mine one diamond", target_lanlan="cat-A"), timeout=2)
        assert service.get_status(target_lanlan="cat-A")["pending_question_items"]
        assert pushes[0]["target_lanlan"] == "cat-A"
        await service.answer("", "yes", target_lanlan="cat-A")
        await asyncio.wait_for(stream, timeout=2)
        assert service.get_status(target_lanlan="cat-A")["recent_results"][-1]["status"] == "completed"
        assert not service._pending
        assert client.last_event_id == "2"
        assert calls.count(("POST", "/v1/task")) == 1
        assert ("POST", "/v1/ask") not in calls
        assert len([p for p in pushes if p["coalesce_key"].startswith("mcbot_done:")]) == 1
    finally:
        stream.cancel()
        await asyncio.gather(stream, return_exceptions=True)
        await service.stop()


async def test_failed_runtime_checkpoint_is_retried_before_event_ack(tmp_path, monkeypatch):
    service, _, _ = await running_service()
    service._cursor_path = tmp_path / "cursor.json"
    calls = 0
    original = service._persist_runtime

    async def first_write_fails():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("synthetic disk failure")
        await original()

    monkeypatch.setattr(service, "_persist_runtime", first_write_fails)
    event = _fresh("done", task_id=7, status="completed", text="Verified result")
    with pytest.raises(RuntimeError):
        await service.handle_event(event)
    await service.handle_event(event)
    assert calls == 2
    assert (tmp_path / "runtime.json").is_file()
    await service.stop()


async def test_unreadable_runtime_does_not_dispatch_or_overwrite_evidence(tmp_path):
    service, _ = _make_service()
    service._cursor_path = tmp_path / "cursor.json"
    path = tmp_path / "runtime.json"
    path.write_text("{broken", encoding="utf-8")
    service._client = _FakeClient()
    result = await service.submit("mine one diamond")
    assert result["error"] == "RUNTIME_UNAVAILABLE"
    assert not [c for c in service._client.calls if c[0] == "post_task"]
    await service.stop()
    assert path.read_text(encoding="utf-8") == "{broken"


async def test_clean_stop_and_restart_of_same_service_restores_observation(tmp_path):
    service, pending, _ = await running_service()
    service._cursor_path = tmp_path / "cursor.json"
    service._session_id = "s1"
    await service.stop()
    service._client = _FakeClient()
    await service._sync_session("s1")
    assert service._pending.task_id == pending.task_id
    assert service._pending.state == "unknown"
    assert not [c for c in service._client.calls if c[0] == "post_task"]
    await service.stop()


async def test_oversized_authorization_notice_is_retained_whole_without_retry_storm():
    service, _, _ = await running_service()
    calls = []

    def reject(**payload):
        calls.append(payload)
        return {"submitted": False, "reason": "payload_too_large"}

    service._push_message = reject
    text = "Route start " + "block-list " * 2000 + " unique-route-end"
    await service.handle_event(_fresh("question", task_id=7, question_id="q1", text=text))
    service._retry_notifications()
    service._retry_notifications()
    assert len(calls) == 1
    assert text in next(iter(service._outbox.values()))["payload"]["parts"][0]["text"]
    assert service.get_status()["notifications_blocked"] == 1
    await service.stop()


async def test_role_mismatch_does_not_echo_original_task_text():
    service, _, _ = await running_service(target="cat-A")
    result = await service.submit("new work", target_lanlan="cat-B")
    assert result["error"] == "TASK_OWNER_MISMATCH"
    assert "mine one diamond" not in json.dumps(result)
    await service.stop()


def test_recipient_uses_invocation_context_on_current_sdk(plugin_module):
    from types import SimpleNamespace

    plugin = SimpleNamespace(_host_ctx=SimpleNamespace(current_lanlan="cat-A"), ctx=object())
    assert plugin_module._recipient(plugin) == "cat-A"

async def test_old_status_response_cannot_roll_back_new_bridge_session():
    service, _ = _make_service()
    service._session_id = "s1"

    class DelayedStatus(_FakeClient):
        async def get_status(self):
            await service._sync_session("s2")
            return {"session_id": "s1", "in_game": True, "current_task": 99}

    service._client = DelayedStatus()
    result = await service.probe_status()
    assert result == {}
    assert service._session_id == "s2"
    assert "current_task" not in service._live_status
    await service.stop()


async def test_old_cancel_response_does_not_clear_new_session_observation():
    service, _ = _make_service()
    service._session_id = "s1"

    class DelayedCancel(_FakeClient):
        async def cancel_task(self, task_id=None):
            await service._sync_session("s2")
            service._live_status = {"current_task": 22}
            return {"ok": True}

    service._client = DelayedCancel()
    result = await service.cancel()
    assert result["error"] == "CANCEL_SESSION_CHANGED"
    assert service._live_status["current_task"] == 22
    await service.stop()

async def test_retired_session_event_cannot_settle_new_world_task():
    service, _ = _make_service()
    service._client = _FakeClient()
    await service._sync_session("s1")
    await service._sync_session("s2")
    pending = await service.try_claim_pending("new world work")
    await service.dispatch_claimed(pending)
    with pytest.raises(RuntimeError, match="retired"):
        await service.handle_event(_fresh("done", session_id="s1", task_id=7, status="completed", text="old result"))
    assert service._session_id == "s2"
    assert service._pending is pending
    await service.stop()

async def test_explicit_old_question_id_cannot_approve_a_new_session():
    service, _ = _make_service()
    service._client = _FakeClient()
    await service._sync_session("s1")
    await service.handle_event(_fresh("question", question_id="q1", task_id=7, text="Old route"))
    await service._sync_session("s2")
    result = await service.answer("q1", "yes")
    assert result["error"] == "QUESTION_EXPIRED"
    assert not [c for c in service._client.calls if c[0] == "answer"]
    await service.stop()


@pytest.mark.parametrize("lang", prompts.SUPPORTED_LANGS)
def test_completion_prompt_preserves_observed_result_in_every_language(lang):
    result = prompts.t("CUE_DONE", lang=lang, identity="", text="evidence-marker", internal_gag=prompts.gag(lang))
    assert "evidence-marker" in result
    assert "{MASTER_NAME}" in result
    assert "I finished" not in result
