"""Verified achievements reuse the reliable notification path without settling work."""
import asyncio
import json
from pathlib import Path

import pytest
from mcbot_bridge_plugin import prompts
from test_mcbot_bridge_plugin import _FakeClient, _fresh, _make_service
from test_task_reliability import running_service


def milestone(ident="a", kind="item", **changes):
    return {
        "id": ident * 64, "kind": kind, "verified": True,
        "facts": "Verified inventory: minecraft:iron_pickaxe=1.",
        "evidence": {"item": "minecraft:iron_pickaxe", "current": 1},
        **changes,
    }


async def test_distinct_milestones_speak_once_keep_task_and_do_not_coalesce_or_redispatch():
    service, pending, pushes = await running_service(target="cat-A")
    for ident in ("a", "a", "b"):
        await service.handle_event(_fresh("progress", task_id=7, milestone=milestone(ident)))
    assert len(pushes) == 2
    assert all(p["ai_behavior"] == "respond" and p["target_lanlan"] == "cat-A" for p in pushes)
    assert all(p["priority"] == 7 for p in pushes)
    assert pushes[0]["coalesce_key"] != pushes[1]["coalesce_key"]
    assert all("mine one diamond" in p["parts"][0]["text"] for p in pushes)
    assert service._pending is pending and not pending.event.is_set()
    assert not service._history
    assert len([c for c in service._client.calls if c[0] == "post_task"]) == 1
    await service.stop()


async def test_ordinary_progress_does_not_erase_the_achievement_ledger():
    service, _, pushes = await running_service()
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone()))
    for index in range(260):
        await service.handle_event(_fresh("progress", task_id=7, tool="status", ok=True, text=str(index)))
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone()))
    assert len([p for p in pushes if p["ai_behavior"] == "respond"]) == 1
    await service.stop()


@pytest.mark.parametrize("changes", [
    {"verified": False}, {"verified": "true"}, {"id": "invalid"}, {"facts": ""},
    {"facts": "x" * 4001}, {"kind": "unknown"}, {"evidence": None},
])
async def test_unverified_or_malformed_report_never_opens_a_speaking_turn(changes):
    service, pending, pushes = await running_service()
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone(**changes)))
    assert not pushes
    assert service._pending is pending
    await service.stop()


async def test_successful_query_accepted_job_and_goal_checkpoint_remain_silent():
    service, _, pushes = await running_service()
    for payload in (
        {"tool": "craft", "ok": True, "text": "query: can_craft=true"},
        {"tool": "move_to", "job_id": "j1", "text": "accepted"},
        {"goal_checkpoint": True, "text": "stage checkpoint"},
    ):
        await service.handle_event(_fresh("progress", task_id=7, **payload))
    assert all(p["ai_behavior"] == "blind" for p in pushes)
    await service.stop()


async def test_fast_milestone_waits_for_task_association_and_ignores_unrelated_task():
    service, pushes = _make_service()
    entered, release = asyncio.Event(), asyncio.Event()

    class DelayedClient(_FakeClient):
        async def post_task(self, text, *, wait_s=0):
            entered.set()
            await release.wait()
            return await super().post_task(text, wait_s=wait_s)

    service._client = DelayedClient()
    pending = await service.try_claim_pending("long crafting task", target_lanlan="cat-A")
    submit = asyncio.create_task(service.dispatch_claimed(pending))
    await entered.wait()
    await service.handle_event(_fresh("progress", task_id=99, milestone=milestone("b")))
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone("a")))
    assert not pushes
    release.set()
    await submit
    assert len(pushes) == 1
    assert pushes[0]["target_lanlan"] == "cat-A"
    assert "long crafting task" in pushes[0]["parts"][0]["text"]
    assert service._pending is pending
    await service.stop()


async def test_rejected_milestones_survive_reload_retry_in_order_and_never_repeat(tmp_path):
    service, _, _ = await running_service(target="cat-A")
    service._session_id = "s1"
    service._cursor_path = tmp_path / "cursor.json"
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    for ident in ("a", "b"):
        await service.handle_event(_fresh("progress", task_id=7, milestone=milestone(ident)))
    assert len(service._outbox) == 2
    await service.stop()
    restored, pushes = _make_service()
    restored._cursor_path = tmp_path / "cursor.json"
    restored._client = _FakeClient()
    await restored._sync_session("s1")
    restored._retry_notifications()
    assert len(pushes) == 2
    assert pushes[0]["coalesce_key"].endswith("a" * 64)
    assert pushes[1]["coalesce_key"].endswith("b" * 64)
    assert not restored._outbox
    await restored.handle_event(_fresh("progress", task_id=7, milestone=milestone("a")))
    assert len(pushes) == 2
    assert restored._pending.task_id == 7
    assert not [c for c in restored._client.calls if c[0] == "post_task"]
    await restored.stop()


async def test_old_task_milestones_cannot_speak_after_done_or_new_session():
    service, _, pushes = await running_service()
    await service.handle_event(_fresh("done", task_id=7, status="cancelled", text="stopped"))
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone()))
    assert not pushes
    await service._sync_session("new-session")
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone()))
    assert not pushes
    await service.stop()

async def test_completion_cannot_overtake_an_earlier_unaccepted_milestone():
    service, pending, pushes = await running_service()
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    await service.handle_event(_fresh("progress", task_id=7, milestone=milestone("a")))
    service._push_message = lambda **payload: pushes.append(payload) or {"submitted": True}
    await service.handle_event(_fresh("done", task_id=7, status="completed", text="Final actual result."))
    assert pending.event.is_set()
    assert not pushes
    assert len(service._outbox) == 2
    service._retry_notifications()
    assert len(pushes) == 2
    assert "mcbot_milestone:" in pushes[0]["coalesce_key"]
    assert "mcbot_done:" in pushes[1]["coalesce_key"]
    await service.stop()


async def test_failed_retry_keeps_later_nodes_queued_without_losing_them():
    service, _, pushes = await running_service()
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    for ident in ("a", "b"):
        await service.handle_event(_fresh("progress", task_id=7, milestone=milestone(ident)))
    attempted = []
    service._push_message = lambda **payload: attempted.append(payload) or {"submitted": False}
    service._retry_notifications()
    assert len(attempted) == 1
    assert len(service._outbox) == 2
    service._push_message = lambda **payload: pushes.append(payload) or {"submitted": True}
    service._retry_notifications()
    assert len(pushes) == 2
    assert not service._outbox
    await service.stop()


def test_all_locales_frame_milestones_as_intermediate_first_person_reports():
    for lang in prompts.SUPPORTED_LANGS:
        text = prompts.t(
            "CUE_MILESTONE", lang=lang, identity="", directive="original-marker",
            text="verified-marker", internal_gag=prompts.gag(lang),
        )
        assert "verified-marker" in text and "original-marker" in text
        assert "{MASTER_NAME}" in text
        assert "{internal_gag}" not in text


@pytest.mark.parametrize("source", ["gather", "craft"])
async def test_automatic_stages_use_the_same_reliable_path_without_settling_or_redispatching(source):
    service, pending, pushes = await running_service(target="cat-A")
    report = milestone(evidence={"source": source, "item": "example:tool", "current": 1})
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    await service.handle_event(_fresh("progress", task_id=7, milestone=report))
    assert service._pending is pending and not pending.event.is_set()
    assert len(service._outbox) == 1
    for _ in range(300):
        await service.handle_event(_fresh("progress", task_id=7, tool="move_to", text="working"))
    assert len(service._outbox) == 1, "ordinary progress cannot erase an unsent achievement"
    service._push_message = lambda **payload: pushes.append(payload) or {"submitted": True}
    service._retry_notifications()
    assert len(pushes) == 1
    assert pushes[0]["ai_behavior"] == "respond"
    assert pushes[0]["target_lanlan"] == "cat-A"
    await service.handle_event(_fresh("progress", task_id=7, milestone=report))
    assert len(pushes) == 1
    assert service._pending is pending
    assert len([call for call in service._client.calls if call[0] == "post_task"]) == 1
    await service.stop()


async def test_actual_runner_sse_reports_are_accepted_in_order_and_survive_backpressure():
    # Generated by AgentRunnerMilestoneTest through the real local HTTP/SSE adapter.
    frames = json.loads((Path(__file__).parent / "fixtures/automatic-milestones.json").read_text(encoding="utf-8"))
    assert [frame["milestone"]["evidence"]["source"] for frame in frames] == ["gather", "craft"]
    service, pending, pushes = await running_service(target="cat-A")
    service._push_message = lambda **_: {"submitted": False, "reason": "backpressure"}
    for frame in frames:
        assert frame["ev"] == "progress" and frame["contract_version"] == "1.0"
        await service.handle_event(_fresh("progress", task_id=7, milestone=frame["milestone"]))
    assert len(service._outbox) == 2 and service._pending is pending
    assert not pending.event.is_set()
    await service.handle_event(_fresh("done", task_id=7, status="completed", text="Actual final result."))
    assert pending.event.is_set() and len(service._outbox) == 3
    service._push_message = lambda **payload: pushes.append(payload) or {"submitted": True}
    service._retry_notifications()
    assert len(pushes) == 3
    assert frames[0]["milestone"]["facts"] in pushes[0]["parts"][0]["text"]
    assert frames[1]["milestone"]["facts"] in pushes[1]["parts"][0]["text"]
    assert "mcbot_done:" in pushes[2]["coalesce_key"]
    assert all(payload["target_lanlan"] == "cat-A" for payload in pushes)
    assert len([call for call in service._client.calls if call[0] == "post_task"]) == 1
    await service.stop()
