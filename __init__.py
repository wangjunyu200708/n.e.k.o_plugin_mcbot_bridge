"""mcbot companion bridge plugin.

Bridges a local `mcbot <https://github.com/qaqms/mcbot>`_ companion (a Fabric
mod fake-player that runs its own LLM brain) into N.E.K.O. over the mod's
loopback REST + SSE bridge on ``127.0.0.1:17897``.

Where the actions live, and why:

* :func:`mcbot_task` is a normal ``@plugin_entry``. The host analyzer can
  discover it and dispatch the owner's request, including on dialog models
  that never call tools. New work has one dispatch surface so the dialog and
  analyzer cannot race to repeat the same short task.
* :func:`mcbot_ask`, :func:`mcbot_answer`, :func:`mcbot_cancel` are
  ``@llm_tool`` methods for observing progress, relaying an answer and stopping
  work. Manager entries expose status, configuration and stop to the panel.

The two audiences are mutually exclusive by construction: ``@plugin_entry``
never reaches main_server's tool registry (only ``LLM_TOOL_REGISTER``, sent for
``@llm_tool``, does), and the analyzer's plugin view has every ``__llm_tool__``
-prefixed entry stripped out (``app/agent_server/api_runtime.py``).

The third channel is the reverse one: the bridge's SSE stream is translated
into ``push_message`` cues, so the character can react to her companion's
progress and relay its questions without anyone polling for them.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict

from plugin.sdk.plugin import (
    Err,
    NekoPluginBase,
    Ok,
    lifecycle,
    llm_tool,
    neko_plugin,
    plugin_entry,
    ui,
)

from . import prompts
from .service import McbotService, task_looks_like_identifier

# ---------------------------------------------------------------------------
# JSON Schemas are module-level constants rather than inline literals so the
# plugin's own introspection (the status entry, unit tests) can reference the
# exact contract the model sees.
# ---------------------------------------------------------------------------

MCBOT_TASK_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "text": {
            "type": "string",
            "description": (
                "A plain-language instruction for the companion, worded the way "
                "the user said it. When the user gave the instruction, relay "
                "their exact wording — do not translate, split, add steps, "
                "quantities or coordinates, or invent a target. Never pass a "
                "bare function or tool name."
            ),
        },
        "overwrite": {
            "type": "boolean",
            "description": (
                "Set true only to interrupt an in-flight task: the user "
                "corrected or cancelled it, or the current task is plainly "
                "stuck. A task that was dispatched only a moment ago cannot be "
                "interrupted yet and returns 'busy' — wait and try again. "
                "Default false, which returns a 'busy' answer and leaves the "
                "in-flight task running."
            ),
        },
    },
    "required": ["text"],
}

MCBOT_TASK_DESCRIPTION = (
    "派发用户明确要求的 Minecraft 游戏任务：移动、跟随护卫、采集挖矿、制作、建筑等"
    "均将用户原话放入text，由游戏里的大脑规划执行，不需要每种行为单独入口。"
    "猫娘是执行角色，bot是她的游戏身体。只派发一次，立即返回实际受理编号；"
    "受理不是完成，关键节点和最终成果会主动推送，不能按入口成功宣称游戏目标完成。"
    "已有任务运行时默认保留，不重派；overwrite仅在用户明确要求改派或打断时true。"
    "查询进度选mcbot_status，停止选mcbot_stop，配置重载选mcbot_reload_config；"
    "进度、结果、反问及用户对反问的回答不是新任务，不能因这些通知重新派活。"
    "text使用最新用户原话，不翻译、不拆分或添加步骤、数量、坐标；不要传工具名。"
)

MCBOT_ASK_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "text": {
            "type": "string",
            "description": "A direct question for the companion.",
        },
    },
    "required": ["text"],
}

MCBOT_ASK_DESCRIPTION = (
    "Read current execution status and the latest observed progress without "
    "starting, interrupting or replacing a game task. Use this for 'how is it "
    "going?'. The returned timestamps matter: old receipts are observations, "
    "not current inventory or goal verification. If a requested fact is absent, "
    "say it is unknown. Never resend work to obtain a status update."
)

MCBOT_ANSWER_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "question_id": {
            "type": "string",
            "description": (
                "Optional. Leave it out: the open question is found for you. "
                "Only set it when the owner is answering a specific question "
                "you were told about by number."
            ),
        },
        "text": {
            "type": "string",
            "description": "The user's answer, in their own words.",
        },
    },
    "required": ["text"],
}

MCBOT_ANSWER_DESCRIPTION = (
    "Answer a question the companion asked through a question cue. Pass the "
    "user's answer as they gave it — do not answer on the user's behalf. Only "
    "call this when the user actually answered; if they did not, leave the "
    "question open. Several questions can be open at once; if you cannot tell "
    "which one the answer is for, ask the user first.\n\n"
    "An answer often sounds like a fresh instruction: to a question about "
    "whether to fight or scout, 'let's scout first' is the answer, not a new "
    "task. When you have relayed one of her questions and the user's next "
    "words answer it, call this instead of starting new work."
)

MCBOT_CANCEL_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "task_id": {
            "type": "integer",
            "description": (
                "Optional id of a specific task to cancel. Omit to cancel "
                "whatever is currently running."
            ),
        },
    },
}

MCBOT_CANCEL_DESCRIPTION = (
    "Tell the companion to stop what she is doing. Use it when the user says "
    "to stop or abort. New or redirected game work is dispatched through the "
    "host analyzer's mcbot_task entry, not by this stop tool."
)

def _recipient(plugin: Any) -> str | None:
    # The SDK facade wraps the host context; invocation identity lives on the
    # original context as a ContextVar, rather than in model-supplied arguments.
    context = getattr(plugin, "_host_ctx", None) or getattr(plugin, "ctx", None)
    value = getattr(context, "current_lanlan", None)
    return value if isinstance(value, str) and value else None


CONTROL_ENTRY_METADATA = {"result_kind": "event"}


@neko_plugin
class McbotBridgePlugin(NekoPluginBase):
    """Plugin facade — lifecycle wiring and the two tool surfaces.

    All game-facing behaviour lives in :class:`McbotService`; this class holds
    only SDK integration so the service stays testable without the plugin SDK.
    """

    def __init__(self, ctx):
        super().__init__(ctx)
        # Follow the global NEKO_LOG_LEVEL (default INFO) rather than pinning a
        # level, so DEBUG actually shows up when someone is diagnosing a bridge
        # that will not connect.
        _log_level = (os.environ.get("NEKO_LOG_LEVEL") or "INFO").strip().upper()
        try:
            self.file_logger = self.enable_file_logging(log_level=_log_level)
        except ValueError:
            self.file_logger = self.enable_file_logging(log_level="INFO")
        self.logger = self.file_logger

        self._cfg: Dict[str, Any] = {}
        self._lang: str = prompts.DEFAULT_LANG
        self._service = McbotService(
            logger=self.logger,
            push_message_fn=self.push_message,
        )
        # The SDK runs ``@lifecycle(id="startup")`` inside a transient
        # ``asyncio.run(...)`` (plugin/core/host.py), which closes the loop as
        # soon as the handler returns and cancels every ``asyncio.create_task``
        # started beneath it. Starting the bridge there would silently produce
        # a plugin that never streams. The real start is deferred to
        # ``_on_command_loop_start`` (the host's long-lived loop) with a lazy
        # first-call fallback, guarded so concurrent callers cannot double-spawn.
        self._service_lazy_started: bool = False
        self._service_start_lock: asyncio.Lock = asyncio.Lock()

    async def _ensure_service_started(self, *, connect_wait_s: float = 2.0) -> bool:
        """Idempotently start the bridge; returns whether it is streaming.

        A ``False`` return is not an error — it is the normal state when
        Minecraft is closed, since mcbot only serves its bridge while a world
        is loaded. Callers that need a live bridge let the dispatch itself
        produce the user-facing explanation.
        """
        async with self._service_start_lock:
            if not self._service_lazy_started:
                try:
                    await self._service.start()
                    self._service_lazy_started = True
                    self.logger.info("[lazy-start] bridge service started")
                except Exception as exc:
                    # Leave the flag clear so the next call retries.
                    self.logger.warning(
                        "[lazy-start] start failed; will retry — {}: {}",
                        type(exc).__name__, exc,
                    )
                    return False

            deadline = time.monotonic() + connect_wait_s
            while time.monotonic() < deadline:
                if self._service.is_connected():
                    return True
                await asyncio.sleep(0.05)
            return self._service.is_connected()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @lifecycle(id="startup")
    async def startup(self, **_):
        # Configuration only — see the note in __init__ for why the service
        # must not be started from this hook.
        cfg = await self.config.dump(timeout=5.0)
        cfg = cfg if isinstance(cfg, dict) else {}
        section = cfg.get("mcbot")
        self._cfg = section if isinstance(section, dict) else {}
        self._lang = prompts.user_lang()
        self._service.set_lang(self._lang)
        self._service.configure(self._cfg)
        return Ok({"status": "ready", "result": self._service.get_status()})

    @lifecycle(id="shutdown")
    async def shutdown(self, **_):
        if self._service_lazy_started:
            try:
                await self._service.stop()
            except Exception as exc:
                self.logger.warning(
                    "[shutdown] stop raised — {}: {}", type(exc).__name__, exc
                )
            self._service_lazy_started = False
        return Ok({"status": "shutdown"})

    async def _on_command_loop_start(self) -> None:
        """Eager-start the bridge on the host's long-lived command loop.

        The SDK calls this from inside the command dispatch loop, on the same
        loop that later runs every entry and tool handler, so the stream task,
        the locks and the events created here all bind to the loop that will
        use them. Starting lazily from a handler also works (both paths are
        kept), but eager start removes the first-call race entirely.
        """
        try:
            await self._ensure_service_started()
        except Exception as exc:
            self.logger.warning(
                "[eager-start] failed; lazy fallback remains — {}: {}",
                type(exc).__name__, exc,
            )

    # ------------------------------------------------------------------
    # Task dispatch belongs to the analyzer; dialog tools never start new work.
    # ------------------------------------------------------------------

    @plugin_entry(
        id="mcbot_task",
        name="派发 Minecraft 游戏任务",
        description=MCBOT_TASK_DESCRIPTION,
        input_schema=MCBOT_TASK_SCHEMA,
        metadata=CONTROL_ENTRY_METADATA,
        llm_result_fields=["summary", "accepted", "task_id"],
        timeout=300.0,
    )
    async def mcbot_task(self, *, text: Any = None, overwrite: Any = False, _ctx: Any = None, **_):
        # ``text`` is typed ``Any`` rather than ``str`` so a model that
        # violates the schema (omits it, sends null, sends a number) lands in
        # this body and gets a structured hint, instead of the SDK raising a
        # TypeError it would surface as a generic tool failure.
        # The analyzer forwards the owner's original utterance separately from
        # its generated arguments. Keep it intact when that context is present.
        original = _ctx.get("latest_user_request") if isinstance(_ctx, dict) else None
        if isinstance(original, str) and original.strip():
            text = original
        if not isinstance(text, str) or not text.strip():
            return Err(prompts.t("TASK_SCHEMA_ERROR", lang=self._lang))
        task_text = text.strip()
        if task_looks_like_identifier(task_text):
            return Err(prompts.t("TASK_IDENTIFIER_ERROR", lang=self._lang))

        await self._ensure_service_started()
        # A reverse question still awaiting an answer is the one thing that
        # turns the owner's next utterance into an answer rather than an
        # instruction. Nothing else in the model's context says so once the
        # question cue's turn has passed, so the hint rides on this tool's
        # result — the model is reading it either way, and a wrong dispatch is
        # exactly where the mistake becomes visible.
        hint = (
            prompts.t("TASK_OPEN_QUESTION_HINT", lang=self._lang)
            if self._service.has_open_question()
            else ""
        )
        result = await self._service.submit(
            task_text, overwrite=overwrite is True, target_lanlan=_recipient(self), return_receipt=True,
        )
        if result.get("is_error"):
            return Err(result["output"]["error"] + hint)
        result["summary"] += hint
        return await self._control_result(result)

    @llm_tool(
        name="mcbot_ask",
        description=MCBOT_ASK_DESCRIPTION,
        parameters=MCBOT_ASK_SCHEMA,
        # Observation uses only GET status and cached receipts.
        timeout=300.0,
    )
    async def mcbot_ask(self, *, text: Any = None, **_):
        if not isinstance(text, str) or not text.strip():
            return prompts.t("ASK_SCHEMA_ERROR", lang=self._lang)
        await self._ensure_service_started()
        return await self._service.ask(text.strip(), target_lanlan=_recipient(self))

    @llm_tool(
        name="mcbot_answer",
        description=MCBOT_ANSWER_DESCRIPTION,
        parameters=MCBOT_ANSWER_SCHEMA,
        timeout=30.0,
    )
    async def mcbot_answer(self, *, question_id: Any = None, text: Any = None, **_):
        if question_id is not None and (not isinstance(question_id, str) or not question_id.strip()):
            return prompts.t("ANSWER_SCHEMA_ERROR", lang=self._lang)
        if not isinstance(text, str) or not text.strip():
            return prompts.t("ANSWER_SCHEMA_ERROR", lang=self._lang)
        await self._ensure_service_started()
        qid = question_id.strip() if isinstance(question_id, str) else ""
        return await self._service.answer(qid, text.strip(), target_lanlan=_recipient(self))

    @llm_tool(
        name="mcbot_cancel",
        description=MCBOT_CANCEL_DESCRIPTION,
        parameters=MCBOT_CANCEL_SCHEMA,
        timeout=30.0,
    )
    async def mcbot_cancel(self, *, task_id: Any = None, **_):
        # Omitted or null means "everything"; an explicit value that is not a
        # task id is refused rather than widened into a cancel-all.
        if task_id is None:
            tid = None
        elif isinstance(task_id, int) and not isinstance(task_id, bool) and task_id >= 0:
            tid = task_id
        elif isinstance(task_id, str) and task_id.strip().isdigit():
            tid = int(task_id.strip())
        else:
            return prompts.t("CANCEL_ID_ERROR", lang=self._lang)
        await self._ensure_service_started()
        return await self._service.cancel(tid, target_lanlan=_recipient(self))


    # ------------------------------------------------------------------
    # @plugin_entry — analyzer / panel / CLI surface
    # ------------------------------------------------------------------

    @plugin_entry(
        id="mcbot_answer",
        name="回答 Minecraft 游戏中的待答问题",
        description=(
            "只在用户实际回答了游戏身体提出的有效问题时，按用户原话text回传答复。"
            "用于路径或攻击授权、选择等反问，不能替用户同意，也不能将答复派成新任务。"
            "question_id可省略，自动匹配当前角色唯一有效问题；多个问题时须明确编号或先询问。"
            "提交答复不是任务完成，之后继续等待真实游戏事件。"
        ),
        input_schema=MCBOT_ANSWER_SCHEMA,
        metadata=CONTROL_ENTRY_METADATA,
        llm_result_fields=["summary"],
        timeout=30.0,
    )
    async def mcbot_answer_entry(self, *, text: Any = None, question_id: Any = None, _ctx: Any = None, **_):
        original = _ctx.get("latest_user_request") if isinstance(_ctx, dict) else None
        if isinstance(original, str) and original.strip():
            text = original
        if (not isinstance(text, str) or not text.strip()
                or question_id is not None and (not isinstance(question_id, str) or not question_id.strip())):
            return Err(prompts.t("ANSWER_SCHEMA_ERROR", lang=self._lang))
        result = await McbotBridgePlugin.mcbot_answer(self, text=text, question_id=question_id)
        if isinstance(result, dict) and result.get("is_error"):
            return Err(result["output"]["error"])
        return await self._control_result({"summary": result, "answer_submitted": True, "goal_satisfied": False})

    async def _control_result(self, data: Dict[str, Any]):
        # A successful manager entry is not a completed Minecraft objective.
        # Only the executor's done event owns the result announcement.
        return await self.finish(
            data=data, delivery="silent", meta={"agent": {"result_kind": "event"}},
        )

    @ui.action(id="mcbot_status", label="刷新状态")
    @plugin_entry(
        id="mcbot_status",
        name="查询 mcbot 同伴状态",
        description=(
            "查询 mcbot 同伴的状态：身体（同伴）是否在线、她当前在做什么、有没有待答复的问题。"
            "这是管理查询，不执行采集、建造或其他游戏任务，也不证明游戏任务完成。"
            "用户要求游戏行动时选择 mcbot_task 入口，不要用状态查询代替派发。"
        ),
        metadata=CONTROL_ENTRY_METADATA,
        llm_result_fields=["summary"],
        input_schema={"type": "object", "properties": {}},
    )
    async def mcbot_status(self, **_):
        try:
            await self._ensure_service_started()
            live = await self._service.probe_status()
            status = self._service.get_status(target_lanlan=_recipient(self))
            connected = bool(status.get("connected"))
            if live:
                connected = True
            status["connected"] = connected

            label = prompts.t(
                "LABEL_CONNECTED" if connected else "LABEL_DISCONNECTED",
                lang=self._lang,
            )
            task = status.get("pending_task") or prompts.t("LABEL_IDLE", lang=self._lang)
            questions = status.get("pending_questions") or 0
            status["summary"] = prompts.t(
                "STATUS_SUMMARY",
                lang=self._lang,
                connected_label=label,
                task=task[:80],
                questions=questions,
            )
            if not connected and not status.get("token_ok"):
                # Only meaningful while we are failing to reach the bridge: a
                # live stream proves the token works, whatever the file says
                # now (it may have been rotated out from under us).
                status["summary"] += " " + prompts.t("NO_TOKEN_HINT", lang=self._lang)
            return await self._control_result(status)
        except Exception as exc:
            return Err(f"{type(exc).__name__}: {exc}")

    @ui.context(id="mcbot")
    async def mcbot_ui_context(self, **_):
        # Required by the hosted panel's props.api.call: without a provider for
        # its context id, the host refuses every action call from the surface.
        try:
            return {"status": self._service.get_status(target_lanlan=_recipient(self))}
        except Exception:
            return {"status": {}}

    @ui.action(id="mcbot_stop", label="停止任务")
    @plugin_entry(
        id="mcbot_stop",
        name="停止 mcbot 当前任务",
        description="仅提交取消当前任务。只在用户明确要求停止时使用；不是采集或建造入口，取消受理不证明目标完成或身体停稳。",
        metadata=CONTROL_ENTRY_METADATA,
        llm_result_fields=["summary"],
        input_schema={"type": "object", "properties": {}},
    )
    async def mcbot_stop(self, **_):
        try:
            await self._ensure_service_started()
            summary = await self._service.cancel(None, target_lanlan=_recipient(self))
            if isinstance(summary, dict) and summary.get("is_error"):
                return Err(summary["output"]["error"])
            return await self._control_result({
                "summary": summary, "result": self._service.get_status(target_lanlan=_recipient(self)),
            })
        except Exception as exc:
            return Err(f"{type(exc).__name__}: {exc}")

    @ui.action(id="mcbot_reload_config", label="重载配置")
    @plugin_entry(
        id="mcbot_reload_config",
        name="重载 mcbot 桥接配置",
        description=(
            "重新读取 plugin.toml [mcbot] 配置并重读 token 文件。base_url、"
            "game_dir、token_file、重连间隔或事件流开关有改动时会重建连接；"
            "纯超时类改动即时生效。仅用于明确修改配置后的管理操作；"
            "不派发或完成游戏任务，不能用于响应采集、挖矿、建造请求，也不是执行前必需步骤。"
        ),
        metadata=CONTROL_ENTRY_METADATA,
        llm_result_fields=["summary"],
        input_schema={"type": "object", "properties": {}},
    )
    async def mcbot_reload_config(self, **_):
        try:
            await self._ensure_service_started()
            cfg = await self.config.dump(timeout=5.0)
            cfg = cfg if isinstance(cfg, dict) else {}
            section = cfg.get("mcbot")
            self._cfg = section if isinstance(section, dict) else {}
            restarted = await self._service.reload_config_live(self._cfg)
            return await self._control_result({
                "summary": (
                    "Bridge configuration reloaded; transport restarted. No game task was dispatched or verified complete."
                    if restarted
                    else "Bridge configuration reloaded. No game task was dispatched or verified complete."
                ),
                "transport_restarted": restarted,
                "result": self._service.get_status(),
            })
        except Exception as exc:
            return Err(f"{type(exc).__name__}: {exc}")
