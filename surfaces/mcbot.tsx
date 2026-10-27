import {
  Alert,
  Button,
  ButtonGroup,
  Card,
  KeyValue,
  Page,
  Stack,
  StatusBadge,
  useEffect,
  useRef,
  useState,
} from "@neko/plugin-ui"
import type { PluginSurfaceProps, Tone } from "@neko/plugin-ui"

type LocaleKey = "zh-CN" | "en"

const STATUS_REFRESH_INTERVAL_MS = 5000

const COPY: Record<LocaleKey, {
  title: string
  refresh: string
  stop: string
  stopping: string
  connected: string
  disconnected: string
  companion: string
  companionUnknown: string
  task: string
  taskIdle: string
  questions: string
  questionsNone: string
  stopHint: string
  errorPrefix: string
  execution: string
  progress: string
  result: string
  delivery: string
  age: string
  unknown: string
}> = {
  "zh-CN": {
    title: "mcbot 同伴",
    refresh: "刷新",
    stop: "停止任务",
    stopping: "正在停止…",
    connected: "已连上",
    disconnected: "未连接",
    companion: "当前身体",
    companionUnknown: "未知",
    task: "当前任务",
    taskIdle: "空闲",
    questions: "待回答的问题",
    questionsNone: "无",
    stopHint: "停止会让同伴放下当前全部任务；她没接受时面板会提示。",
    errorPrefix: "出错：",
    execution: "执行状态",
    progress: "最近观察",
    result: "最近结果",
    delivery: "待送通知",
    age: "状态观察距今",
    unknown: "未知",
  },
  en: {
    title: "mcbot companion",
    refresh: "Refresh",
    stop: "Stop task",
    stopping: "Stopping…",
    connected: "Connected",
    disconnected: "Disconnected",
    companion: "Current body",
    companionUnknown: "unknown",
    task: "Current task",
    taskIdle: "idle",
    questions: "Open questions",
    questionsNone: "none",
    stopHint: "Stopping makes the companion drop everything she is doing; if she refuses, the panel says so.",
    errorPrefix: "Error: ",
    execution: "Execution",
    progress: "Last observation",
    result: "Last result",
    delivery: "Pending notices",
    age: "Status observed",
    unknown: "Unknown",
  },
}

function resolveLocale(locale: string | undefined): LocaleKey {
  return locale && locale.toLowerCase().startsWith("zh") ? "zh-CN" : "en"
}

function unwrapActionResult(envelope: any): Record<string, any> {
  let inner = envelope?.result ?? envelope?.data ?? envelope
  if (inner?.success === true && inner?.meta?.agent && inner.data && typeof inner.data === "object") {
    inner = inner.data
  }
  return inner && typeof inner === "object" ? inner : {}
}

type PanelState = {
  loading: boolean
  stopping: boolean
  connected: boolean | null
  companion: string
  task: string
  questions: number
  notice: string
  error: string
  taskState: string
  progress: string
  result: string
  notices: number
  age: number | null
}

export default function McbotCompanionPanel(props: PluginSurfaceProps) {
  const copy = COPY[resolveLocale(props.locale)]

  const [state, setState] = useState<PanelState>({
    loading: false,
    stopping: false,
    connected: null,
    companion: "",
    task: "",
    questions: 0,
    notice: "",
    error: "",
    taskState: "unknown",
    progress: "",
    result: "",
    notices: 0,
    age: null,
  })

  const refreshingRef = useRef(false)
  const unmountedRef = useRef(false)

  const refresh = async () => {
    if (refreshingRef.current || unmountedRef.current) return
    refreshingRef.current = true
    setState((prev) => ({ ...prev, loading: true, error: "" }))
    try {
      // Hosted surfaces run in a sandboxed iframe, so the only supported path
      // to the plugin is props.api.call, which needs the UI action below.
      const envelope = await props.api.call("mcbot_status")
      if (unmountedRef.current) return
      const data = unwrapActionResult(envelope)
      setState((prev) => ({
        ...prev,
        loading: false,
        connected: Boolean(data.connected),
        companion: String(data.companion || ""),
        task: String(data.pending_task || ""),
        questions: Number(data.pending_questions || 0),
        taskState: String(data.task_state || "unknown"),
        progress: String(data.latest_progress?.text || ""),
        result: String(data.recent_results?.at(-1)?.text || ""),
        notices: Number(data.notifications_pending || 0),
        age: typeof data.status_age_seconds === "number" ? data.status_age_seconds : null,
      }))
    } catch (exc: any) {
      if (unmountedRef.current) return
      const raw = String(exc?.message || exc)
      const code = String(exc?.code || "")
      const notRunning = code === "PLUGIN_NOT_RUNNING" || /not running|not started/i.test(raw)
      setState((prev) => ({
        ...prev,
        loading: false,
        connected: false,
        error: notRunning ? "" : copy.errorPrefix + raw,
      }))
    } finally {
      refreshingRef.current = false
    }
  }

  const stop = async () => {
    if (state.stopping) return
    setState((prev) => ({ ...prev, stopping: true, notice: "", error: "" }))
    try {
      const envelope = await props.api.call("mcbot_stop")
      const data = unwrapActionResult(envelope)
      setState((prev) => ({
        ...prev,
        stopping: false,
        notice: String(data.summary || ""),
      }))
      refresh()
    } catch (exc: any) {
      setState((prev) => ({
        ...prev,
        stopping: false,
        error: copy.errorPrefix + String(exc?.message || exc),
      }))
    }
  }

  useEffect(() => {
    refresh()
    const timer = window.setInterval(refresh, STATUS_REFRESH_INTERVAL_MS)
    return () => {
      unmountedRef.current = true
      window.clearInterval(timer)
    }
  }, [])

  const tone: Tone =
    state.connected === null ? "default" : state.connected ? "success" : "warning"
  const badgeText =
    state.connected === null
      ? copy.refresh
      : state.connected
        ? copy.connected
        : copy.disconnected

  const items = [
    { key: "companion", label: copy.companion, value: state.companion || copy.companionUnknown },
    { key: "task", label: copy.task, value: state.task || (state.taskState === "idle" ? copy.taskIdle : copy.unknown) },
    {
      key: "execution", label: copy.execution,
      value: (resolveLocale(props.locale) === "zh-CN"
        ? { running: "执行中", submitting: "正在提交", claimed: "准备提交", unknown: "结果未确认", idle: "空闲" }
        : { running: "Running", submitting: "Submitting", claimed: "Preparing", unknown: "Unconfirmed", idle: "Idle" }
      )[state.taskState as "running" | "submitting" | "claimed" | "unknown" | "idle"] || copy.unknown,
    },
    { key: "progress", label: copy.progress, value: state.progress || copy.unknown },
    { key: "result", label: copy.result, value: state.result || copy.unknown },
    { key: "delivery", label: copy.delivery, value: String(state.notices) },
    { key: "age", label: copy.age, value: state.age === null ? copy.unknown : `${state.age}s` },
    {
      key: "questions",
      label: copy.questions,
      value: state.questions ? String(state.questions) : copy.questionsNone,
    },
  ]

  return (
    <Page title={copy.title}>
      <Card>
        <Stack>
          <StatusBadge tone={tone}>{badgeText}</StatusBadge>
          {state.error ? <Alert tone="warning">{state.error}</Alert> : null}
          {state.notice ? <Alert tone="info">{state.notice}</Alert> : null}
          <KeyValue items={items} />
          <ButtonGroup>
            <Button onClick={refresh} disabled={state.loading}>
              {copy.refresh}
            </Button>
            <Button tone="danger" onClick={stop} disabled={state.stopping}>
              {state.stopping ? copy.stopping : copy.stop}
            </Button>
          </ButtonGroup>
        </Stack>
      </Card>
    </Page>
  )
}
