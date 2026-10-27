const path = require("node:path")
const assert = require("node:assert/strict")
const vm = require("node:vm")

const [typescriptRoot, hostRoot] = process.argv.slice(2)
if (!typescriptRoot || !hostRoot) {
  throw new Error("Usage: node tools/check-panel.cjs <typescript-package> <host-source>")
}
const ts = require(path.resolve(typescriptRoot, "lib/typescript.js"))
const host = path.resolve(hostRoot)
const panel = path.resolve(__dirname, "../surfaces/mcbot.tsx")
const declarations = path.join(host, "plugin/sdk/hosted-ui/globals.d.ts")
const program = ts.createProgram([panel, declarations], {
  target: ts.ScriptTarget.ES2020,
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler,
  jsx: ts.JsxEmit.React,
  jsxFactory: "h",
  jsxFragmentFactory: "Fragment",
  baseUrl: host,
  paths: { "@neko/plugin-ui": ["plugin/sdk/hosted-ui"] },
  noEmit: true,
  strict: false,
  skipLibCheck: true,
})
const diagnostics = ts.getPreEmitDiagnostics(program)
if (diagnostics.length) {
  console.error(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
    getCanonicalFileName: (file) => file,
    getCurrentDirectory: () => process.cwd(),
    getNewLine: () => "\n",
  }))
  process.exitCode = 1
} else {
  console.log("Panel type check passed against the host declarations.")
  const source = program.getSourceFile(panel)
  const unwrap = source.statements.find(
    (statement) => ts.isFunctionDeclaration(statement) && statement.name?.text === "unwrapActionResult",
  )
  assert.ok(unwrap, "Panel result decoder must exist")
  const code = ts.transpileModule(unwrap.getText(source), {
    compilerOptions: { target: ts.ScriptTarget.ES2020 },
  }).outputText
  const decode = vm.runInNewContext(code + "\nunwrapActionResult")
  const status = { connected: true, recent_results: [{ text: "verified outcome" }] }
  const finish = { success: true, data: status, meta: { agent: { delivery: "silent" } } }
  assert.equal(decode({ result: status }), status)
  assert.equal(decode(finish), status)
  assert.equal(decode({ result: finish }), status)
  assert.equal(decode({ data: finish }), status)
  const stopped = { summary: "cancel accepted", result: status }
  assert.equal(decode({ result: { ...finish, data: stopped } }), stopped)
  console.log("Panel accepts legacy and SDK finish responses without losing status or stop receipts.")
}
