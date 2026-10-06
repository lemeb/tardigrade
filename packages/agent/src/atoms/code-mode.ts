import { Effect, Option, Schema } from "effect"
import { Cancelled, cancel, effectAtom, eventValue, RuntimeError, type ActRequest, type ActService, type CancelRequest, type EventValue } from "@clavia/tardigrade-core"
import { type AskPermission, failureMessage } from "../contracts/acts"
import { pendingTools } from "./durable/tools"
import { permissionState } from "./durable/permissions"
import { DEFAULT_PERMISSION_POLICY, permissionMode } from "./permission-request"
import { type LibrarySource } from "@clavia/tardigrade-libraries/types"
import { codeModeSpec, selectLibraries } from "../contracts/libraries"
import { ToolCatalog } from "../actor/context"
import { ToolReturned, type Decision, type PermissionResolved, type ToolCalled } from "../contracts/events"
import { CodeDenied, CodeModeName, CodeParked, CodeReturned, Event, EvaluateCode, ExecutePackage, MethodReturned } from "../contracts/code-mode"
import { executions as state } from "./durable/code-mode"

const CodeInput = Schema.Struct({ code: Schema.String })

// codeMode observes model tool calls and proposes sandbox evaluation, package execution, and tool results.
// A background call or a call awaiting an asked permission parks the evaluation; the body runs again once every call settles.
export const DEFAULT_CODE_MODE_NAME = "agent.code-mode"
// permissions requests a decision for each package call as action "package.execute" on resource "library.method".
// onDenied "reject" (default) rejects a denied or failed decision inside the body; "fail" ends the evaluation, so the execute call fails with the decision reason.
export interface CodeModeOptions<P = never> {
  readonly name?: string
  readonly signatureDepth?: number
  readonly permissions?: (request: Parameters<typeof AskPermission.request>[0]) => ActRequest<typeof Decision.Type, string, P>
  readonly onDenied?: "reject" | "fail"
}
export function codeMode<P = never>(libraries: readonly LibrarySource[], options?: CodeModeOptions<P>): ReturnType<typeof createCodeMode<P>>
export function codeMode<P = never>(options?: CodeModeOptions<P>): ReturnType<typeof createCodeMode<P>>
export function codeMode<P>(librariesOrOptions: readonly LibrarySource[] | CodeModeOptions<P> = {}, options: CodeModeOptions<P> = {}) {
  return Array.isArray(librariesOrOptions) ? createCodeMode(options, librariesOrOptions) : createCodeMode(librariesOrOptions as CodeModeOptions<P>)
}

function createCodeMode<P>(options: CodeModeOptions<P>, sources?: readonly LibrarySource[]) {
  const name = Schema.decodeSync(CodeModeName)(options.name ?? DEFAULT_CODE_MODE_NAME)
  return Effect.gen(function* () {
    const catalog = yield* ToolCatalog
    const libraries = sources ? selectLibraries(sources, catalog.libraries ?? []) : undefined
    const spec = libraries ? codeModeSpec(libraries, options.signatureDepth) : catalog.specs.find(spec => spec.name === "execute")
    if (!spec) return yield* Effect.fail(new RuntimeError("Code mode requires an execute tool in ToolCatalog"))
    const evaluations = new Map<string, ReturnType<typeof EvaluateCode.request>>()
    const packages = new Map<string, ReturnType<typeof ExecutePackage.request>>()
    const asks = new Map<string, ActRequest<typeof Decision.Type, string, P>>()
    const methods = new Map((libraries ?? catalog.libraries ?? []).flatMap(library => library.specs.map(spec => [JSON.stringify([library.name, spec.method]), spec] as const)))
    let current: string | undefined
    return effectAtom(get => {
      const executions = get(state)
      // Reading the decisions on every evaluation keeps them in each checkpoint.
      const permission = options.permissions ? get(permissionState) : undefined
      const queue = get(pendingTools)
      const call = queue.pending
      if (current !== call?.callId) {
        evaluations.clear()
        packages.clear()
        asks.clear()
        current = call?.callId
      }
      const events: Record<string, EventValue<typeof Event.Type>> = {}
      const acts: Record<string, ActRequest<Schema.Json, string, ActService<"code-mode.evaluate"> | ActService<"code-mode.package"> | P> | CancelRequest> = {}
      const view = { specs: [spec], system: spec.description, executions }
      if (!call) return { view, events, acts }
      const parsed = Schema.decodeUnknownOption(CodeInput, { onExcessProperty: "error" })(call.input)
      const error = call.name !== "execute" ? `Unknown tool: ${call.name}` : Option.isNone(parsed) ? "execute requires { code: string }" : undefined
      if (!queue.running) {
        events.called = eventValue({ type: "ToolCalled", callId: call.callId, codeMode: name, counted: error === undefined } satisfies ToolCalled)
        return { view, events, acts }
      }
      const execution = executions.find(entry => entry.call.callId === call.callId)
      if (!execution) throw new RuntimeError("No code mode state for pending tool call")
      if (error || execution.outcome) {
        const outcome = execution.outcome
        const value = outcome?.status === "fulfilled" ? outcome.value : null
        const bodyError = Schema.is(Schema.Struct({ error: Schema.String }))(value) ? value.error : undefined
        events.returned = eventValue({ type: "ToolReturned", callId: call.callId, output: JSON.stringify(value),
          error: error ?? (outcome?.status === "rejected" ? outcome.reason : bodyError) ?? null,
        } satisfies ToolReturned)
      } else if (Option.isSome(parsed)) {
        const open = execution.calls.filter(call => call.outcome === null)
        if (execution.evaluation !== null || !open.length) {
          const key = JSON.stringify([call.callId, execution.origin])
          let request = evaluations.get(key)
          if (!request) {
            request = EvaluateCode.request({
              origin: execution.origin!, input: { codeMode: name, callId: call.callId, code: parsed.value.code, ...(libraries ? { libraries: libraries.map(library => library.name) } : {}) },
              onSettled: outcome => outcome.status === "rejected" && Schema.is(Cancelled)(outcome.reason) && Schema.is(CodeParked)(outcome.reason.reason) ? []
                : outcome.status === "rejected" && Schema.is(Cancelled)(outcome.reason) && Schema.is(CodeDenied)(outcome.reason.reason) ? [{ type: "CodeReturned", codeMode: name, callId: call.callId, outcome: { status: "rejected", reason: outcome.reason.reason.reason } } satisfies typeof CodeReturned.Type]
                : [{ type: "CodeReturned", codeMode: name, callId: call.callId, outcome: outcome.status === "rejected" ? { ...outcome, reason: failureMessage(outcome.reason) } : outcome } satisfies typeof CodeReturned.Type],
            })
            evaluations.set(key, request)
          }
          acts[name] = request
        }
        const decided = (ordinal: number) => permission?.decisions.find(value => value.action === "package.execute" && value.requestId === JSON.stringify([call.callId, ordinal]))?.decision
        // With onDenied "fail", the earliest recorded denial ends the evaluation; the denials that cancelled asks record later, so the reason stays stable.
        const unaccepted = new Set(open.flatMap(packageCall => packageCall.ref === null ? [JSON.stringify([call.callId, packageCall.ordinal])] : []))
        const denied = options.onDenied === "fail" ? permission?.decisions.find(value => value.action === "package.execute" && !value.decision.allowed && unaccepted.has(value.requestId)
          && permission.decisions.find(other => other.action === "package.execute" && other.requestId === value.requestId) === value) : undefined
        if (denied) {
          // Accepted asks and parked package calls are cancelled before the code result, since the tool result withdraws these proposals.
          const reason = CodeDenied.make({ reason: denied.decision.reason })
          for (const packageCall of open) {
            const ask = asks.get(JSON.stringify([call.callId, packageCall.ordinal]))
            const ref = packageCall.ref ?? (ask && !decided(packageCall.ordinal) ? get(ask.ref) : undefined)
            // A live evaluation forwards its cancellation to accepted package calls.
            if (ref && (packageCall.ref === null || execution.evaluation === null)) acts[`${name}.denied.${packageCall.ordinal}`] = cancel(ref, reason)
          }
          if (execution.evaluation !== null) acts[`${name}.denied`] = cancel(execution.evaluation, reason)
          else events.denied = eventValue({ type: "CodeReturned", codeMode: name, callId: call.callId, outcome: { status: "rejected", reason: reason.reason } } satisfies typeof CodeReturned.Type)
          return { view, events, acts }
        }
        let waiting = false
        if (open.length) {
          for (const packageCall of open) {
            const spec = methods.get(JSON.stringify([packageCall.package, packageCall.method]))
            const requestId = JSON.stringify([call.callId, packageCall.ordinal])
            // The first decision governs the call, so a late answer cannot change an accepted request.
            const decision = decided(packageCall.ordinal)
            if (permission && options.permissions && packageCall.ref === null && !decision) {
              const resource = `${packageCall.package}.${packageCall.method}`
              const metadata = spec?.annotations?.readOnlyHint === undefined ? undefined : { readOnly: spec.annotations.readOnlyHint }
              const pending = asks.get(requestId)
              const mode = pending && get(pending.ref) ? "ask" : permissionMode(permission.policy ?? DEFAULT_PERMISSION_POLICY, "package.execute", resource, metadata?.readOnly)
              if (mode !== "ask") {
                events[`${name}.permission.${packageCall.ordinal}`] = eventValue({ type: "PermissionResolved", action: "package.execute", requestId, decision: { allowed: mode === "allow", reason: `Permission policy: ${mode}` } } satisfies PermissionResolved)
                continue
              }
              let request = asks.get(requestId)
              if (!request) {
                request = options.permissions({
                  origin: packageCall.origin, input: { action: "package.execute", requestId, resource, input: packageCall.input, ...(metadata ? { metadata } : {}) },
                  onSettled: result => [{ type: "PermissionResolved", action: "package.execute", requestId, decision: result.status === "fulfilled"
                    ? result.value : { allowed: false, reason: `Permission request failed: ${failureMessage(result.reason)}` } } satisfies PermissionResolved],
                })
                asks.set(requestId, request)
              }
              acts[`${name}.permission.${packageCall.ordinal}`] = request
              if (get(request.ref)) waiting = true
              continue
            }
            const key = JSON.stringify([call.callId, packageCall.ordinal])
            let request = packages.get(key)
            if (!request) {
              request = ExecutePackage.request({
                origin: packageCall.origin, input: { codeMode: name, callId: call.callId, ordinal: packageCall.ordinal, package: packageCall.package, method: packageCall.method, input: packageCall.input, ...(decision?.allowed === false ? { error: decision.reason } : {}) },
                onSettled: (outcome, ref) => [{ type: "MethodReturned", codeMode: name, callId: call.callId, ordinal: packageCall.ordinal, ref, outcome: outcome.status === "rejected" ? { ...outcome, reason: failureMessage(outcome.reason) } : outcome } satisfies typeof MethodReturned.Type],
              })
              packages.set(key, request)
            }
            acts[`${name}.package.${packageCall.ordinal}`] = request
            if (spec?.execution === "background" && decision?.allowed !== false && packageCall.ref !== null) waiting = true
          }
        }
        if (waiting && execution.evaluation !== null) acts[`${name}.parked`] = cancel(execution.evaluation, CodeParked.make({}))
      }
      return { view, events, acts }
    })
  })
}
