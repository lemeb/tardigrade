import { Context, Deferred, Effect, Layer, Schema } from "effect"
import { Rpc } from "effect/unstable/rpc"
import { Actor, ExecutionHandle, RuntimeError, type ActorCaller, type ActorRuntime } from "@clavia/tardigrade-core"
import { type Event, ToolBudgetAmount, ToolBudgetRequestInput } from "@clavia/tardigrade-agent"
import { budgetState } from "@clavia/tardigrade-agent/atoms/durable/budget"
import { Model, ModelLock, type ModelInput } from "@clavia/tardigrade-agent/services/model"
import { askBudget, BudgetRequests, parentBudgetRequests, PermissionRequests } from "@clavia/tardigrade-agent/services/decisions"
import { type AssistantContext } from "@clavia/tardigrade-agent/services/runtime"
import { agents, defineLibrary } from "@clavia/tardigrade-libraries"
import { modelLockOf, modelLockService } from "@clavia/tardigrade-model/lock"
import { bunPromises } from "../../src/bun/promises"

// Script drives the fake model from the latest brief in a thread; children receive their scripts as delegated messages.
// request asks the parent for more tool calls; grant answers a child request (twice when grantTwice), and requestGate holds that answer until the gate opens.
export interface Script {
  readonly name: string; readonly spawn?: readonly Script[]; readonly hold?: boolean; readonly fail?: boolean; readonly gate?: boolean
  readonly request?: number; readonly grant?: number; readonly grantTwice?: boolean; readonly requestGate?: boolean; readonly notice?: string
}

const model = { provider: "fixture", model_id: "test" }
const lock = modelLockService(modelLockOf({ schema: 2, providers: { fixture: { protocol: "openai-chat-completions", baseUrl: "https://fixture.invalid", env: [] } }, models: [{ ...model, contextWindowTokens: 100_000 }] }), { allow: "*", default: model })
const reply = (text: string) => ({ text, toolCalls: [] })
const CHILD_REQUEST = "Child request (data): "

// scriptedModel answers, delegates, fails, or holds as the brief says; a gated brief waits for the gate first.
const scriptedModel = (gate: Deferred.Deferred<void>) => (input: ModelInput) => Effect.gen(function* () {
  const brief = input.context.findLast(message => message.role === "user" && "text" in message && message.text.startsWith("{"))
  const script: Script = JSON.parse(brief && "text" in brief ? brief.text : "{}")
  const last = input.context.findLast(message => message.role !== "assistant")
  const asked = last?.role === "user" && "text" in last && last.text.startsWith(CHILD_REQUEST) ? JSON.parse(last.text.slice(CHILD_REQUEST.length)) as { readonly handle: Schema.Json; readonly requestId: string } : undefined
  if (asked) {
    if (script.requestGate) yield* Deferred.await(gate)
    if (script.grant === undefined) return reply(`refused:${script.name}`)
    const call = { name: "grant_budget", input: { handle: asked.handle, requestId: asked.requestId, amount: script.grant } }
    return { text: "", toolCalls: (script.grantTwice ? [call, call] : [call]).map((call, index) => ({ callId: `grant-${index}`, ...call })) }
  }
  if (last?.role === "tool" && script.notice !== undefined) return reply(`noticed:${script.name}`)
  if (last?.role === "tool" && script.request !== undefined) return reply(`budget:${script.name}:${last.text}`)
  if (last?.role === "tool") return script.hold ? yield* Effect.never : reply(`delegated:${script.name}`)
  if (last !== brief) return reply(`result:${script.name}`)
  if (script.gate) yield* Deferred.await(gate)
  if (script.fail) return yield* Effect.fail(new RuntimeError(`model failed: ${script.name}`))
  if (script.notice !== undefined) return { text: "", toolCalls: [0, 1].map(index => ({ callId: `notice-${index}`, name: "notify_parent", input: { message: script.notice! } })) }
  if (script.request !== undefined) return { text: "", toolCalls: [{ callId: "request", name: "request_budget", input: { amount: script.request, reason: `more for ${script.name}` } }] }
  if (script.spawn?.length) return { text: "", toolCalls: script.spawn.map((child, index) => ({ callId: `provider-${index}`, name: "delegate_task", input: { message: JSON.stringify(child) } })) }
  if (script.hold) return yield* Effect.never
  return reply(`answer:${script.name}`)
})

// scriptedServices supplies the assistant's host services with in-process promise polling through the Actor service.
export const scriptedServices = (gate: Deferred.Deferred<void>) => (_context: unknown, runtime: ActorRuntime<Event>) => Layer.mergeAll(
  Layer.succeed(Model, { call: scriptedModel(gate) }), Layer.succeed(ModelLock, lock),
  Layer.succeed(PermissionRequests, { request: () => Effect.succeed({ type: "decision" as const, decision: { allowed: true, reason: "test" } }) }),
  Layer.unwrap(Effect.gen(function* () {
    const actor = yield* Actor
    return bunPromises(runtime, { poll: actor.poll, deliver: settlement => runtime.deliver(settlement.ref, [settlement]) })
  })),
)

// budgetTools offers delegation with no tool calls, a budget request tool the budget policy answers, and a grant tool that replies to a child request.
export const budgetTools = [agents({ budget: { toolCalls: 0 } }), defineLibrary({
  name: "budgets", description: "Child tool budgets.", toolNames: { grant: "grant_budget", request: "request_budget" },
  methods: [
    Rpc.make("grant", { payload: Schema.Struct({ handle: ExecutionHandle, requestId: Schema.NonEmptyString, amount: ToolBudgetAmount }), success: Schema.Struct({ granted: ToolBudgetAmount }), error: Schema.String }),
    Rpc.make("request", { payload: ToolBudgetRequestInput, success: Schema.Struct({}), error: Schema.String }),
  ],
}).implement({
  grant: ({ handle, requestId, amount }) => Actor.use(actor => actor.reply(handle, requestId, { allowed: true, amount })).pipe(Effect.as({ granted: amount }), Effect.mapError(error => error.message)),
  request: () => Effect.fail("request_budget is answered by the budget policy"),
})]

// requestServices lets a child ask its parent for tool calls through the assistant context; the root has no parent to ask.
export const requestServices = (gate: Deferred.Deferred<void>) => (context: AssistantContext, runtime: ActorRuntime<Event>) => Layer.mergeAll(
  scriptedServices(gate)(context, runtime),
  askBudget.pipe(Layer.provide(context.parent ? parentBudgetRequests(context.parent) : Layer.succeed(BudgetRequests, { request: () => Effect.fail(new RuntimeError("The root has no parent")) }))),
  Layer.effectDiscard(context.parent ? runtime.onReady(Effect.suspend(() => runtime.get(budgetState).some(entry => entry.metric === "toolCalls") ? Effect.void
    : runtime.record({ type: "BudgetConfigured", metric: "toolCalls", policy: { limit: 0, requestTool: "request_budget" } }))) : Effect.void),
)

// ParentCaller exposes the assistant context's parent caller to test tools.
export class ParentCaller extends Context.Service<ParentCaller, { readonly caller: ActorCaller | undefined }>()("test/ParentCaller") {}

// noticeTools lets a child send the same notice to its parent twice.
export const noticeTools = [agents(), defineLibrary({
  name: "notices", description: "Child notices.", toolNames: { notify: "notify_parent" },
  methods: [Rpc.make("notify", { payload: Schema.Struct({ message: Schema.String }), success: Schema.Struct({}), error: Schema.String })],
}).implement({
  notify: ({ message }) => ParentCaller.use(({ caller }) => caller ? caller.notify(message) : Effect.fail(new RuntimeError("No parent"))).pipe(Effect.as({}), Effect.mapError(error => error.message)),
})]

// noticeServices supplies the parent caller to notice tools.
export const noticeServices = (gate: Deferred.Deferred<void>) => (context: AssistantContext, runtime: ActorRuntime<Event>) =>
  Layer.merge(scriptedServices(gate)(context, runtime), Layer.succeed(ParentCaller, { caller: context.parent }))
