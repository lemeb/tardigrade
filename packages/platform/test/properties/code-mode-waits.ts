import { isDeepStrictEqual } from "node:util"
import { Context, Effect, Layer, Schema } from "effect"
import { Rpc } from "effect/unstable/rpc"
import * as fc from "fast-check"
import { atom, defineActor, durablePromise, type Atom, effectKey, ExecutionHandle, Isolate, Promises, RuntimeError, type IsolateCall, type IsolateInput, type EffectRef, type Journal, type Recorded, type StoredCheckpoint } from "@clavia/tardigrade-core"
import { defineLibrary, MethodExecution, MethodHints } from "@clavia/tardigrade-libraries"
import { codeMode } from "@clavia/tardigrade-agent/atoms/code-mode"
import { infer } from "@clavia/tardigrade-agent/atoms/infer"
import { executions } from "@clavia/tardigrade-agent/atoms/durable/code-mode"
import { permissionState } from "@clavia/tardigrade-agent/atoms/durable/permissions"
import { ToolCatalog, ModelInfo } from "@clavia/tardigrade-agent/actor/context"
import { codeModeActs } from "@clavia/tardigrade-agent/services/code-mode"
import { AskPermission, Generate, Summarize } from "@clavia/tardigrade-agent/contracts/acts"
import { CodeDenied, CodeParked, Event } from "@clavia/tardigrade-agent/contracts/code-mode"
import { type PermissionPolicy } from "@clavia/tardigrade-agent/contracts/events"
import { createTestStore } from "./runtime/store"

// Step is one package call of a scripted body; all runs its calls concurrently and catch keeps a rejection as data. Background value 9 times out.
export type Step = { readonly method: "fg" | "bg" | "write"; readonly value: number; readonly catch?: boolean } | { readonly all: readonly Step[] }
export type Decision = "allow" | "deny" | "timeout"
type Events = typeof Event.Type

const Value = Schema.Struct({ value: Schema.Finite })
const jobs = defineLibrary({ name: "jobs", description: "Jobs", methods: [
  Rpc.make("fg", { payload: Value, success: Schema.Finite, error: Schema.String }).annotate(MethodHints, { readOnlyHint: true }),
  Rpc.make("bg", { payload: Value, success: ExecutionHandle, error: Schema.String }).annotate(MethodExecution, "background"),
  Rpc.make("write", { payload: Value, success: Schema.Finite, error: Schema.String }).annotate(MethodHints, { readOnlyHint: false }),
] })

// harness records commit batches, runs scripted bodies, and settles remote handles on request or on observation.
export function harness(options: { readonly gated?: boolean; readonly turns?: boolean; readonly isolate?: Layer.Layer<Isolate>; readonly policy?: typeof PermissionPolicy.Type; readonly auto?: boolean; readonly decide?: (resource: string) => Decision; readonly drift?: () => boolean; readonly onDenied?: "reject" | "fail" } = {}) {
  const records: Recorded<Events>[] = []
  const boundaries: number[] = [0]
  let checkpoint: StoredCheckpoint | undefined
  const journal: Journal<Events> = {
    read: Effect.sync(() => [...records]), readAfter: position => Effect.sync(() => records.slice(position)),
    readCheckpoint: Effect.sync(() => checkpoint),
    append: (position, events) => Effect.sync(() => {
      if (position !== records.length) throw new RuntimeError("Unexpected journal position")
      records.push(...events)
      boundaries.push(records.length)
    }),
    appendWithCheckpoint: (position, events, value) => Effect.sync(() => {
      if (position !== records.length) throw new RuntimeError("Unexpected checkpoint position")
      records.push(...events)
      if (events.length) boundaries.push(records.length)
      checkpoint = value
    }),
  }
  const counts = { runs: 0, active: 0, submitted: 0, writes: 0, cancelled: [] as string[] }
  const watched = new Map<string, { readonly ref: EffectRef; readonly handle: ExecutionHandle }>()
  let send: (events: readonly Events[]) => Effect.Effect<void, Error> = () => Effect.fail(new RuntimeError("Store is not open"))
  const settlement = (ref: EffectRef, handle: ExecutionHandle) => {
    const promise = durablePromise(ref, { success: Schema.Json, error: Schema.String })
    const background = handle.id.startsWith("bg:")
    const decision = background ? handle.id === "bg:9" ? "timeout" : "allow" : options.decide?.(handle.id.slice("permission:".length)) ?? "allow"
    if (decision === "timeout") return { type: "PromiseSettled" as const, ref, result: { status: "rejected" as const, reason: { _tag: "PromiseTimedOut" as const, deadlineAt: 1 } } }
    return background ? promise.succeed(Number(handle.id.slice(3)) + 1) : promise.succeed({ allowed: decision === "allow", reason: `Reviewer: ${decision}` })
  }
  const library = jobs.implement({
    fg: ({ value }) => Effect.succeed(value),
    bg: ({ value }) => Effect.sync(() => { counts.submitted++; return { executor: "remote", id: `bg:${value}` } }),
    write: ({ value }) => Effect.sync(() => { counts.writes++; return value * 10 }),
  }, { submit: ["bg"], cancel: { bg: handle => Effect.sync(() => { counts.cancelled.push(handle?.id ?? "") }) } })
  const isolate = Layer.succeed(Isolate, {
    run: <Services>(input: IsolateInput, onCall: (call: IsolateCall) => Effect.Effect<Schema.Json, string, Services>) => {
      let ordinal = 0
      const run = (steps: readonly Step[], log: unknown[]): Effect.Effect<void, string, Services> => Effect.forEach(steps, step => {
        if ("all" in step) return Effect.forEach(step.all, inner => run([inner], log), { concurrency: "unbounded", discard: true })
        const call = { ordinal: ordinal++, package: "jobs", method: options.drift?.() ? "fg" : step.method, input: { value: step.value } }
        return onCall(call).pipe(
          Effect.map(value => { log.push(value) }),
          Effect.catch(reason => step.catch ? Effect.sync(() => { log.push({ error: reason }) }) : Effect.fail(reason)),
        )
      }, { discard: true })
      return Effect.suspend(() => {
        counts.runs++
        counts.active++
        const log: unknown[] = []
        return run(JSON.parse(input.code) as Step[], log).pipe(
          Effect.match({ onFailure: error => ({ result: log as Schema.Json, logs: [], error }), onSuccess: () => ({ result: log as Schema.Json, logs: [] }) }),
          Effect.ensuring(Effect.sync(() => { counts.active-- })),
        )
      })
    },
  })
  const promises = Layer.succeed(Promises, {
    watch: registration => Effect.gen(function* () {
      watched.set(effectKey(registration.ref), { ref: registration.ref, handle: registration.handle })
      if (options.auto) yield* send([settlement(registration.ref, registration.handle)])
    }),
    cancel: () => Effect.void,
  })
  let code = "[]"
  let generated = 0
  const model = { provider: "openrouter" as const, model_id: "test" }
  const services = Layer.mergeAll(codeModeActs([library]).pipe(Layer.provideMerge(options.isolate ?? isolate)), promises,
    AskPermission.layer(input => Effect.succeed(AskPermission.defer({ executor: "remote", id: `permission:${input.resource}` }))),
    Layer.succeed(ModelInfo, { model, contextWindowTokens: 1_000_000 }),
    Generate.layer(() => Effect.sync(() => generated++ === 0 ? { text: "", toolCalls: [{ callId: "code", name: "execute", input: { code } }] } : { text: "done", toolCalls: [] })),
    Summarize.layer(() => Effect.succeed({ text: "summary", toolCalls: [] })))
  const gated = defineActor("code-waits", Effect.map(codeMode({ name: "code", permissions: AskPermission.request, ...(options.onDenied ? { onDenied: options.onDenied } : {}) }), node => ({ atom: node, schema: Event })))
  const plain = defineActor("code-waits", Effect.map(codeMode({ name: "code" }), node => ({ atom: node, schema: Event })))
  const turns = defineActor("code-waits-turns", Effect.gen(function* () {
    const tools = yield* codeMode({ name: "code", permissions: AskPermission.request, ...(options.onDenied ? { onDenied: options.onDenied } : {}) })
    const node = yield* infer(atom(get => ({ system: "", tools: get(tools), context: { view: { position: "ready" as const, messages: [] }, events: {}, acts: {} } })))
    return { atom: node, schema: Event }
  }))
  type Extra = { readonly checkpoint?: boolean; readonly drive?: boolean; readonly inspect?: boolean; readonly journal?: Journal<Events> }
  const settings = (extra: Extra) => ({
    journal: extra.journal ?? journal, promiseDelivery: { retryIntervalMs: 1 }, services: () => services, inspect: extra.inspect === true,
    checkpoint: extra.checkpoint ? { mode: "threshold" as const, options: { everyEvents: 1 } } : { mode: "manual" as const },
    ...(extra.drive === false ? { canDrive: Effect.succeed(false) } : {}),
  })
  const bind = <Store extends { readonly send: (events: readonly Events[]) => Effect.Effect<void, Error> }>(store: Store) => { send = store.send; return store }
  // open drives a bare code mode actor; openTurns drives code mode under agent turns.
  const open = (extra: Extra = {}) => (options.gated
    ? createTestStore({ ...settings(extra), actor: gated, actorContext: Context.pick(ToolCatalog) })
    : createTestStore({ ...settings(extra), actor: plain, actorContext: Context.pick(ToolCatalog) })).pipe(Effect.map(bind))
  const openTurns = (extra: Extra = {}) => createTestStore({ ...settings(extra), actor: turns, actorContext: Context.pick(ModelInfo, ToolCatalog) }).pipe(Effect.map(bind))
  const state = (store: { readonly get: <Value>(node: Atom<Value>) => Value; readonly close: Effect.Effect<void> }) =>
    Effect.sync(() => JSON.stringify({ executions: store.get(executions), permissions: store.get(permissionState) })).pipe(Effect.ensuring(store.close))
  return {
    records, boundaries, counts, open, openTurns,
    // inspect replays a journal without driving it and reads the code and permission atoms.
    inspect: (replayed: Journal<Events>) => options.turns ? openTurns({ journal: replayed, inspect: true }).pipe(Effect.flatMap(state)) : open({ journal: replayed, inspect: true }).pipe(Effect.flatMap(state)),
    body: (steps: readonly Step[] | string) => { code = typeof steps === "string" ? steps : JSON.stringify(steps) },
    // call configures the policy and delivers a model reply whose execute call runs steps.
    call: (steps: readonly Step[] | string): readonly Events[] => {
      code = typeof steps === "string" ? steps : JSON.stringify(steps)
      return [...(options.policy && (options.gated || options.turns) ? [{ type: "PermissionConfigured" as const, policy: options.policy }] : []),
        { type: "ModelReturned", purpose: "inference", callId: "model", text: "", toolCalls: [{ callId: "code", providerId: "provider", name: "execute", input: { code } }] }]
    },
    // settle answers each observed handle whose promise is still pending in the journal, or the first one select accepts.
    settle: (select?: (handle: ExecutionHandle) => boolean) => Effect.suspend(() => {
      const settled = new Set(records.flatMap(({ event }) => event.type === "PromiseSettled" || event.type === "EffectCancelled" ? [effectKey(event.ref)] : []))
      const pending = [...watched.values()].filter(({ ref }) => !settled.has(effectKey(ref)))
      return Effect.forEach(select ? pending.filter(({ handle }) => select(handle)).slice(0, 1) : pending, ({ ref, handle }) => send([settlement(ref, handle)]), { discard: true })
    }),
    get checkpoint() { return checkpoint },
  }
}

// replayStates reads the code and permission atoms after a full replay and after checkpoint plus suffix of the same records.
export function replayStates(fixture: Pick<ReturnType<typeof harness>, "inspect" | "records" | "checkpoint">) {
  const records = [...fixture.records]
  const journal = (stored: StoredCheckpoint | undefined): Journal<Events> => ({
    read: Effect.succeed(records), readAfter: position => Effect.succeed(records.slice(position)), readCheckpoint: Effect.succeed(stored),
    append: () => Effect.fail(new RuntimeError("Replay is read-only")), appendWithCheckpoint: () => Effect.fail(new RuntimeError("Replay is read-only")),
  })
  return Effect.all([fixture.inspect(journal(undefined)), fixture.inspect(journal(fixture.checkpoint))])
}

// summary reads the final tool result, every requested package call, and the number of parked attempts.
export function summary(records: readonly Recorded<Events>[]) {
  const events = records.map(record => record.event)
  const returned = events.find(event => event.type === "ToolReturned" && event.callId === "code")
  return {
    returned: returned?.type === "ToolReturned" ? { output: returned.output, error: returned.error } : undefined,
    requested: events.flatMap(event => event.type === "MethodRequested" ? [`${event.ordinal}:${event.method}`] : []),
    parked: events.filter(event => event.type === "EffectCancelled" && Schema.is(CodeParked)(event.reason)).length,
  }
}

// agrees compares code state exactly and decisions per request, since concurrent calls may be decided in another order or not reached.
const agrees = (left: string, right: string) => {
  type State = { readonly executions: unknown; readonly permissions: { readonly decisions: readonly { readonly requestId: string; readonly decision: unknown }[] } }
  const [a, b] = [JSON.parse(left) as State, JSON.parse(right) as State]
  return isDeepStrictEqual(a.executions, b.executions) && a.permissions.decisions.every(value => b.permissions.decisions.every(other => other.requestId !== value.requestId || isDeepStrictEqual(other, value)))
}

// endedByDenial checks onDenied "fail": no denied call is executed, and a denial recorded first is the tool error with no output.
function endedByDenial(records: readonly Recorded<Events>[]): string | undefined {
  const events = records.map(record => record.event)
  const returned = events.find(event => event.type === "ToolReturned" && event.callId === "code")
  if (returned?.type !== "ToolReturned") return "No tool result"
  const decisions = new Map<string, { readonly allowed: boolean; readonly reason: string }>()
  for (const event of events) if (event.type === "PermissionResolved" && !decisions.has(event.requestId)) decisions.set(event.requestId, event.decision)
  const denied = new Set(events.flatMap((event, position) => event.type === "MethodRequested" && decisions.get(JSON.stringify([event.callId, event.ordinal]))?.allowed === false ? [position] : []))
  if (events.some(event => event.type === "EffectRequested" && event.ref.act === "code-mode.package" && denied.has(event.origin ?? -1))) return "A denied call was executed"
  const result = events.findIndex(event => event.type === "CodeReturned")
  const code = events[result]
  const first = events.slice(0, result).find(event => event.type === "PermissionResolved" && !event.decision.allowed && decisions.get(event.requestId) === event.decision)
  // A body may settle on its own failure before the denial cancels it.
  if (first?.type === "PermissionResolved" && code?.type === "CodeReturned" && code.outcome.status === "rejected" && (returned.error !== first.decision.reason || returned.output !== "null")) return "The earliest denial is not the tool error"
  const cancelled = events.filter(event => event.type === "EffectCancelled" && Schema.is(CodeDenied)(event.reason))
  if (cancelled.some(event => event.type === "EffectCancelled" && !isDeepStrictEqual(event.reason, { _tag: "CodeDenied", reason: returned.error }))) return "A denial cancellation differs from the tool error"
  return undefined
}

const step: fc.Arbitrary<Step> = fc.record({ method: fc.constantFrom("fg" as const, "bg" as const, "write" as const), value: fc.integer({ min: 0, max: 9 }), catch: fc.boolean() })
const body = fc.array(fc.oneof({ weight: 3, arbitrary: step }, { weight: 1, arbitrary: fc.array(step, { minLength: 2, maxLength: 3 }).map(all => ({ all })) }), { minLength: 1, maxLength: 4 })

// codeModeWaitRecovery reopens at every commit boundary, by full replay or checkpoint plus suffix, and requires the live result and calls.
export const codeModeWaitRecovery = fc.asyncProperty(fc.record({ steps: body, gated: fc.boolean(), fail: fc.boolean(), denyFg: fc.boolean(), decision: fc.constantFrom<Decision>("allow", "deny", "timeout"), checkpoint: fc.boolean() }), options => Effect.runPromise(Effect.gen(function* () {
  const configure = { gated: options.gated, auto: true, decide: () => options.decision, onDenied: options.fail ? "fail" as const : "reject" as const, policy: { default: "allow" as const, actions: { "package.execute": { resources: { "jobs.write": "ask" as const, ...(options.denyFg ? { "jobs.fg": "deny" as const } : {}) } } } } }
  const live = harness(configure)
  const store = yield* live.open({ checkpoint: options.checkpoint })
  yield* store.send(live.call(options.steps)).pipe(Effect.andThen(store.wait), Effect.ensuring(store.close))
  const expected = summary(live.records)
  if (!expected.returned) return yield* Effect.fail(new RuntimeError("Live code call did not return"))
  if (new Set(expected.requested).size !== expected.requested.length) return yield* Effect.fail(new RuntimeError("A package call was requested twice"))
  const invalid = options.fail ? endedByDenial(live.records) : undefined
  if (invalid) return yield* Effect.fail(new RuntimeError(`${invalid}: ${JSON.stringify(expected)}`))
  const [full, restored] = yield* replayStates(live)
  if (full !== restored) return yield* Effect.fail(new RuntimeError(`Checkpoint plus suffix differs from full replay: ${full} / ${restored}`))
  const delivered = live.records.findIndex(({ event }) => event.type === "ModelReturned") + 1
  for (const boundary of live.boundaries.filter(boundary => boundary >= delivered && boundary < live.records.length)) {
    const crash = harness(configure)
    crash.records.push(...live.records.slice(0, boundary))
    crash.body(options.steps)
    if (options.checkpoint) {
      const prefix = yield* crash.open({ drive: false })
      yield* prefix.checkpoint.pipe(Effect.ignore, Effect.ensuring(prefix.close))
    }
    const reopened = yield* crash.open({ checkpoint: options.checkpoint })
    yield* reopened.wait.pipe(Effect.ensuring(reopened.close))
    const recovered = summary(crash.records)
    // Concurrent calls a rejected body never awaited may be recorded in one interleaving and not another.
    const consistent = recovered.requested.every(call => !expected.requested.some(other => other.split(":")[0] === call.split(":")[0] && other !== call))
    // With onDenied "fail", a run that ends before its result is recorded may end on another concurrent failure; both must be failures that obey the denial rule.
    const same = isDeepStrictEqual(recovered.returned, expected.returned)
    const raced = !same && options.fail && expected.returned.error !== null && typeof recovered.returned?.error === "string"
    const invalid = options.fail ? endedByDenial(crash.records) : undefined
    if (invalid) return yield* Effect.fail(new RuntimeError(`Recovery at ${boundary}: ${invalid}`))
    if (!(raced || same) || !consistent || new Set(recovered.requested).size !== recovered.requested.length) return yield* Effect.fail(new RuntimeError(`Recovery at ${boundary} diverged: ${JSON.stringify(recovered)} from ${JSON.stringify(expected)}`))
    const [replayed, suffix] = yield* replayStates(crash)
    if (replayed !== suffix || !(raced || agrees(replayed, full))) return yield* Effect.fail(new RuntimeError(`Replay after recovery at ${boundary} diverged`))
  }
}).pipe(Effect.scoped, Effect.timeout(20_000))))
