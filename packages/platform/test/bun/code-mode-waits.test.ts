import { describe, expect, test } from "bun:test"
import { Context, Effect, Layer, Schema } from "effect"
import { Rpc } from "effect/unstable/rpc"
import { abortRequested, defineActor, Isolate, RuntimeError, type Journal, type Recorded } from "@clavia/tardigrade-core"
import { defineLibrary } from "@clavia/tardigrade-libraries"
import { codeMode } from "@clavia/tardigrade-agent/atoms/code-mode"
import { executions } from "@clavia/tardigrade-agent/atoms/durable/code-mode"
import { pendingTools } from "@clavia/tardigrade-agent/atoms/durable/tools"
import { ToolCatalog } from "@clavia/tardigrade-agent/actor/context"
import { codeModeActs } from "@clavia/tardigrade-agent/services/code-mode"
import { CodeParked, Event } from "@clavia/tardigrade-agent/contracts/code-mode"
import { requestTurn } from "@clavia/tardigrade-agent/contracts/events"
import { bunIsolate } from "@clavia/tardigrade-platform/bun"
import { codeModeJournals } from "../fixtures/code-mode-journals"
import { createTestStore } from "../properties/runtime/store"
import { harness, replayStates, summary, type Decision, type Step } from "../properties/code-mode-waits"

const run = <A, E>(work: Effect.Effect<A, E>) => Effect.runPromise(work.pipe(Effect.timeout(5_000)))
const policy = { default: "allow" as const, actions: { "package.execute": { resources: { "jobs.write": "ask" as const } } } }
const returned = (result: readonly unknown[]) => ({ output: JSON.stringify({ result, logs: [] }), error: null })

// settled runs steps to completion, settling each wait as it appears, and checks replay agreement.
function settled(steps: readonly Step[] | string, options: Parameters<typeof harness>[0] = {}) {
  return Effect.gen(function* () {
    const fixture = harness(options)
    const store = yield* fixture.open({ checkpoint: true })
    yield* Effect.gen(function* () {
      yield* store.send(fixture.call(steps))
      yield* store.wait
      for (let round = 0; round < 8 && !summary(fixture.records).returned; round++) {
        yield* fixture.settle()
        yield* store.wait
      }
    }).pipe(Effect.ensuring(store.close))
    const [full, restored] = yield* replayStates(fixture)
    expect(restored).toBe(full)
    return fixture
  })
}

describe("code mode waits", () => {
  test("a foreground body runs once without parking", () => run(Effect.gen(function* () {
    const fixture = yield* settled([{ method: "fg", value: 1 }, { all: [{ method: "fg", value: 2 }, { method: "fg", value: 3 }] }])
    expect(summary(fixture.records)).toEqual({ returned: returned([1, 2, 3]), requested: ["0:fg", "1:fg", "2:fg"], parked: 0 })
    expect(fixture.counts.runs).toBe(1)
  })))

  test("parks a background call, frees the isolate, and resumes from recorded calls", () => run(Effect.gen(function* () {
    const fixture = harness()
    const store = yield* fixture.open()
    yield* Effect.gen(function* () {
      yield* store.send(fixture.call([{ method: "fg", value: 1 }, { method: "bg", value: 2 }, { method: "fg", value: 3 }]))
      yield* store.wait
      expect(summary(fixture.records)).toEqual({ returned: undefined, requested: ["0:fg", "1:bg"], parked: 1 })
      expect(fixture.counts).toMatchObject({ runs: 1, active: 0, submitted: 1 })
      expect(store.recoveryState()).toMatchObject({ status: "parked" })
      expect(store.snapshot().deferred().map(work => work.request.act)).toEqual(["code-mode.package"])
      yield* fixture.settle()
      yield* store.wait
      expect(summary(fixture.records)).toEqual({ returned: returned([1, 3, 3]), requested: ["0:fg", "1:bg", "2:fg"], parked: 1 })
      expect(fixture.counts).toMatchObject({ runs: 2, active: 0, submitted: 1 })
      expect(store.getState().view.executions).toEqual([])
    }).pipe(Effect.ensuring(store.close))
  })))

  test("parks once per sequential wait and once for concurrent waits", () => run(Effect.gen(function* () {
    const sequential = yield* settled([{ method: "bg", value: 1 }, { method: "bg", value: 2 }])
    expect(summary(sequential.records)).toEqual({ returned: returned([2, 3]), requested: ["0:bg", "1:bg"], parked: 2 })
    expect(sequential.counts).toMatchObject({ runs: 3, submitted: 2 })
    const concurrent = yield* settled([{ all: [1, 2, 3, 4].map(value => ({ method: "bg" as const, value })) }, { method: "fg", value: 7 }])
    expect(summary(concurrent.records)).toEqual({ returned: returned([2, 3, 4, 5, 7]), requested: ["0:bg", "1:bg", "2:bg", "3:bg", "4:fg"], parked: 1 })
    expect(concurrent.counts).toMatchObject({ runs: 2, submitted: 4 })
    const asked = yield* settled([{ all: [1, 2, 3].map(value => ({ method: "write" as const, value })) }], { gated: true, policy })
    expect(summary(asked.records)).toEqual({ returned: returned([10, 20, 30]), requested: ["0:write", "1:write", "2:write"], parked: 1 })
  })))

  test("a policy update does not preempt an accepted ask", () => run(Effect.gen(function* () {
    const fixture = harness({ gated: true, policy, decide: () => "deny" })
    const store = yield* fixture.open()
    yield* Effect.gen(function* () {
      yield* store.send(fixture.call([{ method: "write", value: 2 }]))
      yield* store.wait
      yield* store.send([{ type: "PermissionUpdated", policy: { default: "allow", actions: {} } }])
      yield* store.wait
      expect(fixture.counts.writes).toBe(0)
      expect(store.snapshot().deferred().map(work => work.request.act)).toEqual(["agent.permission.request"])
      yield* fixture.settle()
      yield* store.wait
    }).pipe(Effect.ensuring(store.close))
    expect(summary(fixture.records).returned?.error).toContain("Reviewer: deny")
    expect(fixture.counts.writes).toBe(0)
    expect(fixture.records.filter(({ event }) => event.type === "PermissionResolved")).toHaveLength(1)
  })))

  test("a parked body survives a restart and a checkpoint, then resumes once", () => run(Effect.gen(function* () {
    const fixture = harness()
    let store = yield* fixture.open({ checkpoint: true })
    yield* store.send(fixture.call([{ method: "fg", value: 1 }, { method: "bg", value: 2 }])).pipe(Effect.andThen(store.wait), Effect.ensuring(store.close))
    const parked = fixture.records.length
    expect(fixture.checkpoint!.position).toBeLessThan(parked)
    const [full, restored] = yield* replayStates(fixture)
    expect(restored).toBe(full)
    store = yield* fixture.open({ checkpoint: true })
    yield* Effect.gen(function* () {
      yield* store.wait
      expect(fixture.records.length).toBe(parked)
      expect(fixture.counts).toMatchObject({ runs: 1, submitted: 1 })
      expect((yield* Effect.exit(store.checkpoint))._tag).toBe("Failure")
      yield* fixture.settle()
      yield* store.wait
    }).pipe(Effect.ensuring(store.close))
    expect(summary(fixture.records)).toEqual({ returned: returned([1, 3]), requested: ["0:fg", "1:bg"], parked: 1 })
    expect(fixture.counts).toMatchObject({ runs: 2, submitted: 1 })
  })))

  test("a checkpoint between settlement and the next attempt restores and resumes", () => run(Effect.gen(function* () {
    const live = yield* settled([{ method: "bg", value: 2 }, { method: "fg", value: 5 }])
    const cut = live.records.findIndex(({ event }) => event.type === "MethodReturned" && event.ordinal === 0) + 1
    const crash = harness()
    crash.records.push(...live.records.slice(0, cut))
    crash.body([{ method: "bg", value: 2 }, { method: "fg", value: 5 }])
    const prefix = yield* crash.open({ drive: false })
    yield* prefix.checkpoint.pipe(Effect.ensuring(prefix.close))
    expect(crash.checkpoint!.position).toBe(cut)
    const store = yield* crash.open()
    yield* store.wait.pipe(Effect.ensuring(store.close))
    expect(summary(crash.records)).toEqual(summary(live.records))
    expect(crash.counts).toMatchObject({ runs: 1, submitted: 0 })
    const [full, restored] = yield* replayStates(crash)
    expect(restored).toBe(full)
  })))

  test("a timed out background call rejects inside the body", () => run(Effect.gen(function* () {
    const fixture = yield* settled([{ method: "bg", value: 9, catch: true }, { method: "bg", value: 9 }])
    const { returned: result } = summary(fixture.records)
    expect(result?.error).toContain("PromiseTimedOut")
    expect(fixture.counts).toMatchObject({ runs: 3, submitted: 2 })
  })))

  test("a body that changes between attempts fails as nondeterministic", () => run(Effect.gen(function* () {
    let attempts = 0
    const fixture = yield* settled([{ method: "bg", value: 1 }], { drift: () => attempts++ > 0 })
    expect(summary(fixture.records).returned?.error).toContain("Nondeterministic code mode")
  })))

  test("asks once per gated call and executes it after approval", () => run(Effect.gen(function* () {
    const fixture = harness({ gated: true, policy })
    const store = yield* fixture.open()
    yield* Effect.gen(function* () {
      yield* store.send(fixture.call([{ method: "fg", value: 1 }, { method: "write", value: 2 }]))
      yield* store.wait
      expect(summary(fixture.records)).toEqual({ returned: undefined, requested: ["0:fg", "1:write"], parked: 1 })
      expect(fixture.counts).toMatchObject({ active: 0, writes: 0 })
      expect(store.snapshot().deferred().map(work => work.request.act)).toEqual(["agent.permission.request"])
      expect(store.snapshot().deferred()[0]!.request.input).toMatchObject({ action: "package.execute", resource: "jobs.write", requestId: JSON.stringify(["code", 1]), input: { value: 2 }, metadata: { readOnly: false } })
      yield* fixture.settle()
      yield* store.wait
    }).pipe(Effect.ensuring(store.close))
    expect(summary(fixture.records)).toEqual({ returned: returned([1, 20]), requested: ["0:fg", "1:write"], parked: 1 })
    expect(fixture.counts.writes).toBe(1)
    expect(fixture.records.filter(({ event }) => event.type === "PermissionResolved").map(({ event }) => event)).toEqual([
      { type: "PermissionResolved", action: "package.execute", requestId: JSON.stringify(["code", 0]), decision: { allowed: true, reason: "Permission policy: allow" } },
      { type: "PermissionResolved", action: "package.execute", requestId: JSON.stringify(["code", 1]), decision: { allowed: true, reason: "Reviewer: allow" } },
    ])
  })))

  test.each([["deny", "Reviewer: deny"], ["timeout", "PromiseTimedOut"]] as const)("a %s decision rejects the call inside the body", (decision: Decision, reason: string) => run(Effect.gen(function* () {
    const thrown = yield* settled([{ method: "write", value: 2 }, { method: "fg", value: 3 }], { gated: true, policy, decide: () => decision })
    expect(summary(thrown.records).returned?.error).toContain(reason)
    expect(thrown.counts.writes).toBe(0)
    const caught = yield* settled([{ method: "write", value: 2, catch: true }, { method: "fg", value: 3 }], { gated: true, policy, decide: () => decision })
    const result = JSON.parse(summary(caught.records).returned!.output) as { result: [{ error: string }, number] }
    expect(result.result[0].error).toContain(reason)
    expect(result.result[1]).toBe(3)
    expect(caught.counts.writes).toBe(0)
  })))

  test.each([["deny", "Reviewer: deny"], ["timeout", "Permission request failed: {\"_tag\":\"PromiseTimedOut\""]] as const)("with onDenied fail, a %s decision ends the evaluation", (decision: Decision, reason: string) => run(Effect.gen(function* () {
    const fixture = yield* settled([{ method: "fg", value: 1 }, { method: "write", value: 2, catch: true }, { method: "fg", value: 3 }], { gated: true, policy, decide: () => decision, onDenied: "fail" })
    const { returned: result } = summary(fixture.records)
    expect(result?.output).toBe("null")
    expect(result?.error).toStartWith(reason)
    expect(summary(fixture.records)).toMatchObject({ requested: ["0:fg", "1:write"], parked: 1 })
    expect(fixture.counts).toMatchObject({ runs: 1, writes: 0 })
    expect(fixture.records.some(({ event }) => event.type === "EffectRequested" && event.ref.act === "code-mode.package" && event.origin === fixture.records.findIndex(({ event }) => event.type === "MethodRequested" && event.ordinal === 1))).toBe(false)
  })))

  test("with onDenied fail, a policy denial cancels the live evaluation", () => run(Effect.gen(function* () {
    const deny = { default: "allow" as const, actions: { "package.execute": { resources: { "jobs.fg": "deny" as const } } } }
    const fixture = yield* settled([{ method: "write", value: 1 }, { method: "fg", value: 2, catch: true }, { method: "write", value: 3 }], { gated: true, policy: deny, onDenied: "fail" })
    expect(summary(fixture.records)).toEqual({ returned: { output: "null", error: "Permission policy: deny" }, requested: ["0:write", "1:fg"], parked: 0 })
    expect(fixture.counts).toMatchObject({ runs: 1, active: 0, writes: 1 })
    expect(fixture.records.flatMap(({ event }) => event.type === "EffectCancelled" ? [[event.ref.act, event.reason]] : [])).toEqual([["code-mode.evaluate", { _tag: "CodeDenied", reason: "Permission policy: deny" }]])
  })))

  test("with onDenied fail, a parked denial cancels the pending ask and the background call", () => run(Effect.gen(function* () {
    const fixture = harness({ gated: true, policy, decide: () => "deny", onDenied: "fail" })
    const store = yield* fixture.open({ checkpoint: true })
    yield* Effect.gen(function* () {
      yield* store.send(fixture.call([{ all: [{ method: "bg", value: 2 }, { method: "write", value: 3 }, { method: "write", value: 4 }] }, { method: "fg", value: 5 }]))
      yield* store.wait
      expect(summary(fixture.records)).toEqual({ returned: undefined, requested: ["0:bg", "1:write", "2:write"], parked: 1 })
      expect(store.snapshot().deferred().map(work => work.request.act).toSorted()).toEqual(["agent.permission.request", "agent.permission.request", "code-mode.package"])
      yield* fixture.settle(handle => handle.id.startsWith("permission:"))
      yield* store.wait
      expect(store.snapshot().deferred()).toEqual([])
      expect(store.getState().view.executions).toEqual([])
    }).pipe(Effect.ensuring(store.close))
    expect(summary(fixture.records)).toEqual({ returned: { output: "null", error: "Reviewer: deny" }, requested: ["0:bg", "1:write", "2:write"], parked: 1 })
    expect(fixture.counts).toMatchObject({ runs: 1, writes: 0, submitted: 1, cancelled: ["bg:2"] })
    const cancelled = fixture.records.flatMap(({ event }) => event.type === "EffectCancelled" && !Schema.is(CodeParked)(event.reason) ? [[event.ref.act, event.reason]] : [])
    expect(cancelled.toSorted()).toEqual([["agent.permission.request", { _tag: "CodeDenied", reason: "Reviewer: deny" }], ["code-mode.package", { _tag: "CodeDenied", reason: "Reviewer: deny" }]])
    const [full, restored] = yield* replayStates(fixture)
    expect(restored).toBe(full)
  })))

  test.each([undefined, "reject"] as const)("with onDenied %s, a denied call stays catchable inside the body", onDenied => run(Effect.gen(function* () {
    const caught = yield* settled([{ method: "write", value: 2, catch: true }, { method: "fg", value: 3 }], { gated: true, policy, decide: () => "deny", ...(onDenied ? { onDenied } : {}) })
    expect(summary(caught.records).returned).toEqual(returned([{ error: "Reviewer: deny" }, 3]))
    expect(caught.counts).toMatchObject({ runs: 2, writes: 0 })
  })))

  test("a body that throws on a resumed rejection is not reported as nondeterministic", () => run(Effect.gen(function* () {
    const fixture = yield* settled([{ all: [{ method: "write", value: 1 }, { method: "write", value: 2 }] }], { gated: true, policy, decide: () => "deny" })
    expect(summary(fixture.records)).toMatchObject({ requested: ["0:write", "1:write"], parked: 1 })
    expect(summary(fixture.records).returned?.error).toBe("Reviewer: deny")
  })))

  test("policy decisions answer without asking or parking", () => run(Effect.gen(function* () {
    const deny = { default: "allow" as const, actions: { "package.execute": { resources: { "jobs.write": "deny" as const } } } }
    const fixture = yield* settled([{ method: "write", value: 2, catch: true }, { method: "write", value: 3 }], { gated: true, policy: deny })
    expect(summary(fixture.records)).toMatchObject({ requested: ["0:write", "1:write"], parked: 0 })
    expect(summary(fixture.records).returned?.error).toContain("Permission policy: deny")
    expect(fixture.counts).toMatchObject({ runs: 1, writes: 0 })
    expect(fixture.records.some(({ event }) => event.type === "EffectRequested" && event.ref.act === "agent.permission.request")).toBe(false)
  })))

  test.each(["bg", "write"] as const)("Stop while parked on %s settles the turn and leaves no code state", method => run(Effect.gen(function* () {
    const fixture = harness({ turns: true, policy })
    const store = yield* fixture.openTurns()
    yield* Effect.gen(function* () {
      fixture.body([{ method: "fg", value: 1 }, { method, value: 2 }])
      yield* store.send([{ type: "PermissionConfigured", policy }, requestTurn({ text: "go", turnId: "first", invocationRef: { method: "turn", id: "first" } })])
      yield* store.wait
      expect(summary(fixture.records).parked).toBe(1)
      const stop = fixture.records.length
      yield* store.send([abortRequested({ ref: { method: "turn", id: "first" }, reason: "stop" })])
      yield* store.wait
      const after = fixture.records.slice(stop).map(({ event }) => event)
      expect(after.filter(event => event.type === "EffectRequested")).toEqual([])
      expect(after.filter(event => event.type === "EffectCancelled").map(event => event.type === "EffectCancelled" && event.ref.act)).toEqual([method === "bg" ? "code-mode.package" : "agent.permission.request"])
      expect(after.filter(event => event.type === "TurnSettled")).toMatchObject([{ outcome: "cancelled", reason: "stop" }])
      expect(fixture.counts.cancelled).toEqual(method === "bg" ? ["bg:2"] : [])
      expect(store.get(pendingTools).pending).toBeNull()
      expect(store.snapshot().deferred()).toEqual([])
      const [full, restored] = yield* replayStates(fixture)
      expect(JSON.parse(full).executions).toEqual([])
      expect(restored).toBe(full)
      yield* store.send([requestTurn({ text: "next", turnId: "second", invocationRef: { method: "turn", id: "second" } })])
      yield* store.wait
      expect(fixture.records.filter(({ event }) => event.type === "TurnSettled").map(({ event }) => event.type === "TurnSettled" && event.outcome)).toEqual(["cancelled", "completed"])
    }).pipe(Effect.ensuring(store.close))
  })))

  test("a real Bun body catches a denied call and awaits concurrent background calls", () => run(Effect.gen(function* () {
    const code = [
      "const first = await jobs.fg({ value: 1 })",
      "let denied = null",
      "try { await jobs.write({ value: 2 }) } catch (error) { denied = String(error) }",
      "const [a, b] = await Promise.all([jobs.bg({ value: 3 }), jobs.bg({ value: 4 })])",
      "return { first, denied, a, b, now: Date.now(), random: Math.random() }",
    ].join("\n")
    const fixture = yield* settled(code, { isolate: bunIsolate(), gated: true, policy, decide: () => "deny" })
    const { returned: result, requested, parked } = summary(fixture.records)
    expect(result?.error).toBeNull()
    const value = (JSON.parse(result!.output) as { result: { first: number; denied: string; a: number; b: number; now: number; random: number } }).result
    expect(value).toMatchObject({ first: 1, a: 4, b: 5 })
    expect(value.denied).toContain("Reviewer: deny")
    const called = fixture.records.find(({ event }) => event.type === "CodeCalled")?.event
    expect(called?.type === "CodeCalled" && value.now).toBe(called?.type === "CodeCalled" ? called.ambient.at : -1)
    expect(requested.toSorted()).toEqual(["0:fg", "1:write", "2:bg", "3:bg"])
    expect(parked).toBeGreaterThanOrEqual(2)
    expect(fixture.counts).toMatchObject({ writes: 0, submitted: 2 })
  })))

  test.each(["completed", "cancelled"] as const)("a %s journal recorded before parked waits replays and continues unchanged", scenario => run(Effect.gen(function* () {
    const recorded = codeModeJournals[scenario]
    const library = defineLibrary({ name: "jobs", description: "Jobs", methods: [Rpc.make("fg", { payload: Schema.Struct({ value: Schema.Finite }), success: Schema.Finite, error: Schema.String })] })
      .implement({ fg: ({ value }) => Effect.succeed(value) })
    const isolate = Layer.succeed(Isolate, { run: (input, onCall) => Effect.forEach(JSON.parse(input.code) as number[], (value, ordinal) => onCall({ ordinal, package: "jobs", method: "fg", input: { value } })).pipe(Effect.map(result => ({ result, logs: [] }))) })
    const actor = defineActor("code-waits", Effect.map(codeMode({ name: "code" }), node => ({ atom: node, schema: Event })))
    const open = (records: Recorded<typeof Event.Type>[], stored: (typeof recorded.checkpoints)[number] | undefined, inspect: boolean) => {
      const journal: Journal<typeof Event.Type> = {
        read: Effect.sync(() => [...records]), readAfter: position => Effect.sync(() => records.slice(position)), appendWithCheckpoint: () => Effect.fail(new RuntimeError("Unexpected checkpoint")),
        readCheckpoint: Effect.succeed(stored && { position: stored.position, digest: stored.digest, payload: new Uint8Array(Buffer.from(stored.payload, "base64")) }),
        append: (position, events) => position === records.length ? Effect.sync(() => { records.push(...events) }) : Effect.fail(new RuntimeError("Unexpected journal position")),
      }
      return createTestStore({ actor, journal, inspect, actorContext: Context.pick(ToolCatalog), services: () => codeModeActs([library]).pipe(Layer.provideMerge(isolate)) })
    }
    const events = recorded.events as unknown as readonly (typeof Event.Type)[]
    for (const stored of [undefined, ...recorded.checkpoints]) {
      const store = yield* open(events.map(event => ({ event })), stored, true)
      expect(store.get(executions)).toEqual([])
      expect(store.get(pendingTools).pending).toBeNull()
      yield* store.close
    }
    if (scenario === "cancelled") return
    for (const stored of recorded.checkpoints) {
      const records = events.slice(0, stored.position).map(event => ({ event }))
      const store = yield* open(records, stored, false)
      yield* store.wait.pipe(Effect.ensuring(store.close))
      expect(records.map(({ event }) => event.type)).toEqual(events.map(event => event.type))
      expect(records.at(-1)?.event).toEqual(events.at(-1)!)
    }
  })))
})
