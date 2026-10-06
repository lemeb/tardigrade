import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Context, Effect, Layer, Schema } from "effect"
import * as fc from "fast-check"
import { Actor, actorMethod, defineActor, durableAtom, effectAtom, event, threadActors, type ActorCall, type ThreadCoordinate } from "@clavia/tardigrade-core"
import { createBunHost } from "../../src/bun"
import { waitFor } from "../fixtures/wait"
import { RUNTIME_PROPERTY_OPTIONS } from "../properties/runtime/config"

const Received = event({ type: "Received", id: Schema.String, input: Schema.Json })
const Released = event({ type: "Released", id: Schema.String, output: Schema.Json })
const Aborted = event({ type: "Aborted", id: Schema.String, reason: Schema.String })
const Entry = Schema.Struct({ id: Schema.String, input: Schema.Json, output: Schema.optionalKey(Schema.Json), aborted: Schema.optionalKey(Schema.String) })
const calls = durableAtom({ name: "probe.calls", input: Schema.Union([Received, Released, Aborted]), schema: Schema.Array(Entry), initial: [],
  reduce: (state, event) => event.type === "Received" ? [...state, { id: event.id, input: event.input }]
    : state.map(entry => entry.id !== event.id ? entry : event.type === "Released" ? { ...entry, output: event.output } : { ...entry, aborted: event.reason }),
})
const probe = defineActor("probe", Effect.succeed({
  atom: effectAtom(get => ({ view: get(calls), events: {}, acts: {} })),
  methods: {
    message: actorMethod({ inputSchema: Schema.Json, outputSchema: Schema.Json, onReceive: Received.from((input, context) => ({ id: context.id, input })),
      result: (_, get, context) => {
        const entry = get(calls).find(entry => entry.id === context.id)
        return entry?.aborted !== undefined ? { status: "cancelled", reason: entry.aborted } : entry?.output !== undefined ? { status: "completed", output: entry.output } : undefined
      },
      onCancel: Aborted.from((_, context) => ({ id: context.id, reason: context.reason })),
    }),
    release: actorMethod({ inputSchema: Schema.Struct({ id: Schema.String, output: Schema.Json }), outputSchema: Schema.Null, onReceive: Released.from(input => input), result: () => ({ status: "completed", output: null }) }),
  },
}))

type Host = Awaited<ReturnType<typeof open>>
const call = (id: string, input: Schema.Json = { task: id }): ActorCall => ({ id, target: { actor: "probe", instance: "ignored", thread: "ignored" }, method: "message", input })

function open(storage: string, maxDepth: number, actors: Map<string, typeof Actor.Service>) {
  return Effect.runPromise(createBunHost({ actor: probe, storage, actorContext: Context.pick(), services: (coordinate, runtime) => Layer.effectDiscard(Effect.gen(function* () {
    actors.set(coordinate.thread, yield* Actor)
  })).pipe(Layer.provide(threadActors({ runtime, method: "message", maxDepth }))) }))
}

async function withHost<Value>(storage: string, maxDepth: number, use: (host: Host, actors: Map<string, typeof Actor.Service>) => Promise<Value>) {
  const actors = new Map<string, typeof Actor.Service>()
  const host = await open(storage, maxDepth, actors)
  try { return await use(host, actors) } finally { await Effect.runPromise(host.close) }
}

async function fixture(name: string, use: (storage: string) => Promise<void>) {
  const storage = await mkdtemp(join(tmpdir(), `tardie-${name}-`))
  try { await use(storage) } finally { await rm(storage, { recursive: true, force: true }) }
}

const directory = async (host: Host) => (await Effect.runPromise(host.supervisorStore("main"))).threads.get()
const records = async (host: Host, coordinate: ThreadCoordinate) => {
  const thread = await Effect.runPromise(host.getThread({ instance: coordinate.instance, thread: coordinate.thread }))
  if (!thread) throw new Error(`Unknown thread: ${coordinate.thread}`)
  return Effect.runPromise(thread.records())
}
const endpoint = (handle: { readonly endpoint?: string }): ThreadCoordinate => JSON.parse(handle.endpoint!)

test("invocation allocates one child per call identity across repetition and host restart", () => fixture("thread-actors-once", async storage => {
  const first = await withHost(storage, 1, async (host, actors) => {
    await Effect.runPromise(host.allocateRootThread({ instance: "main", name: "root" }))
    const handle = await Effect.runPromise(actors.get("root")!.invoke(call("a")))
    expect(await Effect.runPromise(actors.get("root")!.invoke(call("a")))).toEqual(handle)
    await expect(Effect.runPromise(actors.get("root")!.invoke(call("a", { task: "other" })))).rejects.toThrow("Message identity reused")
    return handle
  })
  await withHost(storage, 1, async (host, actors) => {
    await Effect.runPromise(host.recover({ actor: "probe", instance: "main", thread: "root" }))
    expect(await Effect.runPromise(actors.get("root")!.invoke(call("a")))).toEqual(first)
    const threads = await directory(host)
    expect(threads.map(entry => [entry.parent, entry.depth, entry.status])).toEqual([[null, 0, "registered"], ["root", 1, "registered"]])
    const child = endpoint(first)
    expect(threads[1]!.coordinate).toEqual(child)
    const received = (await records(host, child)).filter(record => record.message?.id === "a")
    expect(received).toHaveLength(1)
    expect(received[0]!.message).toMatchObject({ from: { actor: "probe", instance: "main", thread: "root" }, invocation: { method: "message", input: { task: "a" } } })
  })
}))

test("poll observes the durable child reply once and keeps it across caller restart", () => fixture("thread-actors-reply", async storage => {
  const handle = await withHost(storage, 1, async (host, actors) => {
    const root = await Effect.runPromise(host.allocateRootThread({ instance: "main", name: "root" }))
    const handle = await Effect.runPromise(actors.get("root")!.invoke(call("a")))
    expect(await Effect.runPromise(actors.get("root")!.poll(handle))).toEqual({ status: "pending" })
    const child = await Effect.runPromise(host.getThread({ instance: "main", thread: endpoint(handle).thread }))
    await Effect.runPromise(child!.invoke("release", { id: "a", output: { answer: 42 } }, { id: "release-a" }))
    await waitFor(() => Effect.runPromise(actors.get("root")!.poll(handle)), state => state.status !== "pending")
    expect(await Effect.runPromise(actors.get("root")!.poll(handle))).toEqual({ status: "fulfilled", value: { answer: 42 } })
    const replies = (await Effect.runPromise(root.records())).filter(record => record.message?.inReplyTo === "a")
    expect(replies).toHaveLength(1)
    expect(replies[0]!.message!.from).toEqual(endpoint(handle))
    return handle
  })
  await withHost(storage, 1, async (host, actors) => {
    await Effect.runPromise(host.recover({ actor: "probe", instance: "main", thread: "root" }))
    expect(await Effect.runPromise(actors.get("root")!.poll(handle))).toEqual({ status: "fulfilled", value: { answer: 42 } })
  })
}))

test("cancellation reaches the child by handle, by fence before invocation, and by identity after restart", () => fixture("thread-actors-cancel", async storage => {
  const retained = await withHost(storage, 1, async (host, actors) => {
    await Effect.runPromise(host.allocateRootThread({ instance: "main", name: "root" }))
    const root = actors.get("root")!
    const handle = await Effect.runPromise(root.invoke(call("a")))
    await Effect.runPromise(root.cancel(handle))
    await waitFor(() => Effect.runPromise(root.poll(handle)), state => state.status !== "pending")
    expect(await Effect.runPromise(root.poll(handle))).toEqual({ status: "rejected", reason: "Actor call cancelled" })
    await Effect.runPromise(root.cancel({ executor: "actor", id: "b" }))
    await expect(Effect.runPromise(root.invoke(call("b")))).rejects.toThrow("Actor call cancelled")
    expect(await directory(host)).toHaveLength(2)
    return Effect.runPromise(root.invoke(call("c")))
  })
  await withHost(storage, 1, async (host, actors) => {
    await Effect.runPromise(host.recover({ actor: "probe", instance: "main", thread: "root" }))
    await Effect.runPromise(host.recover({ actor: "probe", instance: "main", thread: endpoint(retained).thread }))
    const root = actors.get("root")!
    await Effect.runPromise(root.cancel({ executor: "actor", id: "c" }))
    await waitFor(() => Effect.runPromise(root.poll(retained)), state => state.status !== "pending")
    expect(await Effect.runPromise(root.poll(retained))).toEqual({ status: "rejected", reason: "Actor call cancelled" })
    await Effect.runPromise(root.cancel({ executor: "actor", id: "never-invoked" }))
    expect(await directory(host)).toHaveLength(3)
  })
}))

test("depth, actor, and handle checks precede allocation", () => fixture("thread-actors-limits", async storage => {
  await withHost(storage, 1, async (host, actors) => {
    await Effect.runPromise(host.allocateRootThread({ instance: "main", name: "root" }))
    const root = actors.get("root")!
    await expect(Effect.runPromise(root.invoke({ ...call("x"), target: { actor: "other", instance: "main", thread: "x" } }))).rejects.toThrow("Unknown actor: other")
    await expect(Effect.runPromise(root.invoke({ ...call("x"), method: "release" }))).rejects.toThrow("Unknown actor method: release")
    const handle = await Effect.runPromise(root.invoke(call("a")))
    const child = actors.get(endpoint(handle).thread)!
    await expect(Effect.runPromise(child.invoke(call("grandchild")))).rejects.toThrow("Child depth limit reached: 1")
    expect(await directory(host)).toHaveLength(2)
    expect(await Effect.runPromise(root.poll({ executor: "actor", id: "a", endpoint: "1:2:3:4" }))).toEqual({ status: "rejected", reason: "Local actor handle is no longer available" })
    await Effect.runPromise(root.cancel({ executor: "actor", id: "a", endpoint: "1:2:3:4" }))
    await expect(Effect.runPromise(root.poll({ executor: "actor", id: "a", endpoint: JSON.stringify({ actor: "probe", instance: "elsewhere", thread: "x" }) }))).rejects.toThrow("another instance")
    await expect(Effect.runPromise(root.reply(handle, "request", null))).rejects.toThrow("No matching pending actor request")
  })
  await fixture("thread-actors-leaf", leaf => withHost(leaf, 0, async (host, actors) => {
    await Effect.runPromise(host.allocateRootThread({ instance: "main", name: "root" }))
    await expect(Effect.runPromise(actors.get("root")!.invoke(call("a")))).rejects.toThrow("Child depth limit reached: 0")
    expect(await directory(host)).toHaveLength(1)
  }))
}))

const Step = fc.oneof(
  fc.record({ op: fc.constant("invoke" as const), id: fc.constantFrom("a", "b") }),
  fc.record({ op: fc.constant("release" as const), id: fc.constantFrom("a", "b") }),
  fc.record({ op: fc.constant("cancel" as const), id: fc.constantFrom("a", "b"), retained: fc.boolean() }),
  fc.record({ op: fc.constant("poll" as const), id: fc.constantFrom("a", "b") }),
  fc.record({ op: fc.constant("restart" as const) }),
)

// threadActorHistories interleaves invocation, child completion, cancellation, polling, and host restarts; each call keeps one child, one invocation, at most one reply, and a stable terminal result.
test("thread actor histories keep one child and one terminal result per call", () => fc.assert(fc.asyncProperty(fc.array(Step, { maxLength: 10 }), steps => fixture("thread-actors-history", async storage => {
  const actors = new Map<string, typeof Actor.Service>()
  let host = await open(storage, 1, actors)
  const handles = new Map<string, Awaited<ReturnType<typeof Effect.runPromise<import("@clavia/tardigrade-core").ExecutionHandle, Error>>>>()
  const fenced = new Set<string>()
  const live = new Set<string>()
  const terminal = new Map<string, unknown>()
  const observe = async (id: string) => {
    const state = await Effect.runPromise(actors.get("root")!.poll(handles.get(id)!))
    if (terminal.has(id)) expect(state).toEqual(terminal.get(id) as never)
    else if (state.status !== "pending") terminal.set(id, state)
    return state
  }
  try {
    await Effect.runPromise(host.allocateRootThread({ instance: "main", name: "root" }))
    for (const step of steps) {
      if (step.op === "restart") {
        await Effect.runPromise(host.close)
        actors.clear()
        fenced.clear()
        live.clear()
        host = await open(storage, 1, actors)
        await Effect.runPromise(host.recover({ actor: "probe", instance: "main", thread: "root" }))
        for (const handle of handles.values()) await Effect.runPromise(host.recover(endpoint(handle)))
      } else if (step.op === "invoke") {
        const result = await Effect.runPromise(Effect.result(actors.get("root")!.invoke(call(step.id))))
        if (fenced.has(step.id)) expect(result._tag).toBe("Failure")
        else {
          if (result._tag !== "Success") throw new Error(`Invocation failed: ${String(result.failure)}`)
          if (handles.has(step.id)) expect(result.success).toEqual(handles.get(step.id)!)
          handles.set(step.id, result.success)
          live.add(step.id)
        }
      } else if (step.op === "cancel") {
        const handle = handles.get(step.id)
        if (!live.has(step.id) && !(handle && step.retained)) fenced.add(step.id)
        await Effect.runPromise(actors.get("root")!.cancel(handle && step.retained ? handle : { executor: "actor", id: step.id }))
      } else if (step.op === "release") {
        const handle = handles.get(step.id)
        if (handle) await Effect.runPromise((await Effect.runPromise(host.getThread({ instance: "main", thread: endpoint(handle).thread })))!.invoke("release", { id: step.id, output: step.id }, { id: `release-${step.id}` }))
      } else if (handles.has(step.id)) await observe(step.id)
    }
    const threads = await directory(host)
    expect(threads.filter(entry => entry.parent === "root").map(entry => entry.coordinate).toSorted((left, right) => left.thread.localeCompare(right.thread))).toEqual([...handles.values()].map(endpoint).toSorted((left, right) => left.thread.localeCompare(right.thread)))
    const parent = await records(host, { actor: "probe", instance: "main", thread: "root" })
    for (const [id, handle] of handles) {
      expect((await records(host, endpoint(handle))).filter(record => record.message?.invocation?.method === "message")).toHaveLength(1)
      const finished = (await records(host, endpoint(handle))).some(record => record.event.type === "MessageDelivered")
      if (finished) await waitFor(() => observe(id), state => state.status !== "pending")
      expect(parent.filter(record => record.message?.inReplyTo === id).length).toBeLessThanOrEqual(1)
    }
  } finally { await Effect.runPromise(host.close) }
})), { ...RUNTIME_PROPERTY_OPTIONS, numRuns: 50 }), 60_000)
