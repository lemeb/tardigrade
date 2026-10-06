import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Deferred, Effect } from "effect"
import { type CheckpointPolicy, type Recorded, type ThreadCoordinate } from "@clavia/tardigrade-core"
import { actorContext, createActor, type Event } from "@clavia/tardigrade-agent"
import { assistantServices, assistantThreadServices, type AssistantOptions } from "@clavia/tardigrade-agent/services/runtime"
import { inferenceState, inferState } from "@clavia/tardigrade-agent/atoms/durable/inference"
import { budgetState } from "@clavia/tardigrade-agent/atoms/durable/budget"
import { messages } from "@clavia/tardigrade-agent/atoms/durable"
import { createBunHost } from "../../src/bun"
import { bunThreadPath } from "../../src/bun/observe"
import { waitFor } from "../fixtures/wait"
import { budgetTools, noticeServices, noticeTools, requestServices, scriptedServices, type Script } from "../fixtures/children"

// harness opens one Bun host whose model follows each thread's script; the gate holds gated model calls until released.
function harness(storage: string, options: { readonly local?: boolean; readonly maxChildDepth?: number; readonly checkpoint?: CheckpointPolicy; readonly requests?: boolean; readonly notices?: boolean } = {}) {
  const gate = Deferred.makeUnsafe<void>()
  const maxChildDepth = options.maxChildDepth ?? 1
  const assistant: AssistantOptions = { maxChildDepth, services: scriptedServices(gate) }
  return Effect.runPromise(createBunHost({
    actor: createActor, actorContext, storage, promises: { pollIntervalMs: 5, retryIntervalMs: 5 },
    checkpointPolicy: options.checkpoint ?? { mode: "manual" },
    services: (_coordinate, runtime) => options.local ? assistantServices(runtime, assistant, 0)
      : options.requests ? assistantThreadServices(runtime, { maxChildDepth, services: requestServices(gate), libraries: budgetTools })
      : options.notices ? assistantThreadServices(runtime, { maxChildDepth, services: noticeServices(gate), libraries: noticeTools })
      : assistantThreadServices(runtime, assistant),
  })).then(host => ({ host, release: () => Effect.runPromise(Deferred.succeed(gate, undefined)) }))
}
type Harness = Awaited<ReturnType<typeof harness>>

async function fixture(name: string, use: (storage: string) => Promise<void>) {
  const storage = await mkdtemp(join(tmpdir(), `tardie-${name}-`))
  try { await use(storage) } finally { await rm(storage, { recursive: true, force: true }) }
}

const root = { actor: "tardie", instance: "main", thread: "root" }
const thread = async ({ host }: Harness, coordinate: ThreadCoordinate) => {
  const found = await Effect.runPromise(host.getThread({ instance: coordinate.instance, thread: coordinate.thread }))
  if (!found) throw new Error(`Unknown thread: ${coordinate.thread}`)
  return found
}
const records = async (opened: Harness, coordinate: ThreadCoordinate = root) => Effect.runPromise((await thread(opened, coordinate)).records())
const events = async (opened: Harness, coordinate: ThreadCoordinate = root) => (await records(opened, coordinate)).map(record => record.event)
const children = async ({ host }: Harness) => (await Effect.runPromise(host.supervisorStore("main"))).threads.get().filter(entry => entry.parent !== null && entry.status === "registered")
const start = async (opened: Harness, script: Script, id = "turn") => {
  const ref = await Effect.runPromise(opened.host.allocateRootThread({ instance: "main", name: "root" }))
  await Effect.runPromise(ref.invoke("message", { text: JSON.stringify(script) }, { id }))
  return ref
}
const settled = (prefix: string) => (list: readonly Recorded<Event>["event"][]) => list.some(event => event.type === "TurnSettled" && event.turnId.startsWith(prefix))
const resultTurns = (list: readonly Recorded<Event>["event"][]) => list.flatMap(event => event.type === "TurnSettled" && event.turnId.startsWith("promise:") ? [event] : [])
const settlements = (list: readonly Recorded<Event>["event"][]) => list.flatMap(event => event.type === "PromiseSettled" && event.ref.act === "agent.tool.execute" ? [event] : [])
const recover = async ({ host }: Harness, coordinate: ThreadCoordinate) => { await Effect.runPromise(host.recover(coordinate)) }

test("a delegated child is a journaled child thread whose answer reaches the parent once", () => fixture("agent-children-once", async storage => {
  const opened = await harness(storage)
  try {
    await start(opened, { name: "parent", spawn: [{ name: "child" }] })
    const parent = await waitFor(() => events(opened), settled("promise:"))
    const [child] = await children(opened)
    expect(child).toMatchObject({ parent: "root", depth: 1, status: "registered" })
    const childRecords = await records(opened, child!.coordinate)
    expect(childRecords[0]!.event).toEqual({ type: "ThreadCreated", address: child!.coordinate, parent: root, depth: 1, placement: "colocated" })
    const invocation = childRecords.find(record => record.message?.invocation)
    expect(invocation!.message).toMatchObject({ from: root, invocation: { method: "message", input: { budget: { toolCalls: 20 } } } })
    expect((await thread(opened, child!.coordinate)).get(budgetState)).toMatchObject([{ metric: "toolCalls", policy: { limit: 20, onExhausted: "deny" } }])
    expect(settlements(parent)).toEqual([expect.objectContaining({ result: { status: "fulfilled", value: { answer: "answer:child" } } })])
    expect(resultTurns(parent)).toHaveLength(1)
    const handle = parent.flatMap(event => event.type === "ToolReturned" && event.promise ? [event.promise.handle] : [])
    expect(handle).toEqual([{ executor: "actor", id: invocation!.message!.id, endpoint: JSON.stringify(child!.coordinate) }])
    expect((await records(opened)).filter(record => record.message?.inReplyTo === invocation!.message!.id)).toHaveLength(1)
    expect(Object.keys(await Effect.runPromise(opened.host.methodContracts({ instance: "main" })))).toEqual(["message", "request", "notice"])
  } finally { await Effect.runPromise(opened.host.close) }
}))

for (const checkpointed of [false, true]) test(`a child finishing after a parent restart${checkpointed ? " from a checkpoint" : ""} is delivered once`, () => fixture("agent-children-restart", async storage => {
  if (checkpointed) {
    const warm = await harness(storage, { checkpoint: { mode: "threshold", options: { everyEvents: 1 } } })
    try {
      await start(warm, { name: "parent", spawn: [{ name: "child", gate: true }] }, "warm")
      await waitFor(() => children(warm), list => list.length === 1)
    } finally { await Effect.runPromise(warm.host.close) }
  } else {
    const first = await harness(storage)
    try {
      await start(first, { name: "parent", spawn: [{ name: "child", gate: true }] }, "warm")
      await waitFor(() => events(first), settled("warm"))
    } finally { await Effect.runPromise(first.host.close) }
  }
  const second = await harness(storage)
  try {
    if (checkpointed) {
      const stored = new Database(bunThreadPath(storage, root), { readonly: true })
      try { expect(stored.query("SELECT position FROM checkpoint").get()).toMatchObject({ position: expect.any(Number) }) } finally { stored.close() }
    }
    await recover(second, root)
    const [child] = await children(second)
    await recover(second, child!.coordinate)
    await second.release()
    const parent = await waitFor(() => events(second), settled("promise:"))
    expect(await children(second)).toHaveLength(1)
    expect(settlements(parent)).toEqual([expect.objectContaining({ result: { status: "fulfilled", value: { answer: "answer:child" } } })])
    expect(resultTurns(parent)).toHaveLength(1)
    expect((await records(second, child!.coordinate)).filter(record => record.message?.invocation)).toHaveLength(1)
    expect((await events(second, child!.coordinate)).filter(event => event.type === "ModelCalled")).toHaveLength(1)
  } finally { await Effect.runPromise(second.host.close) }
}))

test("parent stop cancels running children and their descendants", () => fixture("agent-children-stop", async storage => {
  const opened = await harness(storage, { maxChildDepth: 2 })
  try {
    const ref = await start(opened, { name: "parent", hold: true, spawn: [{ name: "child", hold: true, spawn: [{ name: "grandchild", hold: true }] }] })
    const tree = await waitFor(() => children(opened), list => list.length === 2)
    await waitFor(() => events(opened, tree[1]!.coordinate), list => list.some(event => event.type === "ModelCalled"))
    await Effect.runPromise(ref.cancel("message", "turn", "stop"))
    for (const coordinate of [root, ...tree.map(entry => entry.coordinate)]) {
      const list = await waitFor(() => events(opened, coordinate), list => list.some(event => event.type === "TurnSettled"))
      expect(list.filter(event => event.type === "TurnSettled")).toEqual([expect.objectContaining({ outcome: "cancelled" })])
    }
    expect(resultTurns(await events(opened))).toHaveLength(0)
  } finally { await Effect.runPromise(opened.host.close) }
}))

test("a failed child and a refused grandchild surface to their callers", () => fixture("agent-children-failure", async storage => {
  const opened = await harness(storage)
  try {
    await start(opened, { name: "parent", spawn: [{ name: "broken", fail: true }, { name: "nested", spawn: [{ name: "grandchild" }] }] })
    const parent = await waitFor(() => events(opened), list => resultTurns(list).length === 2)
    const tree = await children(opened)
    expect(tree.map(entry => entry.depth)).toEqual([1, 1])
    expect(new Set(tree.map(entry => entry.coordinate.thread)).size).toBe(2)
    const results = settlements(parent).map(event => event.result)
    expect(results).toContainEqual({ status: "rejected", reason: expect.stringContaining("model failed: broken") })
    expect(results).toContainEqual({ status: "fulfilled", value: { answer: "delegated:nested" } })
    const refused = (await Promise.all(tree.map(entry => events(opened, entry.coordinate)))).flat().filter(event => event.type === "ToolReturned" && event.error !== null)
    expect(refused).toEqual([expect.objectContaining({ error: expect.stringContaining("Child depth limit reached: 1") })])
  } finally { await Effect.runPromise(opened.host.close) }
}))

test("checkpoint plus suffix and full replay restore equal parent state", () => fixture("agent-children-replay", async storage => {
  const warm = await harness(storage, { checkpoint: { mode: "threshold", options: { everyEvents: 1 } } })
  try {
    const ref = await start(warm, { name: "parent" }, "warm")
    await waitFor(() => events(warm), settled("warm"))
    await Effect.runPromise(ref.invoke("message", { text: JSON.stringify({ name: "ignored" }) }, { id: "noop" }))
    await waitFor(() => events(warm), settled("noop"))
  } finally { await Effect.runPromise(warm.host.close) }
  const live = await harness(storage)
  let expected: unknown
  try {
    await recover(live, root)
    const ref = await thread(live, root)
    await Effect.runPromise(ref.invoke("message", { text: JSON.stringify({ name: "parent", spawn: [{ name: "a" }, { name: "b" }] }) }, { id: "turn" }))
    await waitFor(() => events(live), list => resultTurns(list).length === 2)
    await Effect.runPromise(ref.wait)
    expected = [ref.get(inferenceState), ref.get(budgetState), ref.get(messages)]
  } finally { await Effect.runPromise(live.host.close) }
  const stateAfter = async () => {
    const opened = await harness(storage)
    try {
      await recover(opened, root)
      const ref = await thread(opened, root)
      return [ref.get(inferenceState), ref.get(budgetState), ref.get(messages)]
    } finally { await Effect.runPromise(opened.host.close) }
  }
  const database = new Database(bunThreadPath(storage, root))
  try {
    const checkpoint = database.query<{ position: number }, []>("SELECT position FROM checkpoint").get()
    const replies = database.query<{ seq: number }, []>("SELECT seq FROM experimental_events WHERE event LIKE '%inReplyTo%'").all()
    expect(replies).toHaveLength(2)
    expect(replies.every(reply => reply.seq >= checkpoint!.position)).toBe(true)
  } finally { database.close() }
  expect(await stateAfter()).toEqual(expected as never)
  const stripped = new Database(bunThreadPath(storage, root))
  try { stripped.exec("DELETE FROM checkpoint_chunks; DELETE FROM checkpoint") } finally { stripped.close() }
  expect(await stateAfter()).toEqual(expected as never)
}))

test("threads recorded with local child actors replay and settle their lost children once", () => fixture("agent-children-legacy", async storage => {
  const legacy = await harness(storage, { local: true })
  try {
    await start(legacy, { name: "parent", spawn: [{ name: "done" }] }, "finished")
    await waitFor(() => events(legacy), settled("promise:"))
    const ref = await thread(legacy, root)
    await Effect.runPromise(ref.invoke("message", { text: JSON.stringify({ name: "parent", spawn: [{ name: "lost", hold: true }] }) }, { id: "lost" }))
    await waitFor(() => events(legacy), settled("lost"))
  } finally { await Effect.runPromise(legacy.host.close) }
  const upgraded = await harness(storage)
  try {
    await recover(upgraded, root)
    const parent = await waitFor(() => events(upgraded), list => resultTurns(list).length === 2)
    expect(settlements(parent).map(event => event.result)).toEqual([
      { status: "fulfilled", value: { answer: "answer:done" } },
      { status: "rejected", reason: "Local actor handle is no longer available" },
    ])
    expect(await children(upgraded)).toHaveLength(0)
    await recover(upgraded, root)
    expect(settlements(await events(upgraded))).toHaveLength(2)
  } finally { await Effect.runPromise(upgraded.host.close) }
}))

// requestTurn names the parent turn of the child's only request; its id embeds the child's tool call id.
const requestTurn = (child: ThreadCoordinate) => (turnId: string) => turnId.startsWith(`["request","${child.thread}",`)
const requestTurnId = (list: readonly Recorded<Event>[], child: ThreadCoordinate) => list.flatMap(record => record.message?.invocation?.method === "request" && requestTurn(child)(record.message.id) ? [record.message.id] : [])
const budgetDecisions = (list: readonly Recorded<Event>["event"][]) => list.flatMap(event => event.type === "BudgetResolved" ? [event.decision] : [])
const replies = (list: readonly Recorded<Event>["event"][]) => list.filter(event => event.type === "ActorReplyReceived")
const childAnswer = (list: readonly Recorded<Event>["event"][]) => settlements(list).map(event => event.result)

for (const grant of [true, false]) test(`a thread child's budget request is ${grant ? "granted" : "refused"} by its parent once`, () => fixture("agent-children-request", async storage => {
  const opened = await harness(storage, { requests: true })
  try {
    await start(opened, { name: "parent", spawn: [{ name: "child", request: 2 }], ...(grant ? { grant: 2 } : {}) })
    const parent = await waitFor(() => events(opened), list => resultTurns(list).length === 1)
    const [child] = await children(opened)
    const childEvents = await events(opened, child!.coordinate)
    expect(budgetDecisions(childEvents)).toEqual([grant ? { allowed: true, additional: 2 } : { allowed: false, reason: "Budget request failed: Request answered without a reply: refused:parent" }])
    expect(childAnswer(parent)).toEqual([{ status: "fulfilled", value: { answer: expect.stringContaining(grant ? `budget:child:{"allowed":true,"additional":2}` : "budget:child:") } }])
    const [turnId] = requestTurnId(await records(opened), child!.coordinate)
    expect(requestTurnId(await records(opened), child!.coordinate)).toHaveLength(1)
    expect(JSON.parse(turnId!)).toEqual(["request", child!.coordinate.thread, expect.any(String)])
    expect(replies(parent)).toEqual(grant ? [expect.objectContaining({ requestId: JSON.parse(turnId!)[2], result: { allowed: true, amount: 2 }, turnId, handle: { executor: "actor", id: expect.any(String), endpoint: JSON.stringify(child!.coordinate) } })] : [])
    expect(parent.filter(event => event.type === "TurnSettled" && event.turnId === turnId)).toEqual([expect.objectContaining({ outcome: "completed" })])
    expect((await records(opened)).filter(record => record.message?.invocation?.method === "request")).toEqual([expect.objectContaining({ message: expect.objectContaining({ id: turnId, from: child!.coordinate }) })])
    expect((await records(opened, child!.coordinate)).filter(record => record.message?.inReplyTo === turnId)).toHaveLength(1)
    const handle = parent.flatMap(event => event.type === "ToolReturned" && event.promise ? [event.promise.handle] : [])
    if (grant) expect(replies(parent)[0]).toMatchObject({ handle: handle[0] })
  } finally { await Effect.runPromise(opened.host.close) }
}))

for (const checkpointed of [false, true]) test(`a pending child request survives parent and child restart${checkpointed ? " with checkpoints" : ""} and is answered once`, () => fixture("agent-children-request-restart", async storage => {
  const checkpoint = checkpointed ? { checkpoint: { mode: "threshold", options: { everyEvents: 1 } } as const } : {}
  const first = await harness(storage, { requests: true, ...checkpoint })
  let child: ThreadCoordinate
  try {
    await start(first, { name: "parent", spawn: [{ name: "child", request: 3 }], grant: 3, requestGate: true })
    child = (await waitFor(() => children(first), list => list.length === 1))[0]!.coordinate
    await waitFor(() => events(first), list => list.some(event => event.type === "ModelCalled" && event.purpose === "inference" && requestTurn(child)(event.turnId)))
  } finally { await Effect.runPromise(first.host.close) }
  if (checkpointed) {
    const stored = new Database(bunThreadPath(storage, root), { readonly: true })
    try { expect(stored.query("SELECT position FROM checkpoint").get()).toMatchObject({ position: expect.any(Number) }) } finally { stored.close() }
  }
  const second = await harness(storage, { requests: true, ...checkpoint })
  try {
    await recover(second, root)
    await recover(second, child)
    await second.release()
    const parent = await waitFor(() => events(second), list => resultTurns(list).length === 1)
    const childEvents = await waitFor(() => events(second, child), list => list.some(event => event.type === "TurnSettled"))
    expect(budgetDecisions(childEvents)).toEqual([{ allowed: true, additional: 3 }])
    expect(replies(parent)).toHaveLength(1)
    expect(childAnswer(parent)).toEqual([{ status: "fulfilled", value: { answer: expect.stringContaining(`"additional":3`) } }])
    expect((await records(second)).filter(record => record.message?.invocation?.method === "request")).toHaveLength(1)
    expect((await records(second, child)).filter(record => record.message?.inReplyTo !== undefined && requestTurn(child)(record.message.inReplyTo))).toHaveLength(1)
    expect(childEvents.filter(event => event.type === "EffectRequested" && event.ref.act === "agent.budget.request")).toHaveLength(1)
  } finally { await Effect.runPromise(second.host.close) }
}))

test("a second answer to a child request fails and the first one stands", () => fixture("agent-children-request-once", async storage => {
  const opened = await harness(storage, { requests: true })
  try {
    await start(opened, { name: "parent", spawn: [{ name: "child", request: 2 }], grant: 2, grantTwice: true })
    const parent = await waitFor(() => events(opened), list => resultTurns(list).length === 1)
    expect(replies(parent)).toHaveLength(1)
    expect(parent.filter(event => event.type === "ToolReturned" && event.error?.includes("No matching pending actor request"))).toHaveLength(1)
    const [child] = await children(opened)
    expect(budgetDecisions(await events(opened, child!.coordinate))).toEqual([{ allowed: true, additional: 2 }])
  } finally { await Effect.runPromise(opened.host.close) }
}))

// Child cancellation cleanup runs after the stopped turn settles, so the request turn may start its model call before its own cancellation lands.
test("parent stop while a child request waits cancels the request turn", () => fixture("agent-children-request-stop", async storage => {
  const opened = await harness(storage, { requests: true })
  try {
    const ref = await start(opened, { name: "parent", hold: true, spawn: [{ name: "child", request: 2 }], grant: 2, requestGate: true })
    const child = (await waitFor(() => children(opened), list => list.length === 1))[0]!.coordinate
    await waitFor(() => records(opened), list => requestTurnId(list, child).length === 1)
    await Effect.runPromise(ref.cancel("message", "turn", "stop"))
    const parent = await waitFor(() => events(opened), list => list.filter(event => event.type === "TurnSettled").length === 2)
    expect(parent.filter(event => event.type === "TurnSettled").map(event => event.type === "TurnSettled" && [event.turnId, event.outcome])).toEqual([["turn", "cancelled"], [requestTurnId(await records(opened), child)[0]!, "cancelled"]])
    expect(parent.filter(event => event.type === "ModelCalled" && event.purpose === "inference" && requestTurn(child)(event.turnId)).length).toBeLessThanOrEqual(1)
    expect(parent.filter(event => event.type === "ModelReturned" && event.purpose === "inference" && event.callId.includes("request"))).toEqual([])
    expect(replies(parent)).toEqual([])
    const childEvents = await waitFor(() => events(opened, child), list => list.some(event => event.type === "TurnSettled"))
    expect(childEvents.filter(event => event.type === "TurnSettled")).toEqual([expect.objectContaining({ outcome: "cancelled" })])
    expect(resultTurns(await events(opened))).toHaveLength(0)
  } finally { await Effect.runPromise(opened.host.close) }
}))

test("checkpoint plus suffix and full replay restore equal state after a child request", () => fixture("agent-children-request-replay", async storage => {
  const live = await harness(storage, { requests: true, checkpoint: { mode: "threshold", options: { everyEvents: 1 } } })
  let child: ThreadCoordinate
  const read = async (opened: Harness) => Promise.all([root, child].map(async coordinate => {
    const ref = await thread(opened, coordinate)
    return [ref.get(inferenceState), ref.get(budgetState), ref.get(messages)]
  }))
  let expected: unknown
  try {
    await start(live, { name: "parent", spawn: [{ name: "child", request: 2 }], grant: 2 })
    await waitFor(() => events(live), list => resultTurns(list).length === 1)
    child = (await children(live))[0]!.coordinate
    await waitFor(() => events(live, child), list => list.some(event => event.type === "TurnSettled"))
    await Effect.runPromise((await thread(live, root)).wait)
    expected = await read(live)
    expect(JSON.stringify(expected)).toContain(`"reply":{"allowed":true,"amount":2}`)
  } finally { await Effect.runPromise(live.host.close) }
  const stateAfter = async () => {
    const opened = await harness(storage, { requests: true })
    try {
      await recover(opened, root)
      await recover(opened, child)
      return await read(opened)
    } finally { await Effect.runPromise(opened.host.close) }
  }
  expect(await stateAfter()).toEqual(expected as never)
  for (const coordinate of [root, child!]) {
    const stripped = new Database(bunThreadPath(storage, coordinate))
    try { stripped.exec("DELETE FROM checkpoint_chunks; DELETE FROM checkpoint") } finally { stripped.close() }
  }
  expect(await stateAfter()).toEqual(expected as never)
}))

test("a local child reply carries no request turn and leaves turn state unchanged", () => fixture("agent-children-local-reply", async storage => {
  const opened = await harness(storage, { local: true })
  try {
    await start(opened, { name: "parent" })
    await waitFor(() => events(opened), settled("turn"))
    const before = (await thread(opened, root)).get(inferenceState)
    expect(inferState(before, { type: "ActorReplyReceived", handle: { executor: "actor", id: "turn", endpoint: "local" }, requestId: "turn", result: { allowed: true, amount: 1 } })).toBe(before)
    expect(before.turns.every(turn => turn.reply === undefined)).toBe(true)
  } finally { await Effect.runPromise(opened.host.close) }
}))

test("a thread child's repeated notice reaches its parent once as an agent turn", () => fixture("agent-children-notice", async storage => {
  const opened = await harness(storage, { notices: true })
  try {
    await start(opened, { name: "parent", spawn: [{ name: "child", notice: "halfway" }] })
    await waitFor(() => events(opened), list => resultTurns(list).length === 1)
    const child = (await children(opened))[0]!.coordinate
    const notices = (await records(opened)).filter(record => record.message?.invocation?.method === "notice")
    expect(notices).toEqual([expect.objectContaining({ message: expect.objectContaining({ from: child, invocation: { method: "notice", input: { handle: { executor: "actor", id: expect.any(String), endpoint: JSON.stringify(child) }, message: "halfway" } } }) })])
    const parent = await waitFor(() => events(opened), list => list.some(event => event.type === "TurnSettled" && event.turnId === notices[0]!.message!.id))
    expect(parent.filter(event => event.type === "TurnSettled" && event.turnId === notices[0]!.message!.id)).toEqual([expect.objectContaining({ outcome: "completed" })])
    expect(childAnswerOf(parent)).toEqual(["noticed:child"])
    expect((await records(opened, child)).filter(record => record.message?.inReplyTo === notices[0]!.message!.id)).toHaveLength(1)
  } finally { await Effect.runPromise(opened.host.close) }
}))
const childAnswerOf = (list: readonly Recorded<Event>["event"][]) => settlements(list).flatMap(event => event.result.status === "fulfilled" ? [(event.result.value as { answer: string }).answer] : [])
