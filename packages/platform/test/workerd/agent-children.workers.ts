import { env } from "cloudflare:workers"
import { SELF, runInDurableObject } from "cloudflare:test"
import { expect, test } from "vitest"
import type { ThreadCoordinate } from "@clavia/tardigrade-core"
import { cloudflareThreadName } from "../../src/cloudflare"
import type { AgentThreadDO } from "./agent-fixture.worker"
import type { Script } from "../fixtures/children"

const bindings = env as unknown as { THREADS: DurableObjectNamespace<AgentThreadDO> }
const root = (instance: string) => ({ actor: "tardie", instance, thread: "root" })
const stub = (coordinate: ThreadCoordinate) => bindings.THREADS.getByName(cloudflareThreadName(coordinate))
const events = async (coordinate: ThreadCoordinate) => (await stub(coordinate).records()).map(record => record.event)
const start = async (instance: string, script: Script) => {
  const base = `http://test/v1/actors/${instance}/threads`
  expect((await SELF.fetch(base, { method: "POST", body: JSON.stringify({ name: "root" }) })).status).toBe(200)
  expect((await SELF.fetch(`${base}/root/methods/message`, { method: "POST", headers: { "idempotency-key": "turn" }, body: JSON.stringify({ text: JSON.stringify(script) }) })).status).toBe(202)
}
const handles = async (instance: string) => (await events(root(instance))).flatMap(event => event.type === "ToolReturned" && event.promise ? [event.promise.handle] : [])
const child = async (instance: string): Promise<ThreadCoordinate> => {
  await expect.poll(() => handles(instance)).toHaveLength(1)
  return JSON.parse((await handles(instance))[0]!.endpoint!)
}
const toolSettlements = async (instance: string) => (await events(root(instance))).flatMap(event => event.type === "PromiseSettled" && event.ref.act === "agent.tool.execute" ? [event.result] : [])

test("a delegated child runs in its own thread object and answers its parent once across parent eviction", async () => {
  const instance = "children-delegate"
  await start(instance, { name: "parent", spawn: [{ name: "child", gate: true }] })
  const coordinate = await child(instance)
  expect((await stub(coordinate).records())[0]!.event).toEqual({ type: "ThreadCreated", address: coordinate, parent: root(instance), depth: 1, placement: "independent" })
  await runInDurableObject(stub(root(instance)), async object => { await object.dispose() })
  await stub(coordinate).releaseChildren()
  await expect.poll(() => toolSettlements(instance)).toEqual([{ status: "fulfilled", value: { answer: "answer:child" } }])
  await expect.poll(async () => (await events(root(instance))).filter(event => event.type === "TurnSettled" && event.turnId.startsWith("promise:")).length).toBe(1)
  expect(await toolSettlements(instance)).toHaveLength(1)
})

test("stopping the parent turn cancels its child thread object", async () => {
  const instance = "children-stop"
  await start(instance, { name: "parent", hold: true, spawn: [{ name: "child", hold: true }] })
  const coordinate = await child(instance)
  await expect.poll(async () => (await events(coordinate)).some(event => event.type === "ModelCalled")).toBe(true)
  const cancel = `http://test/v1/actors/${instance}/threads/root/methods/message/calls/turn/cancellation`
  expect((await SELF.fetch(cancel, { method: "PUT", body: JSON.stringify({ reason: "stop" }) })).status).toBe(202)
  for (const thread of [root(instance), coordinate]) {
    await expect.poll(async () => (await events(thread)).filter(event => event.type === "TurnSettled").map(event => event.type === "TurnSettled" ? event.outcome : undefined)).toEqual(["cancelled"])
  }
})

const replies = async (instance: string) => (await events(root(instance))).filter(event => event.type === "ActorReplyReceived")
const decisions = async (coordinate: ThreadCoordinate) => (await events(coordinate)).flatMap(event => event.type === "BudgetResolved" ? [event.decision] : [])

test("a child thread object asks its parent for tool calls and receives one grant", async () => {
  const instance = "requests-grant"
  await start(instance, { name: "parent", spawn: [{ name: "child", request: 2 }], grant: 2 })
  const coordinate = await child(instance)
  await expect.poll(() => toolSettlements(instance)).toEqual([{ status: "fulfilled", value: { answer: `budget:child:{"allowed":true,"additional":2}` } }])
  expect(await replies(instance)).toHaveLength(1)
  expect(await decisions(coordinate)).toEqual([{ allowed: true, additional: 2 }])
})

test("a child thread object evicted while it waits for a grant reads the grant once on wake", async () => {
  const instance = "requests-evict"
  await start(instance, { name: "parent", spawn: [{ name: "child", request: 3 }], grant: 3, requestGate: true })
  const coordinate = await child(instance)
  await expect.poll(async () => (await events(root(instance))).some(event => event.type === "ModelCalled" && event.purpose === "inference" && event.turnId.startsWith(`["request",`))).toBe(true)
  await runInDurableObject(stub(coordinate), async object => { await object.dispose() })
  await stub(root(instance)).releaseChildren()
  await expect.poll(() => toolSettlements(instance)).toEqual([{ status: "fulfilled", value: { answer: `budget:child:{"allowed":true,"additional":3}` } }])
  expect(await replies(instance)).toHaveLength(1)
  expect(await decisions(coordinate)).toEqual([{ allowed: true, additional: 3 }])
  expect((await events(coordinate)).filter(event => event.type === "EffectRequested" && event.ref.act === "agent.budget.request")).toHaveLength(1)
})
