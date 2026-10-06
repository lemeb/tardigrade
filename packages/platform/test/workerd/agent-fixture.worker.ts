import { Deferred, Effect, Layer } from "effect"
import { Prompt } from "effect/unstable/ai"
import { actorContext, createActor } from "@clavia/tardigrade-agent"
import { assistantThreadServices } from "@clavia/tardigrade-agent/services/runtime"
import { objectStorageFromR2 } from "@clavia/tardigrade-cloudflare/object-storage/r2"
import { createActorWorker } from "../../src/cloudflare"
import { retryServices } from "../fixtures/retry-provider"
import { contentServices } from "../fixtures/model-services"
import { budgetTools, requestServices, scriptedServices } from "../fixtures/children"

export interface AgentEnv { readonly OBJECTS: R2Bucket }
export let observedPrompt: Prompt.Prompt | undefined
let providerCalls = 0
export const providerCallCount = () => providerCalls
const services = contentServices(prompt => { providerCalls++; observedPrompt = prompt })
const retry = retryServices()
export const retryCallCount = retry.calls
const gate = Deferred.makeUnsafe<void>()
const worker = createActorWorker({
  actor: createActor,
  actorContext,
  promises: { pollIntervalMs: 5 },
  services: (env: AgentEnv, coordinate, runtime, storage) => coordinate.instance.startsWith("children-") ? assistantThreadServices(runtime, { maxChildDepth: 2, services: scriptedServices(gate) })
    : coordinate.instance.startsWith("requests-") ? assistantThreadServices(runtime, { maxChildDepth: 1, services: requestServices(gate), libraries: budgetTools }) : coordinate.instance.startsWith("retry-") ? retry.services : Layer.merge(
    services,
    objectStorageFromR2(env.OBJECTS, { cache: { storage, bucketNamespace: "agent-content" } }),
  ),
})
export const AgentActorDO = worker.ActorObject
// AgentThreadDO releases gated scripted model calls from inside the thread object that awaits them.
export class AgentThreadDO extends worker.ThreadObject {
  async releaseChildren() { await Effect.runPromise(Deferred.succeed(gate, undefined)) }
}
export default worker
