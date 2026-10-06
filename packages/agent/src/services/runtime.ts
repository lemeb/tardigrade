import { ModelInfo, ToolCatalog, actorContext } from "../actor/context"
import { modelInfo, modelActs, ModelLock, Model } from "./model"
import { askPermission, PermissionRequests } from "./decisions"
import { toolActs } from "./tools"
import { createHash } from "node:crypto"
import { Actor, ActorRequest, RuntimeError, ThreadCoordinate, abortRequested, methodResult, type ActorCaller, type ActorRuntime, type ExecutionHandle, type RuntimeEvent, type ActorDefinition, type ActorOutput, type EffectExecution, type ActService, Promises, localActors, threadActors, createActorStore, Invocation, type Supervisor } from "@clavia/tardigrade-core"
import { Effect, Layer, Schema, Semaphore } from "effect"
import { Workspace, memoryWorkspace, AgentMessage, AgentBudget, DEFAULT_AGENT_TOOL_CALLS, fetch, alarm, workspace, agents, type LibraryImplementation } from "@clavia/tardigrade-libraries"
import { createActor } from "../agent"
import { budgetState } from "../atoms/durable/budget"
import { Event } from "../contracts/events"
import { inferenceState, turnOutput } from "../atoms/durable/inference"
import { AgentMessageInput, AgentMessageOutput } from "../actor/methods"

export interface AssistantContext {
  readonly depth: number
  readonly parent: ActorCaller | undefined
}

type AgentActs = ActService<"agent.model.generate"> | ActService<"agent.model.summarize"> | ActService<"agent.tool.execute"> | ActService<"agent.permission.request">

type AssistantDefinition<Services> = ActorDefinition<Event, ActorOutput<unknown, Event, Services | AgentActs>, ModelInfo | ToolCatalog>

export interface AssistantOptions<Services = never> {
  readonly actor?: AssistantDefinition<Services>
  readonly services: Layer.Layer<Model | ModelLock | Promises | PermissionRequests | Services, Error, Actor> | ((context: AssistantContext, host: ActorRuntime<Event>) => Layer.Layer<Model | ModelLock | Promises | PermissionRequests | Services, Error, Actor>)
  readonly libraries?: readonly LibraryImplementation<Services | Actor | Workspace | Promises | EffectExecution>[]
  readonly maxChildDepth: number
  readonly onEvent?: (event: RuntimeEvent<Event>, depth: number) => void
}

// assistantServices supplies local child actors while the host chooses model and promise implementations.
export function assistantServices<Services>(host: ActorRuntime<Event>, options: AssistantOptions<Services>, depth: number, parent?: ActorCaller, budget?: typeof AgentBudget.Type): Layer.Layer<ModelInfo | ToolCatalog | AgentActs | Model | ModelLock | Promises | Actor | Workspace | Services, Error> {
  const children = localActors({
    run: (call, caller) => Effect.gen(function* () {
      if (depth >= options.maxChildDepth) return yield* Effect.fail(new RuntimeError(`Child depth limit reached: ${options.maxChildDepth}`))
      const actor: AssistantDefinition<Services> = options.actor ?? createActor
      if (call.target.actor !== actor.actorName) return yield* Effect.fail(new RuntimeError(`Unknown actor: ${call.target.actor}`))
      if (call.method !== "message") return yield* Effect.fail(new RuntimeError(`Unknown agent method: ${call.method}`))
      const input = yield* Schema.decodeUnknownEffect(AgentMessage)(call.input).pipe(Effect.mapError(RuntimeError.from))
      const childBudget = input.budget ?? { toolCalls: DEFAULT_AGENT_TOOL_CALLS }
      let childRuntime!: ActorRuntime<Event>
      const configuration = assistantRuntime(options, depth + 1, caller, childBudget)
      return yield* Effect.acquireUseRelease(
        createActorStore({ actor, ...configuration, services: runtime => {
          childRuntime = runtime
          return configuration.services(runtime)
        } }),
        child => Effect.gen(function* () {
          yield* childRuntime.send([{ type: "TurnRequested", text: input.text, turnId: call.id }])
          yield* child.wait
          const reply = child.snapshot().events.filter(Schema.is(Event)).findLast(event => event.type === "TurnSettled")
          if (!reply || reply.type !== "TurnSettled") return yield* Effect.fail(new RuntimeError("Child finished without an answer"))
          return { answer: turnOutput(child.snapshot().events.filter(Schema.is(Event)), reply) }
        }).pipe(Effect.onInterrupt(() => caller.cancelled() ? Effect.gen(function* () {
          const snapshot = child.snapshot()
          const refs = [...snapshot.pending().map(work => work.ref), ...snapshot.deferred().map(work => work.ref)]
          for (const ref of refs) yield* child.cancel(ref, "Parent invocation cancelled")
          yield* child.wait
        }) : Effect.void)),
        child => child.close,

      )
    }),
    onRequest: (handle, request) => host.send([{ type: "ActorRequestReceived", handle, request }, { type: "TurnRequested", source: "agent", turnId: `request:${JSON.stringify([handle, request.requestId])}`, text: `Child request (data): ${JSON.stringify({ handle, ...request })}` }]),
    onReply: (handle, requestId, result) => host.record({ type: "ActorReplyReceived", handle, requestId, result }),
    onMessage: (handle, message) => host.send([{ type: "TurnRequested", source: "agent", turnId: `${handle.id}:notice:${crypto.randomUUID()}`, text: JSON.stringify({ handle, message }) }]),
  })
  return assistantLayer(host, options, { depth, parent }, children, budget)
}

// assistantThreadServices supplies persisted child threads to a thread host; each delegated call runs in a journaled child of the calling thread, and a child reaches its parent through durable request and notice messages.
export function assistantThreadServices<Services>(host: ActorRuntime<Event>, options: AssistantOptions<Services>): Layer.Layer<ModelInfo | ToolCatalog | AgentActs | Model | ModelLock | Promises | Actor | Workspace | Services, Error, Supervisor | Invocation> {
  const children = Layer.effect(Actor, Effect.gen(function* () {
    const actor = yield* Actor
    const replies = Semaphore.makeUnsafe(1)
    const sibling = (handle: ExecutionHandle) => {
      const address = host.thread?.address
      const value = handle.executor === "actor" && handle.endpoint !== undefined ? parseJson(handle.endpoint) : undefined
      return address && Schema.is(ThreadCoordinate)(value) && value.actor === address.actor && value.instance === address.instance ? value : undefined
    }
    // open lists the unsettled request turns a child started, so cancelling that child also ends them.
    const open = (child: ThreadCoordinate) => host.get(inferenceState).turns.filter(turn => turn.settlement === null && turn.cancellation === null && turn.invocationRef?.method === "request" && turn.turnId.startsWith(childRequestPrefix(child.thread)))
    return {
      ...actor,
      // invoke rejects malformed input before allocation leaves an empty child.
      invoke: call => Schema.decodeUnknownEffect(AgentMessageInput, { onExcessProperty: "error" })(call.input).pipe(Effect.mapError(RuntimeError.from), Effect.andThen(actor.invoke(call))),
      poll: handle => actor.poll(handle).pipe(Effect.map(state => state.status === "fulfilled" && Schema.is(AgentMessageOutput)(state.value) ? { status: "fulfilled" as const, value: { answer: state.value.text } } : state)),
      cancel: handle => actor.cancel(handle).pipe(Effect.andThen(Effect.suspend(() => {
        const child = sibling(handle)
        const turns = child ? open(child) : []
        return turns.length ? host.send(turns.map(turn => abortRequested({ ref: turn.invocationRef!, reason: "Child cancelled" }))) : Effect.void
      }))),
      // reply records the first answer to a child's open request turn; the request method returns it to that child.
      reply: (handle, requestId, result) => replies.withPermit(Effect.gen(function* () {
        const child = sibling(handle)
        const turnId = child ? childRequestId(child.thread, requestId) : undefined
        const turn = host.get(inferenceState).turns.find(turn => turn.turnId === turnId && turn.invocationRef?.method === "request")
        if (!turnId || !turn || turn.settlement !== null || turn.reply !== undefined) return yield* Effect.fail(new RuntimeError("No matching pending actor request"))
        const value = yield* Schema.decodeEffect(Schema.Json)(result).pipe(Effect.mapError(RuntimeError.from))
        yield* host.record({ type: "ActorReplyReceived", handle, requestId, result: value, turnId })
      })),
    } satisfies typeof Actor.Service
  })).pipe(Layer.provide(threadActors({ runtime: host, method: "message", maxDepth: options.maxChildDepth })))
  return Layer.unwrap(Effect.gen(function* () {
    const invocation = yield* Invocation
    const created = host.thread
    const parent = created?.parent ? threadCaller(host, invocation, created.address, created.parent) : undefined
    return assistantLayer(host, options, { depth: created?.depth ?? 0, parent }, children)
  }))
}

const parseJson = (text: string): unknown => {
  try { return JSON.parse(text) } catch { return undefined }
}

// childRequestId names a child's request message and the parent turn it starts.
const childRequestId = (child: string, requestId: string) => JSON.stringify(["request", child, requestId])
const childRequestPrefix = (child: string) => `${JSON.stringify(["request", child]).slice(0, -1)},`

// threadCaller sends requests and notices to the parent thread; a request waits for the parent's reply in this thread's journal, so a rerun after restart resends the same message and reads the same reply.
function threadCaller(host: ActorRuntime<Event>, invocation: typeof Invocation.Service, self: ThreadCoordinate, parent: ThreadCoordinate): ActorCaller {
  const delegated = () => host.get(inferenceState).turns.find(turn => turn.invocationRef?.method === "message")
  const handle = (): ExecutionHandle => {
    const id = delegated()?.invocationRef?.id
    if (id === undefined) throw new RuntimeError("Child thread has no delegated call")
    return { executor: "actor", id, endpoint: JSON.stringify({ actor: self.actor, instance: self.instance, thread: self.thread }) }
  }
  const reply = (id: string): Effect.Effect<Schema.Json, Error> => host.reply(parent, id).pipe(Effect.flatMap(body => body === undefined
    ? Effect.sleep(host.promisePolicy.pollIntervalMs).pipe(Effect.andThen(Effect.suspend(() => reply(id))))
    : Schema.decodeUnknownEffect(methodResult(Schema.Json))(body).pipe(Effect.mapError(RuntimeError.from), Effect.flatMap(result => result.status === "completed"
      ? Effect.succeed(result.output) : Effect.fail(new RuntimeError(result.status === "failed" ? result.error : result.reason))))))
  return {
    get handle() { return handle() },
    cancelled: () => { const turn = delegated(); return turn !== undefined && (turn.cancellation !== null || turn.settlement === "cancelled") },
    notify: message => Effect.gen(function* () {
      const value = yield* Schema.decodeEffect(Schema.Json)(message).pipe(Effect.mapError(RuntimeError.from))
      // Equal notices share one identity, so a rerun cannot deliver a notice twice.
      const id = JSON.stringify(["notice", self.thread, createHash("sha256").update(JSON.stringify(value)).digest("hex")])
      yield* invocation.send({ id, target: parent, body: { method: "notice", input: { handle: yield* Effect.try({ try: handle, catch: RuntimeError.from }), message: value } } })
    }),
    request: request => Effect.gen(function* () {
      const value = yield* Schema.decodeEffect(ActorRequest)(request).pipe(Effect.mapError(RuntimeError.from))
      const id = childRequestId(self.thread, value.requestId)
      yield* invocation.send({ id, target: parent, body: { method: "request", input: { handle: yield* Effect.try({ try: handle, catch: RuntimeError.from }), ...value } } })
      return yield* reply(id)
    }),
  }
}

// assistantLayer composes agent acts over host services, the workspace, and the chosen child actors.
function assistantLayer<Services, Children>(host: ActorRuntime<Event>, options: AssistantOptions<Services>, context: AssistantContext, children: Layer.Layer<Actor, Error, Children>, budget?: typeof AgentBudget.Type) {
  const services = typeof options.services === "function" ? options.services(context, host) : options.services
  const platform = Layer.mergeAll(services, memoryWorkspace, Layer.effectDiscard(budget ? host.onReady(Effect.suspend(() => host.record({
    type: host.get(budgetState).some(entry => entry.metric === "toolCalls") ? "BudgetUpdated" : "BudgetConfigured",
    metric: "toolCalls", policy: { limit: budget.toolCalls, onExhausted: "deny" },
  }))) : Effect.void)).pipe(Layer.provideMerge(children))
  return Layer.mergeAll(modelInfo, modelActs, askPermission, toolActs(options.libraries ?? [fetch(), alarm(), workspace(), agents({ actor: (options.actor ?? createActor).actorName })])).pipe(Layer.provideMerge(platform))
}

// assistantRuntime configures services and observation for one level of child actors.
export function assistantRuntime<Services = never>(options: AssistantOptions<Services>, depth = 0, parent?: ActorCaller, budget?: typeof AgentBudget.Type) {
  if (!Number.isSafeInteger(options.maxChildDepth) || options.maxChildDepth < 0) throw new RuntimeError("maxChildDepth must be a nonnegative integer")
  return {
    actorContext,
    services: (host: ActorRuntime<Event>) => assistantServices(host, options, depth, parent, budget),
    onEvent: (event: RuntimeEvent<Event>) => options.onEvent?.(event, depth),
  }
}
