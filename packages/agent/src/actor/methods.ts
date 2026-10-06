import { RuntimeError, type Getter, type MethodResult, actorMethod, AbortRequested, ActorRequest, ExecutionHandle } from "@clavia/tardigrade-core"
import { Schema } from "effect"
import { inferenceState } from "../atoms/durable/inference"
import { MessageContent, TurnBudget, TurnRequested } from "../contracts/events"

export const AgentMessageOutput = Schema.Struct({ text: Schema.String })
export type AgentMessageOutput = typeof AgentMessageOutput.Type

// agentReply projects a terminal turn into an invocation result for durable return delivery.
export function agentReply(id: string, get: Getter): MethodResult<AgentMessageOutput> | undefined {
  const turn = get(inferenceState).turns.find(turn => turn.turnId === id)
  if (!turn || turn.settlement === null) return undefined
  if (turn.settlement === "completed") {
    if (turn.answer === null) throw new RuntimeError("Completed turn has no answer")
    return { status: "completed", output: { text: turn.answer } }
  }
  if (turn.settlement === "failed") {
    if (turn.failure === null) throw new RuntimeError("Failed turn has no error")
    return { status: "failed", error: turn.failure }
  }
  if (turn.cancellation === null) throw new RuntimeError("Cancelled turn has no reason")
  return { status: "cancelled", reason: turn.cancellation }
}

// requestReply answers a child request with the first recorded reply; a request turn that settles without one ends the request.
export function requestReply(id: string, get: Getter): MethodResult<Schema.Json> | undefined {
  const turn = get(inferenceState).turns.find(turn => turn.turnId === id)
  if (!turn) return undefined
  if (turn.reply !== undefined) return { status: "completed", output: turn.reply }
  if (turn.settlement === null) return undefined
  if (turn.settlement === "cancelled") return { status: "cancelled", reason: turn.cancellation ?? "Request cancelled" }
  return { status: "failed", error: turn.settlement === "failed" ? turn.failure ?? "Request failed" : `Request answered without a reply: ${turn.answer ?? ""}` }
}

// ChildRequest and ChildNotice carry a thread child's handle, so the parent can answer or cancel that child.
export const ChildRequest = Schema.Struct({ handle: ExecutionHandle, ...ActorRequest.fields })
export const ChildNotice = Schema.Struct({ handle: ExecutionHandle, message: Schema.Json })

export const AgentMessageInput = Schema.Struct({
  text: Schema.optionalKey(Schema.String), content: Schema.optionalKey(MessageContent), budget: Schema.optionalKey(TurnBudget),
}).pipe(Schema.refine((input): input is typeof input & ({ readonly text: string; readonly content?: never } | { readonly text?: never; readonly content: MessageContent }) =>
  (input.text === undefined) !== (input.content === undefined), { message: "exactly one of text or content is required" }))
export const agentMethods = {
  message: actorMethod({
    inputSchema: AgentMessageInput, outputSchema: AgentMessageOutput,
    onReceive: TurnRequested.from((input, context) => ({
      text: input.text ?? input.content!.filter((part) => part.type === "text").map((part) => part.text).join(""),
      ...(input.content === undefined ? {} : { content: input.content }),
      ...(input.budget === undefined ? {} : { budget: input.budget }),
      source: "user", turnId: context.id, invocationRef: context.ref,
    })),
    result: (_, get, context) => agentReply(context.id, get),
    onCancel: AbortRequested.from((_, context) => ({ ref: context.ref, reason: context.reason })),
  }),
  // request starts a parent turn for a thread child's request; Actor.reply records the answer that becomes its result.
  request: actorMethod({
    inputSchema: ChildRequest, outputSchema: Schema.Json,
    onReceive: TurnRequested.from((input, context) => ({ text: `Child request (data): ${JSON.stringify(input)}`, source: "agent", turnId: context.id, invocationRef: context.ref })),
    result: (_, get, context) => requestReply(context.id, get),
    onCancel: AbortRequested.from((_, context) => ({ ref: context.ref, reason: context.reason })),
  }),
  // notice delivers a thread child's message as a parent turn and completes on receipt.
  notice: actorMethod({
    inputSchema: ChildNotice, outputSchema: Schema.Null,
    onReceive: TurnRequested.from((input, context) => ({ text: JSON.stringify(input), source: "agent", turnId: context.id })),
    result: () => ({ status: "completed", output: null }),
  }),
}
