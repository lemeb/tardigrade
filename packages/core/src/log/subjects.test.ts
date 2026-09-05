import { describe, expect, test } from "bun:test"
import type { Event } from "@clavia/tardigrade-core/log/event"
import { boundaryId, messageSubjects, replySubjectOf } from "../communication/message"
import { composeSubjects, type SubjectFragment } from "./subjects"

const aSubjects: SubjectFragment = {
  prefixes: ["a:"],
  subjectOf: (e) => (e.type === "A" ? `a:${String((e as { id?: unknown }).id)}` : undefined)
}

describe("composeSubjects", () => {
  test("first answer wins and unclaimed types answer nothing", () => {
    const subjectOf = composeSubjects(messageSubjects, aSubjects)
    expect(subjectOf({ type: "MessageReceived", id: "m1", text: "", at: 0 })).toBe("msg:m1")
    expect(subjectOf({ type: "A", id: "x", at: 0 })).toBe("a:x")
    expect(subjectOf({ type: "ModelCalled", at: 0 })).toBeUndefined()
  })

  test("a prefix claimed twice dies at construction", () => {
    const rival: SubjectFragment = { prefixes: ["a:"], subjectOf: () => undefined }
    expect(() => composeSubjects(aSubjects, rival)).toThrow('subject prefix "a:" claimed by fragments 0 and 1')
  })
})

describe("messageSubjects", () => {
  test("a plain message is addressable by its id and a message without one is not", () => {
    expect(messageSubjects.subjectOf({ type: "MessageReceived", id: "m1", text: "go", at: 0 })).toBe("msg:m1")
    expect(messageSubjects.subjectOf({ type: "MessageReceived", text: "no id", at: 0 })).toBeUndefined()
    expect(messageSubjects.subjectOf({ type: "ToolCalled", callId: "c1", at: 0 })).toBeUndefined()
  })

  test("a reply subject names the outbound id it answers", () => {
    expect(replySubjectOf(boundaryId("call-1", 0))).toBe("reply:call-1")
    expect(replySubjectOf(boundaryId("call-1", 2))).toBe("reply:call-1")
    expect(replySubjectOf("m1")).toBeUndefined()
    const reply: Event = { type: "MessageReceived", id: "call-1.reply", text: "done", at: 0 }
    expect(messageSubjects.subjectOf(reply)).toBe("reply:call-1")
    const response: Event = { type: "ResponseReceived", id: "call-1.reply.1", method: "message", call: "call-1", status: "completed", from: "a:main:t1", at: 0 }
    expect(messageSubjects.subjectOf(response)).toBe("reply:call-1")
  })
})