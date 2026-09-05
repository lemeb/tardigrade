import { describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect, ManagedRuntime } from "effect"
import { SqlClient } from "effect/unstable/sql"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import type { Event } from "@clavia/tardigrade-core/log/event"
import { threadKeys } from "@clavia/tardigrade-core/thread"
import {
  CloudflareEventStore,
  hmacSha256EventKeyIndex,
  plaintextEventCodec,
  plaintextEventKeyIndex
} from "./storage"

// The store over any SQLite client, not only a Durable Object storage binding: its guarantees
// live in the statements, so a bun:sqlite file proves them as well as a deployed object would.

const dir = mkdtempSync(join(tmpdir(), "tardigrade-cf-storage-"))
let n = 0
const freshPath = (): string => join(dir, `store-${n++}.sqlite`)

interface Opened {
  readonly store: CloudflareEventStore
  readonly dispose: () => Promise<void>
}

const open = async (
  path: string,
  policy: { readonly indexKey?: CloudflareEventStore["indexKey"] } = {}
): Promise<Opened> => {
  const runtime = ManagedRuntime.make(SqliteClient.layer({ filename: path }))
  const sql = await runtime.runPromise(SqlClient.SqlClient)
  return {
    store: new CloudflareEventStore(sql, threadKeys.keyOf, plaintextEventCodec, policy.indexKey ?? plaintextEventKeyIndex),
    dispose: () => runtime.dispose()
  }
}

const message = (id: string, at: number): Event => ({ type: "MessageReceived", id, text: `text ${id}`, at })
const created: Event = { type: "ThreadCreated", address: { actor: "echo", instance: "main", thread: "t1" }, depth: 0, at: 0 }

describe("CloudflareEventStore exact-fact reads", () => {
  test("a key lookup answers the event a durable key names", async () => {
    const opened = await open(freshPath())
    await Effect.runPromise(opened.store.initialize())
    await Effect.runPromise(opened.store.append([created, message("m1", 1)]))
    expect(await Effect.runPromise(opened.store.readKey("thread:created"))).toEqual({
      seq: 1,
      event: created
    })
    // A message is not keyed in the store, so only its subject names it.
    expect(await Effect.runPromise(opened.store.readKey("msg:m1"))).toBeUndefined()
    await opened.dispose()
  })

  test("a subject lookup answers the latest event naming it", async () => {
    const opened = await open(freshPath())
    await Effect.runPromise(opened.store.initialize())
    await Effect.runPromise(opened.store.append([created, message("m1", 1), message("out-1.reply", 2)]))
    expect(await Effect.runPromise(opened.store.readSubject("msg:m1"))).toEqual({
      seq: 2,
      event: message("m1", 1)
    })
    // A later boundary round under the same subject supersedes the round before it.
    await Effect.runPromise(opened.store.append([message("out-1.reply.2", 3)]))
    expect(await Effect.runPromise(opened.store.readSubject("reply:out-1"))).toEqual({
      seq: 4,
      event: message("out-1.reply.2", 3)
    })
    expect(await Effect.runPromise(opened.store.readSubject("reply:never-sent"))).toBeUndefined()
    await opened.dispose()
  })

  test("an empty log answers nothing for any coordinate", async () => {
    const opened = await open(freshPath())
    await Effect.runPromise(opened.store.initialize())
    expect(await Effect.runPromise(opened.store.readKey("thread:created"))).toBeUndefined()
    expect(await Effect.runPromise(opened.store.readSubject("msg:m1"))).toBeUndefined()
    await opened.dispose()
  })

  test("a sealed deployment answers indexed facts without holding coordinate text", async () => {
    const path = freshPath()
    const key = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode("abcdef0123456789abcdef0123456789"),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"]
    )
    const opened = await open(path, { indexKey: hmacSha256EventKeyIndex(key, "main:t1") })
    await Effect.runPromise(opened.store.initialize())
    await Effect.runPromise(opened.store.append([created, message("out-1.reply", 2)]))
    const raw = new Database(path, { readonly: true })
    try {
      const subjects = raw.query("SELECT subject FROM event_subjects").all() as ReadonlyArray<{ readonly subject: string }>
      expect(subjects.every((row) => /^hmac-sha256:[a-f0-9]{64}$/.test(row.subject))).toBe(true)
    } finally {
      raw.close()
    }
    expect(await Effect.runPromise(opened.store.readSubject("reply:out-1"))).toEqual({
      seq: 2,
      event: message("out-1.reply", 2)
    })
    expect(await Effect.runPromise(opened.store.readKey("thread:created"))).toEqual({ seq: 1, event: created })
    await opened.dispose()
  })

  test("a pre-existing log answers indexed facts after upgrade", async () => {
    // A database from before the subject table: the old schema, its migrations recorded, and
    // events the index has never seen.
    const path = freshPath()
    const old = new Database(path)
    try {
      old.exec(`CREATE TABLE events (
        seq INTEGER NOT NULL,
        key TEXT,
        event TEXT NOT NULL,
        PRIMARY KEY (seq)
      ) WITHOUT ROWID`)
      old.exec("CREATE UNIQUE INDEX events_key ON events (key) WHERE key IS NOT NULL")
      old.exec(`CREATE TABLE effect_sql_migrations (
        migration_id integer PRIMARY KEY NOT NULL,
        created_at datetime NOT NULL DEFAULT current_timestamp,
        name VARCHAR(255) NOT NULL
      )`)
      old.run("INSERT INTO effect_sql_migrations (migration_id, name) VALUES (1, '0001_thread_identity')")
      old.run("INSERT INTO effect_sql_migrations (migration_id, name) VALUES (2, '0002_thread_events')")
      old.run("INSERT INTO events (seq, key, event) VALUES (1, ?, ?)", ["thread:created", JSON.stringify(created)])
      old.run("INSERT INTO events (seq, key, event) VALUES (2, NULL, ?)", [JSON.stringify(message("m1", 1))])
      old.run("INSERT INTO events (seq, key, event) VALUES (3, NULL, ?)", [JSON.stringify(message("out-1.reply", 2))])
    } finally {
      old.close()
    }
    const opened = await open(path)
    await Effect.runPromise(opened.store.initialize())
    expect(await Effect.runPromise(opened.store.readSubject("msg:m1"))).toEqual({ seq: 2, event: message("m1", 1) })
    expect(await Effect.runPromise(opened.store.readSubject("reply:out-1"))).toEqual({ seq: 3, event: message("out-1.reply", 2) })
    expect(await Effect.runPromise(opened.store.readKey("thread:created"))).toEqual({ seq: 1, event: created })
    await opened.dispose()
  })

  test("a fresh store instance over the same storage answers after a reopen", async () => {
    const path = freshPath()
    const first = await open(path)
    await Effect.runPromise(first.store.initialize())
    await Effect.runPromise(first.store.append([created, message("m1", 1)]))
    await first.dispose()
    // A Durable Object eviction is exactly this: the same durable storage, a store that starts
    // over. Its initialize pass is what keeps the answers standing.
    const second = await open(path)
    await Effect.runPromise(second.store.initialize())
    expect(await Effect.runPromise(second.store.readSubject("msg:m1"))).toEqual({ seq: 2, event: message("m1", 1) })
    await Effect.runPromise(second.store.append([message("out-1.reply", 3)]))
    expect(await Effect.runPromise(second.store.readSubject("reply:out-1"))).toEqual({
      seq: 3,
      event: message("out-1.reply", 3)
    })
    await second.dispose()
  })

  test("the capture pass is idempotent and absorbs a re-run", async () => {
    const opened = await open(freshPath())
    await Effect.runPromise(opened.store.initialize())
    await Effect.runPromise(opened.store.append([created, message("m1", 1)]))
    await Effect.runPromise(opened.store.initialize())
    await Effect.runPromise(opened.store.initialize())
    expect(await Effect.runPromise(opened.store.readSubject("msg:m1"))).toEqual({ seq: 2, event: message("m1", 1) })
    await opened.dispose()
  })
})