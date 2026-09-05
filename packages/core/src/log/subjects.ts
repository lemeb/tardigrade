import type { Event } from "@clavia/tardigrade-core/event"

/**
 * SubjectFragment derives stable read subjects for one event alphabet.
 *
 *   SubjectFragment
 *     ├── prefixes   namespaces claimed by the fragment
 *     └── subjectOf  subject derived from an event
 *
 * A key names one occurrence; a subject names one fact, and a later event under
 * the same subject supersedes the earlier one. The read index holds the latest
 * event per subject, so duplicate prefixes across fragments throw during
 * composition exactly as they do for keys (keys.ts, composeKeys).
 */
export interface SubjectFragment {
  readonly prefixes: ReadonlyArray<string>
  readonly subjectOf: (e: Event) => string | undefined
}

// composeSubjects combines disjoint subject fragments into one read-subject
// derivation. Duplicate prefixes throw during construction.
export const composeSubjects = (...fragments: ReadonlyArray<SubjectFragment>): ((e: Event) => string | undefined) => {
  const claimed = new Map<string, number>()
  fragments.forEach((fragment, i) => {
    for (const prefix of fragment.prefixes) {
      const prior = claimed.get(prefix)
      if (prior !== undefined) {
        throw new Error(`subject prefix "${prefix}" claimed by fragments ${prior} and ${i}`)
      }
      claimed.set(prefix, i)
    }
  })
  return (e) => {
    for (const fragment of fragments) {
      const subject = fragment.subjectOf(e)
      if (subject !== undefined) return subject
    }
    return undefined
  }
}