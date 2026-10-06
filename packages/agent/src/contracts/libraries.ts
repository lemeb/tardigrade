import { RuntimeError } from "@clavia/tardigrade-core"
import { type LibraryContract, type LibrarySource, type ToolSpec } from "@clavia/tardigrade-libraries/types"
import { renderSignature, renderShape } from "@clavia/tardigrade-code/execution/contract"

// selectLibraries resolves declared libraries against the host's configured contracts.
export function selectLibraries(sources: readonly LibrarySource[], configured: readonly LibraryContract[]): readonly LibraryContract[] {
  const names = sources.map(source => "library" in source ? source.library.name : source.name)
  if (new Set(names).size !== names.length) throw new RuntimeError("Duplicate library name")
  return names.map(name => {
    const library = configured.find(library => library.name === name)
    if (!library) throw new RuntimeError(`Library is not configured in host services: ${name}`)
    return library
  })
}

// codeModeSpec describes library methods, whose background calls resolve to their settled results, and combines their behaviour hints conservatively.
export function codeModeSpec(libraries: readonly LibraryContract[], signatureDepth?: number): ToolSpec {
  const methods = libraries.flatMap(library => library.specs)
  for (const library of libraries) {
    if (["console", "Date", "Math"].includes(library.name)) throw new RuntimeError(`Reserved code binding: ${library.name}`)
  }
  const description = [
    "Run an async JavaScript body against the connected libraries. Await library methods and end with return <value>. The result includes console logs. Library calls must occur in the same order with the same arguments during replay.",
    ...libraries.map(library => [`${library.name}: ${library.description}`, ...library.specs.map(method =>
      `  ${library.name}.${renderSignature(method.method, method.inputSchema, signatureDepth)} -> ${method.execution === "background" ? "settled result (background: awaiting it suspends the body, which then runs again from the top)" : renderShape(method.outputSchema, signatureDepth)}: ${method.description}${method.annotations ? ` [annotations: ${JSON.stringify(method.annotations)}]` : ""}`)].join("\n")),
  ].join("\n")
  return {
    name: "execute", description, inputSchema: {
      type: "object", properties: { code: { type: "string" } }, required: ["code"], additionalProperties: false,
    }, annotations: {
      readOnlyHint: methods.every(method => method.annotations?.readOnlyHint === true),
      destructiveHint: methods.some(method => method.annotations?.readOnlyHint !== true && method.annotations?.destructiveHint !== false),
      idempotentHint: methods.every(method => method.annotations?.readOnlyHint === true || method.annotations?.idempotentHint === true),
      openWorldHint: methods.some(method => method.annotations?.openWorldHint !== false),
    }, execution: "foreground",
  }
}
