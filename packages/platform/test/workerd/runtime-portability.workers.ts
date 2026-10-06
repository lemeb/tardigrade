import { test } from "vitest"
import { propertyCases } from "../properties/suite"

// Some cases reopen each run at every commit boundary, which exceeds the default 5 s test timeout on slow hosts.
for (const [name, run] of Object.entries(propertyCases)) test(`runtime portability: ${name}`, run, 30_000)
