# README rewrite

**Status:** in-progress
**Updated:** 2026-04-26

## Motivation

A first-time-reader audit of README.md surfaced 18 items that confuse, repeat, or drive evaluators away. The README has accreted as the design evolved; it now reads more like an internal design doc than an entry point for new users. This plan tracks the cleanup pass.

## Items

### Terminology and consistency

- [x] **Pick one term for "the source of items"** — standardized on "source" everywhere. Dropped "producer", "external source", "items source", "source iterator". Kept "source generator" only when being specific about the type, and "Item source" in diagram labels for visual clarity.
- [x] **Explain why `run(items)` not `run(items())`** — added a callout in the Bounded subsection of "Driving a flow" explaining that the framework owns iteration, plus the runtime contract in DESIGN.md and an item in `migrate-to-flow.md` to enforce with a helpful error message.

### Quick Start and onboarding

- [x] **Quick Start should be a 5-line example.** Replaced with a 3-function minimal example (`double`, `write`, `items`) and a single `flow(double, write).run(items)` call. Routing/scaling/etc. now point to dedicated sections.
- [x] **Add a "When to use / vs alternatives" section.** Added between Scope and Contents. Three-question framing: when it fits / when it doesn't, then "why not asyncio.Queue / Celery / Faust." Linked from the Contents.
- [x] **Move or rephrase the Scope callout.** Deleted entirely. The new "When to use flowrhythm" section that follows the tagline covers the same ground (CPU-bound warning is in the "wrong fit" bullets) with positive framing first.

### Duplication and structure

- [x] **Routing is explained twice** — consolidated at "Designing a flow → Routing" (now the canonical location with signature, params, example, runtime graph, and miss behavior). Stages → Router subsection deleted; the "How a worker invokes" Router entry shrunk to a one-line pointer to Routing. Stages → Transformer table row links to Routing.
- [x] **Sub-flow composition is explained 2-3 times** — consolidated at "Designing a flow → Composing flows" (effective graph + namespacing + standalone-vs-composed). Stages → Sub-flow shrunk to a one-line pointer. Deleted the redundant "Reusable as a transformer" subsection in Designing a flow. Stages → Transformer table row links to Composing flows.
- [x] **CM factory examples appear in 3 sections** — restructured Stages → Transformer "Examples" block: replaced one big mashup (5 things wired together, including unused `WithDB` class) with one focused snippet per shape. Each shape now has its own minimal, self-contained example. Sub-flow and router examples are one-liners that point to their canonical sections (Composing flows / Routing). Driving a flow → "Source can be a CM factory" stays — it's a different topic.

### Implementation leakage in user-facing prose

- [x] **Remove pseudocode with `my_queue.get()` / `next_queue.put()` / `running` flag** from "How a worker invokes each shape." Renamed section to "How each shape behaves at runtime." Each subsection now describes what the user observes and what the framework guarantees, with no implementation pseudocode. Router pseudocode was already removed in item 6.

### Bugs in examples

- [ ] **Naming example uses `flow(reader, ...)` where `reader` is an async generator** — would raise `TypeError` per current design rules. Fix the example.

### Vocabulary and pacing

- [ ] **Heavy jargon appears before basic concepts** — "graph fragment", "call-shape", "type-level separation", "namespaced sub-stages" all show up before the user has built a working pipeline. Defer jargon to deeper sections.

### Reference quality

- [ ] **`UtilizationScaling` parameters are unexplained** — 9 params with no per-param meaning. Add a parameter table: what each does, sensible defaults, when to tune.
- [ ] **`Last(value)` is in the API table but never explained in prose** — it's load-bearing (in Guarantees) but has no narrative section. Add a "Stopping the chain from inside a transformer" subsection.
- [ ] **Clarify `set_error_handler` vs constructor option** — is there a `flow(stages, on_error=...)` form? Document either way.

### Production / debugging

- [ ] **Add a debugging / dev story.** `dump()` is mentioned but never shown. Add an example of dump output in both modes.
- [ ] **No troubleshooting guide.** What happens when the pipeline misbehaves? Common failure modes and how to diagnose.

### Polish

- [ ] **Markdown rendering risk in tables** — escaped `\|` pipes in cells like `dump(mode="structure" \| "stats")` look ugly. Rewrite as separate column or note.
- [ ] **Push-mode "Why a separate `PushHandle`?" subsection** is design rationale that doesn't help users — move to DESIGN.md, leave a one-liner in README.
- [ ] **Consider whether two mermaid diagrams in Architecture are too much** — the class diagram has 8+ entities and signals "complex framework" to evaluators. Either simplify or remove the class diagram (keep the Pipeline Flow one).

## Done

(none yet)
