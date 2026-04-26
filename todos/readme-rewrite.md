# README rewrite

**Status:** implemented
**Updated:** 2026-04-26

## Motivation

A first-time-reader audit of README.md surfaced 18 items that confuse, repeat, or drive evaluators away. The README has accreted as the design evolved; it now reads more like an internal design doc than an entry point for new users. This plan tracks the cleanup pass.

## Items

### Terminology and consistency

- [x] **Pick one term for "the source of items"** — standardized on "source" everywhere. Dropped "producer", "external source", "items source", "source iterator". Kept "source generator" only when being specific about the type, and "Item source" in diagram labels for visual clarity.
- [x] **Explain why `run(items)` not `run(items())`** — added a callout in the Bounded subsection of "Driving a flow" explaining that the framework owns iteration, plus the runtime contract in DESIGN.md and an item in `implement-runtime.md` to enforce with a helpful error message.

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

- [x] **Naming example uses `flow(reader, ...)` where `reader` is an async generator** — fixed by removing `reader` from the `flow()` args; the example now shows just `flow(stage(normalize, name="parse"), db_write)`.

### Vocabulary and pacing

- [x] **Heavy jargon appears before basic concepts** — replaced framework-coined terms with plain English: "call-shape transformers" → "per-item transformers"; "graph fragments" → "sub-pipelines"; "auto-trigger" → "no source"; "namespaced stage names" → "stage names in the parent". Updated everywhere (Stages, Composing flows, Routing, Architecture diagram + match block, Public API table, lifecycle pointers). "Type-level separation" deferred — its whole subsection is on the chopping block in item 18.

### Reference quality

- [x] **`UtilizationScaling` parameters are unexplained** — added inline comments to every parameter in the constructor example, plus a "How it works" section explaining the sampling+cooldown gates and the scale-up/scale-down logic, plus a "Tuning guide" with four common scenarios (bursty, steady, expensive workers, high-throughput).
- [x] **`Last(value)` is in the API table but never explained in prose** — added a new "Stopping from inside a transformer — `Last(value)`" subsection in "Driving a flow", right before the existing "Stopping a running flow" subsection. Covers use case (terminator-marker pattern), code example, what the user observes, the guarantee, and the `Dropped(...,UPSTREAM_TERMINATED)` events for dropped upstream items.
- [x] **Clarify `set_error_handler` vs constructor option** — both forms now documented. Constructor accepts `on_error=`, `default_scaling=`, `default_queue=` as shorthand for the corresponding methods. Per-stage configuration remains method-only. Updated DESIGN.md (DSL section), README "Configuring a flow" (split into Constructor keywords / Methods subsections), Public API table, and added an implementation item to `implement-runtime.md`.

### Production / debugging

- [x] **Add a debugging / dev story.** Both items needed for this — README example showing `dump()` output, and a "debugging" subsection that points to `dump()` as the first thing to try — already exist in [`dump-implementation.md`](dump-implementation.md) under its Documentation section. They'll be done as part of building `dump()` (real output to show, not hypothetical).
- [x] **No troubleshooting guide.** Moved to [`implement-runtime.md`](implement-runtime.md) Polish section. Will be written based on real failure modes encountered during implementation and testing — writing it now without a runtime to validate against would be speculative.

### Polish

- [x] **Markdown rendering risk in tables** — only one instance: `flow.dump(mode="structure" \| "stats")`. Rewrote as `flow.dump(mode=...)` with prose describing the two modes in the same cell. No escaped pipes remain.
- [x] **Push-mode "Why a separate `PushHandle`?" subsection** — moved the rationale ("Why a separate PushHandle type") to DESIGN.md under the Drive modes section. The README's "Driving a flow" intro now says only the user-relevant fact ("Push mode returns a `PushHandle`; `send()` and `complete()` live on the handle"). Same thing in the "type-level separation" sentence — trimmed.
- [x] **Consider whether two mermaid diagrams in Architecture are too much** — moved the 11-entity class diagram to DESIGN.md ("Component class diagram" subsection). README's "Component overview" subsection is now a short paragraph pointing to DESIGN.md and noting that Pipeline Flow below is enough for day-to-day use. Pipeline Flow diagram kept in README.

## Done

(none yet)
