# README rewrite

**Status:** planned
**Updated:** 2026-04-26

## Motivation

A first-time-reader audit of README.md surfaced 18 items that confuse, repeat, or drive evaluators away. The README has accreted as the design evolved; it now reads more like an internal design doc than an entry point for new users. This plan tracks the cleanup pass.

## Items

### Terminology and consistency

- [ ] **Pick one term for "the source of items"** — currently mixes "producer", "source", "source iterator", "source generator", "items source", "external source." Pick one (probably "source") and use it everywhere.
- [ ] **Explain why `run(items)` not `run(items())`** — every example passes the function reference, never the call. Add one-line note (e.g., "framework controls iteration / can re-iterate"), or change the API to accept the called generator if simpler.

### Quick Start and onboarding

- [ ] **Quick Start should be a 5-line example.** Currently it has 5 functions and a router. Lead with `flow(transform).run(source)`. Routing belongs in a second example.
- [ ] **Add a "When to use / vs alternatives" section.** Compare to Celery, Faust, raw asyncio.Queue. Without this, new users default to what they know.
- [ ] **Move or rephrase the Scope callout.** Currently the first thing after the tagline is "what flowrhythm is NOT for." Lead with what it IS for, then narrow scope.

### Duplication and structure

- [ ] **Routing is explained twice** — Stages → Router subsection and Designing a flow → Routing subsection. Pick one canonical location, link from the other.
- [ ] **Sub-flow composition is explained 2-3 times** — Stages → Transformer table, Stages → Sub-flow section, Designing a flow → Composing flows. Consolidate.
- [ ] **CM factory examples appear in 3 sections** — Stages → Transformer Examples, Stages → How a worker invokes → CM factory. Pick one and link.

### Implementation leakage in user-facing prose

- [ ] **Remove pseudocode with `my_queue.get()` / `next_queue.put()` / `running` flag** from "How a worker invokes each shape." This is implementation detail, contradicts CLAUDE.md "describe behavior from the user's perspective" rule. Replace with a user-perspective description.

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
- [ ] **Push-mode "Why a separate `PushHandle`?" subsection** is design rationale that doesn't help users — move to ROADMAP, leave a one-liner in README.
- [ ] **Consider whether two mermaid diagrams in Architecture are too much** — the class diagram has 8+ entities and signals "complex framework" to evaluators. Either simplify or remove the class diagram (keep the Pipeline Flow one).

## Done

(none yet)
