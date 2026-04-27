# Flowrhythm

Asynchronous job processing framework for Python with dynamic worker scaling.
Pipeline DSL for chaining async stages (producer → transformers → sink) with branching, auto-scaling, and error handling.

## Development

- **Install:** `uv sync`
- **Test:** `make test` or `uv run pytest`
- **Coverage:** `make cov`
- **Lint:** `make lint` (ruff)
- **Build:** `uv build`
- **Publish:** `uv publish` — token in 1Password: `Servers / flowrhythm PyPI publish token`. Retrieve via `op read 'op://Servers/flowrhythm PyPI publish token/credential'`. For one-off publish: `UV_PUBLISH_TOKEN=$(op read ...) uv publish`. Account-wide initially; scope to project after first successful publish.

## Architecture

- Read README.md "Architecture" section before proposing any structural changes
- Proposals must align with the design principles described there
- Update README architecture diagrams when adding/removing/renaming components

## README writing

- README is the entry point for developers who have **never used this library before** and are not familiar with stream processing or async pipelines
- Assume no prior context about flowrhythm or its design choices
- For every constraint or non-obvious behavior, explain **why** it exists, not just what it is
- For every limitation, give a **recommended workaround** with a worked example
- **Always illustrate with concrete code examples** — shapes, signatures, and prose are not enough on their own
- Prefer "Why? / What to do instead" subsections over single-line caveats

### Describe behavior from the user's perspective

- Document what the user **writes**, what they **observe**, and what the framework **guarantees** — never how it's implemented
- Words like *queue*, *worker*, *sentinel*, *close()* are implementation details. Avoid them in user-facing docs unless the user can directly observe or configure them (e.g., they can configure a queue *type*, but they don't see queue close events)
- When explaining a feature, write each step as either "the user does X" or "the user observes Y", not "the framework calls Z internally"
- Implementation mechanics belong in DESIGN.md or code comments, not in the README

## Project documents

There are three layers of documentation, each with its own audience:

| File | For | What it contains |
|---|---|---|
| `README.md` | Users | How to use the library |
| `DESIGN.md` | Implementers | Design contract — how things should behave, why decisions were made, open questions, deferred ideas |
| `todos/INDEX.md` + plan files | Maintainers | Concrete implementation work, in priority order, with checklists |

### DESIGN.md

- The source of truth for **how the library should behave**
- Read it at the start of any non-trivial design or implementation work
- Update it when: a design decision is made, an open question is resolved, a deferred idea is reconsidered
- Do **not** put implementation tasks here — those go in `todos/`
- DESIGN.md says "the rule"; `todos/` says "what we're doing about it"

## TODO plans

- `todos/` holds individual TODO plan files; `todos/INDEX.md` is the index, listed in **priority order (top is highest)**
- Each plan is a markdown file with a status header (`planned` | `in-progress` | `implemented`) and a checklist of items

### When to read

- **Before starting any non-trivial task**, scan `todos/INDEX.md` to see if an existing plan covers it. If so, work from that plan.
- When the user asks "what's left?" or "what should we work on next?", consult the index — top-of-list is highest priority.

### When to create

- When the user proposes substantial new work (multiple steps, multiple files, design decisions to make), create a new plan file before starting. Add it to the index in the right priority slot.
- Skip a plan for trivial one-shots (one file change, no design discussion) — those don't need plans.

### When to update

- When starting a plan: flip status to `in-progress` in both the file and the index.
- When checking off an item: add a brief note about how/where it was done — the file becomes a record, not just a checklist.
- When all items are done: flip status to `implemented` in both the file and the index.
- When priority changes (something becomes more or less important): reorder rows in the index.

### Never delete

- Implemented plans stay as a record of what was decided and shipped. Never delete a plan file.

## Asking questions (doubt protocol)

When you hit a real ambiguity that needs a user decision (an API shape, a behavioral default, a trade-off between two equally valid designs):

- **Number questions uniquely** when there's more than one open at once (Q1, Q2, …). Makes it easy for the user to answer in any order: "Q1: A. Q3: skip."
- **Give 2–3 concrete options**, each with a short code/usage example so the user sees the call site, not just an abstract description.
- **List pros/cons per option** on the axes that matter (UX, performance, complexity, maintenance burden, escape hatches).
- **Recommend one with rationale.** Don't just present a menu — say which you'd pick and why. The user can override; defaulting to "you decide" is unhelpful.
- **Wait for the answer before acting.** If the question is non-trivial, don't pre-emptively implement option A "to save time" — the rework cost when the user picks B is higher than the wait.

Don't invoke the doubt protocol for trivial choices (variable names, comment phrasing, which file to put a one-off helper in). Pick a reasonable default and move on.

## Milestone exit

When the user signals that a milestone, phase, or chunk of work is complete (e.g. "M5 is done", "let's wrap up", "ready to commit"), read and execute [`docs/milestone-exit.md`](docs/milestone-exit.md). It's a checklist that gates code, aligns DESIGN ↔ README ↔ code, updates plans, and prepares the commit.

Also useful before opening a PR or publishing a release.

## Conventions

### Typing & interfaces
- Protocols for all interfaces (duck typing, no ABCs or inheritance)
- Full type annotations on all public API (parameters + return types); internals optional
- This library is designed to be consumed by LLMs — clarity in public API matters

### Docstrings
- Plain/minimal one-liner docstrings on public API
- Usage examples on key entry points (Builder, Flow), not every method
- Type annotations carry the parameter/return documentation

### Documenting non-obvious internal design decisions

When you make a design decision that future contributors would otherwise have to re-derive — choosing a dataclass over parallel lists, a state-driven completion mechanism over `gather`, a sync protocol over `async`, etc. — document it in **three places**, each owning a different layer:

1. **DESIGN.md owns "why"** — the architectural rationale, alternatives considered, trade-offs. Add a subsection under the relevant existing section. This is the canonical home; other places point here.
2. **Class / function docstrings own "what + pointer to why"** — describe the role of the thing in 2-4 sentences, then end with `See DESIGN.md "<section name>" for the rationale.` A reader of the code finds the link to the rationale without needing to know the doc structure.
3. **Inline comments at decision sites own "here's where the why applies"** — one short comment at the construction site, the worker loop, the place where the pattern is enforced. Points back at DESIGN.md by section name.

This three-tier pattern keeps the rationale findable from any entry point without duplicating it. If you find yourself explaining the same "why" in code review or in a PR description, that's the signal — it belongs in DESIGN.md.

Don't apply this to obvious choices (using `set()` for unique items, naming a variable `count`). Only when "why" isn't visible from the code alone.

### Code style
- Python >=3.13, asyncio-native, zero runtime dependencies (3.13 chosen for stdlib `asyncio.Queue.shutdown()`)
- f-strings in main code; lazy `%` formatting in logging
- Dataclasses by default; NamedTuples only for trivial immutable values
- `match/case` where cleaner than if/elif
- Keep functions simple and laconic; avoid deep nesting
- Raise built-in exceptions (ValueError, TypeError) — no custom hierarchy
- `_prefix.py` modules signal internal; no need to double-prefix names within them

### Testing
- All tests are async — pytest-asyncio with `asyncio_mode = "auto"`
- Real objects by default; mocks only for timing/error simulation
- Inline setup per test, no fixtures
- Ruff for linting; ignores F841 (unused vars) and E701 (multiple statements)

#### Async testing strategy
- **Never use `wait_for(..., timeout=N)` to prove a coroutine "doesn't complete."** It costs N wall-clock per test and is flaky on slow CI. Use one of:
  - **`x_nowait()` + matching exception** — `put_nowait()` / `QueueFull`, `get_nowait()` / `QueueEmpty`. Zero time, exact semantic match for "this would block."
  - **State inspection** — `q.full()`, `q.qsize()`, `task.done()`. Cheaper than waiting; reads what's actually true.
  - **Task introspection** — when you must prove "this coroutine is blocked": `task = asyncio.create_task(coro); await asyncio.sleep(0); assert not task.done(); task.cancel()`. The `sleep(0)` yields exactly one event-loop tick — long enough for tasks to reach their first await, no real time spent.
- **`await asyncio.sleep(0)` is the canonical idiom for "let pending tasks reach their await point."** Use it (not `sleep(0.01)` or larger) when coordinating multiple tasks in a test.
- **Time mocking** (freezegun, custom event loop) is only worth the setup cost when the *thing being tested* is time-based logic — e.g., `cooldown_seconds` or `sampling_period`. For "queue is full," "task is blocked," "shutdown unblocks waiters" — don't reach for time mocking; use the techniques above.
- **Coordinating concurrent tasks**: don't use `asyncio.sleep(N)` to "let other workers pick up items." Use a barrier — `asyncio.Event` set when the desired condition is reached, with workers awaiting it. Zero wall-clock time, deterministic.
- **`asyncio.timeout(N)` IS valid as a safety net** (e.g., `async with asyncio.timeout(2): await chain.run(items)`) to prevent a buggy test from hanging the suite. This is a different use case from `wait_for(timeout)` to prove non-completion — here the timeout should never fire on a correct implementation; it's a "fail fast on hang" guard, not a measurement.

#### Async error handling
- **Catch `Exception`, never `BaseException`** in `try/except` blocks that wrap user code or async work. `asyncio.CancelledError` extends `BaseException` (not `Exception`) precisely so `except Exception` doesn't swallow it. Catching `BaseException` breaks `task.cancel()`, `Flow.stop()`, and any cooperative cancellation.
- The one exception is when cancellation IS the deliberate signal at that point — e.g., a worker awaiting `queue.get()` catches `CancelledError` because cancellation there means "retire cleanly per the worker pool protocol." Document the exception explicitly when doing this.
- `KeyboardInterrupt`, `GeneratorExit`, and `CancelledError` are all `BaseException` subclasses; all should propagate by default in async code.

### Git
- Conventional commits (`feat:`, `fix:`, `refactor:`, etc.)
- Logical chunks — one commit per complete idea
- Never push unless all tests pass (`make test`)
- Ask before significant rewrites of existing code
