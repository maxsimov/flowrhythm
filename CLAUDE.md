# Flowrhythm

Asynchronous job processing framework for Python with dynamic worker scaling.
Pipeline DSL for chaining async stages (producer → transformers → sink) with branching, auto-scaling, and error handling.

## Development

- **Install:** `uv sync`
- **Test:** `make test` or `uv run pytest`
- **Coverage:** `make cov`
- **Lint:** `make lint` (ruff)
- **Build:** `uv build`
- **Publish:** `uv publish`

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
- Implementation mechanics belong in ROADMAP or code comments, not in the README

## Roadmap

- ROADMAP.md tracks decided design, open questions, and implementation backlog
- Read it at the start of any non-trivial design or implementation work to know current state
- Update it when: a design decision is made, an open question is resolved, work moves between backlog/done, or new work is identified
- Move completed items to the "Done" section rather than deleting

## Conventions

### Typing & interfaces
- Protocols for all interfaces (duck typing, no ABCs or inheritance)
- Full type annotations on all public API (parameters + return types); internals optional
- This library is designed to be consumed by LLMs — clarity in public API matters

### Docstrings
- Plain/minimal one-liner docstrings on public API
- Usage examples on key entry points (Builder, Flow), not every method
- Type annotations carry the parameter/return documentation

### Code style
- Python >=3.12, asyncio-native, zero runtime dependencies
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

### Git
- Conventional commits (`feat:`, `fix:`, `refactor:`, etc.)
- Logical chunks — one commit per complete idea
- Never push unless all tests pass (`make test`)
- Ask before significant rewrites of existing code
