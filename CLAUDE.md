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

- `flowrhythm/flow.py` — Core Flow orchestrator (wiring, lifecycle: start/wait/stop)
- `flowrhythm/_builder.py` — Fluent DSL for constructing pipeline graphs
- `flowrhythm/_stage.py` — Stage abstractions: SinkStage, TransformerStage, BranchStage
- `flowrhythm/_scaling.py` — ScalingStrategy protocol, FixedScaling, StageStats
- `flowrhythm/_utilizationscaling.py` — Utilization-based auto-scaling
- `flowrhythm/_types.py` — Type aliases and protocols (Producer, Transformer, Sink, Branch, ErrorHandler)
- `flowrhythm/_queue.py` — Queue factory (FIFO, LIFO, Priority)

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
- Ask before significant rewrites of existing code
