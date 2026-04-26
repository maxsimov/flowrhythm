# Roadmap

Tracks design decisions, open questions, and implementation work for flowrhythm.

---

## Decided design

### DSL
- Single entry point: `flow(*stages)` — lowercase function, returns a `Flow` instance
- `Flow` (uppercase class) is exported for type hints only — users always construct via `flow(...)`, never via `Flow(...)`
- No separate `pipe()` / `Builder` concept — `flow()` is both structure and runnable
- **`flow()` accepts only transformers** — passing an async generator at any position is an error; the producer is supplied separately via `run()`
- Sink is **implicit** — the last stage in `flow()` plays the sink role when run autonomously (output dropped). The same flow used as a transformer in another flow forwards its last stage's output into the parent's downstream queue.
- Stage names auto-derived from function names; collisions get numeric suffix; override via `stage(fn, name=...)`

### Drive modes
A `Flow` is symmetric — only how you activate it differs. Three modes, all on the same `Flow` instance:

| Mode | API | Item source | Termination |
|---|---|---|---|
| **Bounded autonomous** | `await chain.run(source)` | Caller passes async generator (or CM factory yielding one) | Source exhausts → drain → exit |
| **Unbounded autonomous** | `await chain.run()` | Framework auto-emits `None` signals indefinitely | External `chain.stop()` or first stage raises |
| **Push** | `async with chain.push() as handle: await handle.send(item)` | Caller pushes via the handle | `handle.complete()` (explicit or via `async with` exit) |

`Flow` exposes **activation modes** (`run`, `push`) but does not expose `send`/`complete` directly. Push mode returns a separate `PushHandle` type via `chain.push()`; `send()` and `complete()` live on `PushHandle`.

This is type-level separation — there is no way to call `send()` on a `Flow`. No runtime mode-locking is needed.

`stop()` is always available on `Flow` for graceful shutdown regardless of mode.

### Routing
- `router(classifier, **arms, default=...)` — branching as a regular transformer
- If classifier returns unknown label and no `default`, raises `ValueError`
- Arms can be any Transformer (callable, chain, Flow)

### Composability
- A `Flow` plugs into another `flow()` as a stage
- Sub-flows are first-class — framework discriminates them for diagnostics

### Transformer (tagged union)
A Transformer is one of three concrete kinds:
1. `Flow` — a sub-pipeline (introspectable for `dump()`, etc.)
2. `AsyncContextManager[Callable]` — for resources that need acquire/release
3. `Callable[[item], Awaitable[item]]` — plain async function

Plus `router(...)` results, which are also first-class for introspection.

### Async-only
- All transformers and context managers must be async (`async def`, `AsyncContextManager`)
- Sync code is rejected at construction with a clear error pointing to `asyncio.to_thread` or the `sync_stage()` helper
- Rationale: library is asyncio-native and orchestrates external work; sync blocks the event loop and defeats the purpose

### Resource scope
- Async context managers are **per-worker** — each worker enters its own context on startup, exits on shutdown
- Context lifecycle = worker lifecycle. Resources are acquired lazily as workers spawn, released as workers exit.
- Scale-to-zero is supported. When the last worker exits, the resource is released. First item after a `0→1` transition pays the acquire cost.
- Shared resources (pools, models) are managed outside the framework

### AsyncContextManager Transformer shape
A context-managed transformer is a **factory** — a no-arg callable that returns a fresh `AsyncContextManager` whose `__aenter__` yields the actual transformer callable. The framework calls the factory once per worker.

```python
TransformerFn  = Callable[[Any], Awaitable[Any]]
TransformerCMF = Callable[[], AsyncContextManager[TransformerFn]]
```

Accepted forms:
- `@asynccontextmanager`-decorated function (most common)
- A class whose constructor takes no args and which implements `__aenter__`/`__aexit__` — instantiated as `cls()` per worker
- Any callable matching the factory shape (e.g., `lambda: MyT(args)`)

Per-worker state without resource lifecycle: yield a closure capturing local state from the CM body. No special case needed.

**Producer as CM** is also supported — single producer means single CM entered once. Same factory shape, but the inner yielded value is an async generator (or async fetcher).

### Composition (sub-flows)
Composing a `Flow` into another `flow()` is **graph-level inlining**, not a function call. The sub-flow's stages are stitched into the parent's pipeline graph; each retains its own queue, worker pool, scaling strategy, and configuration. No correlation, no per-item return value — items flow queue-to-queue.

Naming under composition:
- Sub-flow stages are namespaced with the sub-flow's stage name in the parent: `inner.decode`, `inner.validate`
- Parent can override: `outer.configure("inner.decode", scaling=...)`
- Sub-flow's pre-existing config stays in effect unless overridden
- Recursion works: a sub-flow can contain sub-flows; names compose dotted (`outer.inner.deeper`)

The same `Flow` definition works **standalone** (`await inner.run(producer)`) or **composed** (`flow(parent_stage, inner, sink)`) — its behavior is identical in both cases. There is no "transformer mode" vs "standalone mode."

`Flow` is therefore **not a Transformer** in the call-shape sense. It is a graph fragment that the framework expands during construction. The Transformer call-shape protocol applies only to plain async functions, CM factories, and `Router`.

### Stage role detection
At construction, `flow()` validates each stage. Async generators are rejected — they belong to `run()` as the producer, not in the chain.

```python
if inspect.isasyncgenfunction(stage):
    raise TypeError("flow() does not accept async generators; pass producers to run() instead")
elif isinstance(stage, Flow):           shape = subflow   # graph-inlined, not called
elif isinstance(stage, Router):         shape = router
elif callable(stage):
    n = len(inspect.signature(stage).parameters)
    if n == 0:   shape = ctx_factory   # 0 args → returns AsyncContextManager
    elif n == 1: shape = transformer   # 1 arg → takes item
    else:        raise TypeError(...)
```

Sync functions and sync context managers are rejected with a clear error pointing to `asyncio.to_thread` or `sync_stage()`.

The **last stage** in `flow()` plays the sink role when run autonomously — its output is dropped. When the same flow is used as a transformer in another flow, the last stage's output is forwarded to the parent's downstream queue. No marking required.

### Scaling
- **Producers** always have exactly one worker (cannot scale). Async generators are not safe to consume concurrently — duplicates or races.
- For parallel ingestion (Kafka consumer, paginated API), use a trigger producer + multi-worker transformer pattern:
  ```python
  async def trigger():
      while True: yield None
  async def fetch(_): return await kafka.poll()
  main = flow(trigger, fetch, ...)
  main.configure("fetch", scaling=UtilizationScaling(min_workers=4))
  ```
- **Transformers and sinks** can scale, including `min_workers=0` (scale-to-zero)
- Default scaling: `FixedScaling(workers=1)` if no configuration is provided
- `FixedScaling(workers=N)` requires `N >= 1` (it's fixed, not elastic — use `UtilizationScaling` for scale-to-zero)
- `UtilizationScaling(min_workers=M)` allows `M >= 0`
- Validation at construction; raise `ValueError` on invalid combinations

### Configuration (separate from definition)
- `flow.configure(name, scaling=..., queue=...)` — per-stage tuning
- `flow.configure_default(scaling=..., queue=...)` — pipeline-wide defaults
- `flow.set_error_handler(handler)` — one per pipeline, last resort

### Architecture rules
- Stream processing pipeline, not a workflow engine
- DAG only — no cycles, all paths terminate at sink
- Orchestrator, not a worker — coordinates external heavy work, not CPU-bound Python computation
- Retry/iteration belongs inside a stage, not in graph topology

### Error handling
- Two layers: handle inside transformer (preferred), pipeline error handler (last resort)
- Built-in exceptions only — no custom hierarchy
- Error handler receives **typed events**, not raw `(item, exception)` tuples
- Handler behavior decides policy:
  - Returns normally → pipeline continues
  - Raises → pipeline aborts, exception propagates out of `run()`
- Default behavior when no handler is set:
  - `TransformerError` → log to stderr, continue
  - `SourceError` → re-raise (fatal)
  - `Dropped` → silent continue

#### Event types (initial set)
```python
@dataclass
class TransformerError:
    item: Any
    exception: Exception
    stage: str

@dataclass
class SourceError:
    exception: Exception

@dataclass
class Dropped:
    item: Any
    stage: str
    reason: DropReason   # enum
```

`DropReason` enum:
- `UPSTREAM_TERMINATED` — `Last(value)` upstream caused this item to be discarded
- `ROUTER_MISS` — router classifier returned an unknown arm and there was no `default`

### Termination
- **`Last(value)`** — wrapper a transformer can return to mean "this is the absolute last item." `value` flows downstream as the final item; everything still upstream of this transformer is dropped (each dropped item generates a `Dropped` event).
- **`chain.run(source)` returns naturally** when source generator completes — graceful drain.
- **`chain.drain()`** — graceful from outside (only meaningful in unbounded `run()` mode where there's no source the user controls).
- **`chain.stop()`** — immediate abort; resources released, items in flight dropped.
- **Source generator raises** — abort by default (re-raise from `run()`); user can wrap source for retry.

### Lifecycle
- Public API: `run(source)`, `run()`, `push()`, `drain()`, `stop()` only
- `start()` / `join()` are internal — used by `run()` and `push()` but not exposed. There is no legitimate user scenario for them; every way of feeding items into a flow is covered by the public methods. Hiding them keeps the surface minimal and prevents misuse (leaked workers, undefined-state mode mixing).

### Queue type
- Pipeline-wide default + per-stage override
- Built-in: `fifo_queue`, `lifo_queue`, `priority_queue`

---

## Open questions

- **`configure()` validation** — warn if user configures a stage name that doesn't exist in the flow?
- **Backpressure** — what's the bounded queue size default? How does it interact with auto-scaling?
- **Multi-source producers** — single worker per producer is decided. If a user needs multi-source ingestion, do they merge upstream (router-style), run multiple flows, or do we add parallel producers?
- **`dump()` output format** — JSON, mermaid, plain text, or all three?
- **Per-stage error handling** — currently pipeline-only. Useful to override per stage, or keep simple?

## Future enhancements (deferred)

- **`source(fn)` for multi-worker producers** — explicit primitive for "fetcher" style producers (Kafka consumer, SQS poller, paginated API). Each worker calls `fn()` independently to fetch one item; multiple workers parallelize ingestion. Termination via raised `StopAsyncIteration` or sentinel return. Defer until trigger+transformer pattern proves insufficient.
- **`merge(*sources)` helper** — combine multiple async generators into one for use as a single producer.

---

## Implementation backlog

### Core migration (current code uses `Builder` DSL, README documents `flow()`)
- [ ] Replace `_builder.py` with `flow()` function
- [ ] Implement role detection (async generator → producer; last stage → sink)
- [ ] Implement auto-naming + collision suffixing
- [ ] Implement `stage(fn, name=...)` wrapper
- [ ] Implement `router(classifier, **arms, default=...)`
- [ ] Make `Flow` introspectable as a transformer
- [ ] Update `__init__.py` exports: `flow`, `router`, `stage`, `Flow`, scaling classes, queue factories

### Flow runtime
- [ ] Implement `Flow.start() / join() / stop() / run()`
- [ ] Wire scaling strategies to actual worker spawn/kill
- [ ] Implement per-worker async context manager lifecycle
- [ ] Implement `Flow.configure() / configure_default()`
- [ ] Implement `Flow.set_error_handler()`

### Diagnostics
- [ ] Implement `Flow.dump()` — structure mode (graph as text/mermaid)
- [ ] Implement `Flow.dump()` — stats mode (live runtime stats)

### Tests
- [ ] `tests/test_builder.py` is currently skipped at module level (Builder DSL is being replaced) — rewrite against `flow()` API once it exists
- [ ] Add tests for sub-flow composition
- [ ] Add tests for router (incl. default and unknown-label-raises)
- [ ] Add tests for per-worker context manager scope
- [ ] Add tests for `run(producer)`, `run()` (auto-trigger), `async with` + `send()` push mode
- [ ] Add tests for "async generator passed to flow() raises TypeError"

### Polish
- [ ] Implement `sync_stage(fn)` helper — wraps a sync function with `asyncio.to_thread` so users can opt into running sync code as a transformer
- [ ] Fix mutable-default bug in `_builder.py` (`_Branch.options`, `_Branch.arm`) — moot if `_builder.py` is replaced
- [ ] Decide on Python target (currently `>=3.12` per `pyproject.toml`)

---

## Done

- Migrated build system from Poetry to uv/hatchling
- Created CLAUDE.md with project conventions
- Drafted README with new `flow()` DSL design
