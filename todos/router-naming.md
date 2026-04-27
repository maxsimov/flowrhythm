# Router naming improvements

**Status:** implemented
**Updated:** 2026-04-27

## Motivation

Right now a `router(...)` without `stage(name=...)` wrapping gets the auto-derived name `_router_N` (where N is the per-flow counter). That's ugly in `dump()` output and in per-stage `configure(...)` calls — users see things like `_router_0.convert.download` instead of `dispatch.convert.download`.

Plain transformer functions get nice auto-derived names from `fn.__name__`. Routers don't, even though their classifier function HAS a name (e.g. `classify_for_conversion`). The asymmetry is jarring — a user writing this:

```python
chain = flow(
    fetch_metadata,
    router(classify_for_conversion, fast=fast, slow=slow),
    upload,
)
```

…ends up with `_router_0` in their `dump()` output instead of something derived from `classify_for_conversion`. The information is right there in the API call; we just don't use it.

## Items

- [x] **Auto-derive router name from classifier `__name__`.** Implemented in `flow()`'s router-expansion branch. Symmetric with how plain functions are auto-named.
- [x] **Optional `name` kwarg on `router()`.** Reserved alongside `default`; popped from `**arms` in the factory; rejected if not a non-empty string.
- [x] **Precedence implemented:** `stage(router(..), name="X")` > `router(.., name="Y")` > `classifier.__name__` (skipped for `<lambda>`) > `_router_N` fallback.
- [x] **Lambda classifier falls back to `_router_N`** (when `__name__` is `"<lambda>"`).
- [x] **Arm-prefix uniqueness done at expansion time, NOT post-suffix.** Catches the "two routers same classifier" case correctly: `classify`, `classify.a`, `classify_2`, `classify_2.b` (instead of the broken `classify.b`). The collision check runs against all base names previously added to `raw_items` before arms are appended.
- [x] **Updated example** — `examples/video_pipeline.py` now uses `router(classify_for_conversion, name="dispatch", ...)` showing both the auto-derived name path AND the explicit `name=` override path. Output shows `dispatch`, `dispatch.convert.download`, etc. — no `_router_0`.
- [x] **Tests** in `tests/test_router_naming.py` (10 tests): auto-derive; lambda fallback; explicit `name=` kwarg; `name=` + `default=` together; full precedence chain; classifier-name beats fallback; two routers same classifier → suffixed prefix; two unnamed-lambda routers → indexed; reject empty `name=""`; routing functionality preserved with auto-derived name.
- [x] **DESIGN.md** Routing section updated with the new naming-precedence rules and a "Router naming" subsection. Reserved kwargs (`default`, `name`) listed.
- [x] **Existing tests updated**: `test_router_fallback_name_uses_router_index` and `test_two_unwrapped_routers_get_distinct_indices` adjusted since they were pinned to the old `_router_N` behaviour for non-lambda classifiers.

## Open questions (resolved)

- `name="default"` as an arm label: not allowed (both `default` and `name` are reserved kwargs). Deemed acceptable — arm labels are usually domain words, not config words. Documented in DESIGN.
- Lambdas: fall back to `_router_N` since `<lambda>` would be misleading as a stage name.

## Done

- 211 tests pass (was 187, +24 — 10 from `test_router_naming.py` + retained existing). Lint clean. Example output now shows clean stage names like `dispatch.convert.download`.
