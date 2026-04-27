# Router naming improvements

**Status:** planned
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

- [ ] **Auto-derive router name from classifier `__name__`.** When `router(classifier, ...)` is given without an enclosing `stage(name=...)`, use `classifier.__name__` as the auto-name (subject to the existing collision-suffix rule). Keeps the symmetry with plain functions.
- [ ] **Optional `name` kwarg on `router()`.** Like `default`, a reserved kwarg name. Lets users explicitly name without wrapping in `stage()`:
    ```python
    router(classify, name="dispatch", fast=fast, slow=slow)
    ```
  Reserves `name` as an arm label (already true for `default`); document the limitation.
- [ ] **Decide precedence.** If the user wraps a router in `stage(router(...), name="X")` AND passes `name="Y"` to the router AND the classifier is named `Z`: most-specific wins (`stage(name=)` > `router(name=)` > classifier `__name__` > fallback `_router_N`).
- [ ] **Update example** — switch `examples/video_pipeline.py` to rely on the auto-derived classifier name (likely renaming `classify_for_conversion` to something like `classify_video` for nicer output).
- [ ] **Tests.** Cover: auto-name from classifier; explicit `name=` kwarg; both-set precedence; collision suffix when two routers have the same classifier name; lambda classifier falls back to `_router_N`.
- [ ] **DESIGN.md** — update the Routing section to document naming rules.

## Open questions

- Should `name="default"` be allowed? It's both an arm-label keyword AND would now be a router-name keyword. Disallowing avoids ambiguity but is a small papercut.
- Lambda classifiers have `__name__ == "<lambda>"` — ugly. Fallback to `_router_N` for that case probably best.

## Why now

Surfaced when writing `examples/video_pipeline.py`. Without this, the example either uses `stage(name=)` (which we want to de-emphasize as an "always optional" feature) or shows `_router_0` in its dump output (which is genuinely ugly for a "real-world" demo). Picking up this feature would let the example just work with sensible defaults.
