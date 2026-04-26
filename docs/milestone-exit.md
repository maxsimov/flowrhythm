# Milestone exit checklist

Run this when wrapping up a milestone, a phase of work, or before opening a PR. The goal is a clean handoff: code passes gates, docs match reality, plans reflect what shipped.

Steps run roughly in order — earlier steps are cheap gates that block the rest.

## 1. Code & test gates

Run and ensure both pass before going further:

```
make test
make lint
```

If either fails, stop and fix. Don't continue with stale code.

Optionally check `make cov` if coverage matters for this milestone.

## 2. DESIGN ↔ code alignment

For DESIGN sections touched (or relevant to) this milestone, spot-check that the documented behavior matches the implementation.

- Cross-reference with the milestone's tests — they're the executable contract. If a test expects behavior that DESIGN contradicts, one of them is wrong; resolve before continuing.
- If implementation diverged from DESIGN intentionally, update DESIGN to match (and note the rationale).
- If DESIGN says X but implementation does Y by accident, fix the implementation.

## 3. DESIGN ↔ README alignment

Compare DESIGN and README on every topic the milestone touched. Common drift points:

- Default behaviors (what happens when the user does nothing)
- Public API signatures and kwargs
- Termination / lifecycle semantics
- Error handling and event types
- Configuration shapes

When mismatches are found: surface them with options + recommendation (see "Asking questions" in CLAUDE.md), don't silently pick.

## 4. Public API ↔ docs alignment

- `flowrhythm/__init__.py` exports match what README's "Public API" section promises
- Any symbol referenced in README examples but not yet exported is explicitly tagged `*(planned)*` (or the section is removed)
- Docstrings on exported symbols are present and not stale

## 5. Plan files & INDEX

For the milestone's plan file (`todos/<plan>.md`):

- Tick checked items; add a brief note next to each on **how/where** it was done — the file becomes a record, not just a checklist
- Flip status (`in-progress` → `implemented`) in both the plan file and `todos/INDEX.md`
- If priorities shifted during the work (something else became more urgent, or a deferred item became blocking), reorder `INDEX.md`
- If new follow-up work emerged, create new plan files (don't pile loose notes onto the closing plan)

## 6. Three-tier doc check (light)

For any non-obvious decision made during this milestone, verify the three-tier pattern (per CLAUDE.md "Documenting non-obvious internal design decisions"):

- DESIGN.md owns the **why** (rationale, alternatives, trade-offs)
- Class/function docstring owns **what + pointer to why** (`See DESIGN.md "<section>" for the rationale.`)
- Inline comment at the decision site owns **here's where the why applies**

Skip if the milestone made no such decisions (most bug fixes, simple features).

## 7. Open questions sweep

Scan DESIGN.md's "Open questions" section. Anything answered during this milestone? Move it from "Open questions" into the appropriate decided-design section, keeping the resolution and rationale.

## 8. Commit & offer push

- Conventional commit, milestone-tagged where applicable (e.g. `feat(M5): Last + drain + stop`)
- Logical chunks if the work spans multiple concerns — one commit per complete idea
- Confirm with user before pushing (per CLAUDE.md git rules)

## When to apply this checklist

- Wrapping up a milestone (M1, M2, …)
- Before opening a PR
- Before publishing a release (`uv publish`)
- After a long pause, picking work back up — useful to confirm state hasn't drifted

Skip steps that don't apply (e.g. step 6 for a docs-only change). Don't skip steps 1, 3, and 5 — those catch the most common drift.
