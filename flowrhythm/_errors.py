"""Typed events delivered to the pipeline-level error handler.

A `Flow.set_error_handler(handler)` (or the `flow(*, on_error=)` constructor
kwarg) registers a single async callable that receives one of these events
when something goes wrong. The handler decides policy by what it does with
the event:

- **Returns normally** → pipeline continues
- **Raises** → pipeline aborts; whatever the handler raised propagates out
  of `run()`

See README "Error Handling" and DESIGN.md for the contract.
"""

import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any


@dataclass
class TransformerError:
    """A transformer raised on an item. The item is dropped (not delivered
    downstream); the worker continues with the next item unless the error
    handler raises to abort the run.
    """

    item: Any
    exception: Exception
    stage: str


@dataclass
class SourceError:
    """The source generator raised. The framework treats the source as
    exhausted (drain begins) unless the error handler raises to abort.
    """

    exception: Exception


class DropReason(Enum):
    """Why an item was dropped without an exception."""

    UPSTREAM_TERMINATED = "upstream_terminated"
    """An upstream `Last(value)` (M5) cut the stream; in-flight upstream
    items are discarded."""

    ROUTER_MISS = "router_miss"
    """A router (M8) classifier returned a label with no matching arm and
    no `default` was set."""


@dataclass
class Dropped:
    """An item was dropped intentionally (no exception). The handler can
    log/account for it; raising aborts the run.
    """

    item: Any
    stage: str
    reason: DropReason


# Type alias for the handler signature
ErrorEvent = TransformerError | SourceError | Dropped


async def default_handler(event: ErrorEvent) -> None:
    """Default error handler used when the user hasn't set one.

    The handler is an observer: it logs visible errors to stderr and
    silently accepts drops. The pipeline always continues — handler-raise
    does NOT abort. To stop a run, call `Flow.stop()` externally.
    See DESIGN.md "Error handler is observer-only".

    - `TransformerError` → log to stderr, continue
    - `SourceError` → log to stderr, continue (source is exhausted; pipeline drains)
    - `Dropped` → silent

    Users override this with `set_error_handler` or the `on_error=` kwarg.
    """
    match event:
        case TransformerError(item=item, exception=exc, stage=stage):
            print(
                f"[flowrhythm] {stage} failed on {item!r}: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
        case SourceError(exception=exc):
            print(
                f"[flowrhythm] source failed: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
        case Dropped():
            pass
