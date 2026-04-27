"""Realistic example: video processing pipeline.

Demonstrates how to wire flowrhythm for a stream-processing job —
fetch metadata for each URL, conditionally transcode with ffmpeg,
upload to object storage, then record to a database.

All external calls (HTTP, ffmpeg, S3, DB) are mocked with `asyncio.sleep`
so this runs without yt-dlp / ffmpeg / boto3 / a database.

    Topology:

        URLs → fetch_metadata → router(needs_conversion?) ─┬─ download → transcode ─┐
                                                           └─ download ──────────────┴─► upload → record_db

The example uses defaults everywhere (single worker per stage, fifo
queue, default error handling) — minimal ceremony. For production
you'd tune scaling per stage with `chain.configure(...)`; see the
"Configuring a flow" and "Scaling Strategies" sections in README.md.

Run:    python examples/video_pipeline.py
"""

import asyncio
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass

from flowrhythm import (
    Dropped,
    SourceError,
    TransformerError,
    flow,
    router,
)


# ---------------------------------------------------------------------------
# Domain
# ---------------------------------------------------------------------------


@dataclass
class Video:
    url: str
    title: str = ""
    duration_s: int = 0
    codec: str = ""
    needs_conversion: bool = False
    local_path: str = ""
    s3_key: str = ""

    def __repr__(self) -> str:
        return f"Video({self.url})"


# ---------------------------------------------------------------------------
# Source — feed URLs into the pipeline (would be a feed reader,
# Kafka consumer, paginated API, etc.)
# ---------------------------------------------------------------------------


async def url_source():
    for i in range(20):
        yield f"https://example.com/video/{i:02d}"
        await asyncio.sleep(0.005)  # simulate paginated API trickling


# ---------------------------------------------------------------------------
# fetch_metadata — CM factory: each worker opens an HTTP session ONCE
# and reuses it for every item it processes.
# ---------------------------------------------------------------------------


@asynccontextmanager
async def fetch_metadata():
    session = {"connected": True}  # would be: aiohttp.ClientSession()

    async def fetch(url: str) -> Video:
        await asyncio.sleep(0.05)  # simulate API call
        # Deterministic "unavailable" failure for /video/13 — exercises the
        # error handler so users see how per-item failures are routed
        # without killing the pipeline.
        if url.endswith("/13"):
            raise RuntimeError(f"video unavailable: {url}")
        codec = random.choice(["h264", "vp9", "av1"])
        return Video(
            url=url,
            title=f"Title for {url.rsplit('/', 1)[-1]}",
            duration_s=random.randint(30, 600),
            codec=codec,
            needs_conversion=(codec != "h264"),
        )

    try:
        yield fetch
    finally:
        session["connected"] = False  # would be: await session.close()


# ---------------------------------------------------------------------------
# Classifier — route based on whether transcoding is needed.
# ---------------------------------------------------------------------------


async def classify_for_conversion(video: Video) -> str:
    return "convert" if video.needs_conversion else "passthrough"


# ---------------------------------------------------------------------------
# download — used by both arms. I/O bound, scales wide.
# ---------------------------------------------------------------------------


async def download(video: Video) -> Video:
    await asyncio.sleep(video.duration_s * 0.0005)  # ~0.5ms per video-sec
    video.local_path = f"/tmp/{video.url.rsplit('/', 1)[-1]}.bin"
    return video


# ---------------------------------------------------------------------------
# transcode (convert arm only) — CPU-bound via ffmpeg subprocess.
# Capped to 2 workers because more would just oversubscribe the CPU.
# ---------------------------------------------------------------------------


async def transcode(video: Video) -> Video:
    # Real code: await asyncio.create_subprocess_exec("ffmpeg", ...)
    await asyncio.sleep(0.1)  # ffmpeg is the slow stage
    video.local_path = video.local_path.replace(".bin", ".mp4")
    video.codec = "h264"
    return video


# ---------------------------------------------------------------------------
# upload — CM factory for per-worker S3 client.
# ---------------------------------------------------------------------------


@asynccontextmanager
async def upload():
    client = {"region": "us-east-1"}  # would be: boto3.client("s3")

    async def do_upload(video: Video) -> Video:
        await asyncio.sleep(0.03)
        video.s3_key = f"videos/{video.url.rsplit('/', 1)[-1]}.mp4"
        return video

    try:
        yield do_upload
    finally:
        client["region"] = None  # would be: client.close()


# ---------------------------------------------------------------------------
# record_db — light, single-worker for insert ordering.
# ---------------------------------------------------------------------------


_db_recorded: list[Video] = []


async def record_db(video: Video) -> None:
    await asyncio.sleep(0.005)  # would be: await db.insert(...)
    _db_recorded.append(video)


# ---------------------------------------------------------------------------
# Error handler — log per-event-type. Pipeline keeps running.
# ---------------------------------------------------------------------------


_errors: list[str] = []


async def on_error(event):
    if isinstance(event, TransformerError):
        item_repr = (
            event.item.url if isinstance(event.item, Video) else event.item
        )
        _errors.append(
            f"{event.stage}: {type(event.exception).__name__}: "
            f"{event.exception} (item={item_repr})"
        )
    elif isinstance(event, SourceError):
        _errors.append(f"source: {event.exception}")
    elif isinstance(event, Dropped):
        _errors.append(f"dropped at {event.stage}: {event.reason.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main():
    chain = flow(
        fetch_metadata,
        router(
            classify_for_conversion,
            convert=flow(download, transcode),
            passthrough=download,
        ),
        upload,
        record_db,
        on_error=on_error,
    )

    # 1. Show the topology before running — useful as a sanity check
    print("=== TOPOLOGY ===")
    print(chain.dump(mode="structure"))
    print()

    # 2. Run the pipeline
    print("=== RUNNING ===")
    await chain.run(url_source)

    # 3. Inspect final stats
    print()
    print("=== STATS ===")
    print(chain.dump(mode="stats"))

    # 4. Summary
    print()
    print("=== SUMMARY ===")
    print(f"  videos delivered to DB: {len(_db_recorded)}")
    transcoded = sum(1 for v in _db_recorded if v.local_path.endswith(".mp4"))
    print(f"  transcoded:    {transcoded}")
    print(f"  passed through:{len(_db_recorded) - transcoded}")
    print(f"  errors logged: {len(_errors)}")
    for err in _errors:
        print(f"    - {err}")


if __name__ == "__main__":
    random.seed(42)  # deterministic codec/duration distribution
    asyncio.run(main())
