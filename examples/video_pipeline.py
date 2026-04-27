"""Realistic example: video processing pipeline.

Demonstrates how to wire flowrhythm for a stream-processing job —
fetch metadata for each URL, conditionally transcode with ffmpeg,
upload to object storage, then record to a database.

All external calls (HTTP, ffmpeg, S3, DB) are mocked with `asyncio.sleep`
so this runs without yt-dlp / ffmpeg / boto3 / a database. The pipeline
shape, scaling configuration, and resource handling match what you'd
actually ship.

    Topology:

        URLs → fetch_metadata → router(needs_conversion?) ─┬─ download → transcode ─┐
                                                           └─ download ──────────────┴─► upload → record_db

    Per-stage characteristics drive scaling:

      fetch_metadata   I/O bound (HTTP)         scale wide      (2..8)
      download         I/O bound (network)      scale wider     (4..12)
      transcode        CPU bound (ffmpeg)       cap narrow      (workers=2)
      upload           I/O bound (S3)           scale wide      (2..8)
      record_db        light, ordering matters  single worker

Run:    python examples/video_pipeline.py
"""

import asyncio
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass

from flowrhythm import (
    Dropped,
    FixedScaling,
    SourceError,
    TransformerError,
    UtilizationScaling,
    flow,
    router,
    stage,
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
# Stage 1: fetch metadata (mocked yt-dlp / HTTP API).
# CM factory pattern: each worker opens an HTTP session ONCE and reuses
# it for every item it processes.
# ---------------------------------------------------------------------------


@asynccontextmanager
async def with_http_session():
    session = {"connected": True}  # would be: aiohttp.ClientSession()

    async def fetch(url: str) -> Video:
        await asyncio.sleep(0.05)  # simulate API call
        # 1-in-20 simulated "video unavailable" failure
        if random.random() < 0.05:
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
# Stage 2: classifier — route based on whether transcoding is needed.
# ---------------------------------------------------------------------------


async def classify_for_conversion(video: Video) -> str:
    return "convert" if video.needs_conversion else "passthrough"


# ---------------------------------------------------------------------------
# Stage 3: download — used by both arms. I/O bound, scales wide.
# ---------------------------------------------------------------------------


async def download(video: Video) -> Video:
    await asyncio.sleep(video.duration_s * 0.0005)  # ~0.5ms per video-sec
    video.local_path = f"/tmp/{video.url.rsplit('/', 1)[-1]}.bin"
    return video


# ---------------------------------------------------------------------------
# Stage 4 (convert arm only): transcode — CPU-bound via ffmpeg subprocess.
# Capped to 2 workers because more would just oversubscribe the CPU.
# ---------------------------------------------------------------------------


async def transcode(video: Video) -> Video:
    # Real code: await asyncio.create_subprocess_exec("ffmpeg", ...)
    await asyncio.sleep(0.1)  # ffmpeg is the slow stage
    video.local_path = video.local_path.replace(".bin", ".mp4")
    video.codec = "h264"
    return video


# ---------------------------------------------------------------------------
# Stage 5: upload — per-worker S3 client via CM factory.
# ---------------------------------------------------------------------------


@asynccontextmanager
async def with_s3_client():
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
# Stage 6: record_db — light, single-worker for insert ordering.
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
        stage(with_http_session, name="fetch_metadata"),
        stage(
            router(
                classify_for_conversion,
                convert=flow(download, transcode),
                passthrough=download,
            ),
            name="dispatch",
        ),
        stage(with_s3_client, name="upload"),
        record_db,
        on_error=on_error,
    )

    # Per-stage tuning — each strategy matched to the stage's nature.
    chain.configure(
        "fetch_metadata", scaling=UtilizationScaling(min_workers=2, max_workers=8)
    )
    chain.configure(
        "dispatch.convert.download",
        scaling=UtilizationScaling(min_workers=4, max_workers=12),
    )
    chain.configure(
        "dispatch.convert.transcode", scaling=FixedScaling(workers=2)
    )
    chain.configure(
        "dispatch.passthrough",
        scaling=UtilizationScaling(min_workers=4, max_workers=12),
    )
    chain.configure(
        "upload", scaling=UtilizationScaling(min_workers=2, max_workers=8)
    )
    chain.configure("record_db", scaling=FixedScaling(workers=1))

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
    random.seed(42)  # deterministic failures across runs
    asyncio.run(main())
