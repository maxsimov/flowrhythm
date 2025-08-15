# flowrhythm

**Asynchronous Job Processing Framework with Dynamic Worker Scaling**

`flowrhythm` is a modular, asyncio-based framework for building efficient data pipelines. It provides automatic worker scaling based on job utilization, supports custom job capacities, and includes mechanisms for routing failed tasks to error handlers.

---

## Features

- 🔁 **Pipeline Composition** — Chain multiple async jobs with flexible configuration
- 🧠 **Utilization-Based Scaling** — Add or remove workers based on actual workload
- ⚙️ **Custom Job Capacity** — Fine-tune queue size, worker counts, and scaling thresholds
- 🚨 **Error Handling Support** — Automatically reroute failed jobs to an error handler
- 📊 **Worker Metrics** — Tracks idle, active, and blocked workers
- ✅ **Graceful Termination** — Built-in signaling for orderly pipeline shutdown

---

## Installation

```bash
pip install flowrhythm
```

> Not yet published. Run `pip install .` locally after building if you're testing it yourself.

---

## Usage Example

```python
import asyncio
from flowrhythm import Flow, job_name, LastWorkItem

@job_name("Stage 1")
async def first_stage(item):
    print("Received:", item)
    return item + 1

@job_name("Stage 2")
async def second_stage(item):
    print("Processed:", item)
    if item >= 5:
        return LastWorkItem()
    return item + 1

async def main():
    flow = Flow()
    flow.add(first_stage)
    flow.add(second_stage)
    await flow.start()
    await flow._jobs[0]._input.put(0)  # Start the pipeline
    await flow.run()

asyncio.run(main())
```

---

## Concepts

### Flow

Manages the chain of jobs. You can add regular jobs and a dedicated error job.

### Job

A processing step that consumes input and produces output. Each job has its own queue and worker pool.

### Capacity

Controls worker scaling and queue limits:
- `initial_workers`, `min_workers`, `max_workers`
- `queue_length`
- `lower_threshold`, `upper_threshold`
- `cooldown`, `sampling`, `dampening`

### LastWorkItem

Signals the end of processing. Triggers shutdown across all jobs.

### Error Routing

Exceptions like `RouteToErrorQueue` and `StopProcessing` help redirect or skip processing safely.

---

## Decorators

- `@job_name(name)` — Assigns a readable name
- `@job_capacity(capacity)` — Binds a custom `Capacity` instance
- `@workers(min, max, initial)` — Sets worker range and startup count

---

## Strategy

Currently supports:
- `Strategy.UTILIZATION` (default): scales workers based on real-time job utilization

---

## License

MIT License. See `LICENSE` for details.

---

## Author

Andrey Maximov  
[GitHub](https://github.com/yourusername)