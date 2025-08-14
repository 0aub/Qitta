"""Playwright automation micro-service.

This FastAPI app exposes:
- POST /jobs/{task_name} to enqueue a browser task
- GET  /jobs/{job_id}   to poll status
- /metrics for Prometheus and /healthz for liveness

Jobs run on a worker pool that reuses a shared Chromium instance.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import uuid
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from .config import LOG_ROOT, DATA_ROOT, MAX_CONCURRENT_JOBS, API_KEY, HEADLESS
from .jobs import JobRecord, SubmitRequest, JobStore
from .runtime import BrowserRuntime
from .workers import WorkerPool
from .tasks import task_registry, normalise_task

# ----------------------------------------------------------------------------
# Logging (kept in main as requested)
# ----------------------------------------------------------------------------

def init_service_logger() -> logging.Logger:
    """Initialise the base logger for the browser service."""
    today = datetime.date.today().isoformat()
    root = pathlib.Path(LOG_ROOT)
    base_dir = root / "base" / "browser"
    base_dir.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = []
    handlers.append(logging.FileHandler(base_dir / f"{today}.log"))
    handlers.append(logging.StreamHandler())
    logger = logging.getLogger("browser")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt)
        for h in handlers:
            h.setFormatter(formatter)
            logger.addHandler(h)
    return logger


service_logger = init_service_logger()

# ----------------------------------------------------------------------------
# Metrics
# ----------------------------------------------------------------------------

REQUEST_COUNT = Counter(
    "browser_request_count",
    "Number of requests received",
    labelnames=["endpoint", "method", "status"],
)
REQUEST_LATENCY = Histogram(
    "browser_request_latency_seconds",
    "Request latency in seconds",
    labelnames=["endpoint"],
)

# ----------------------------------------------------------------------------
# App + global runtime (kept in main)
# ----------------------------------------------------------------------------

app = FastAPI(title="Browser Automation Service")

job_store: JobStore | None = None
browser_runtime: BrowserRuntime | None = None
worker_pool: WorkerPool | None = None


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    """Collect Prometheus metrics for each request."""
    endpoint = request.url.path
    method = request.method
    with REQUEST_LATENCY.labels(endpoint).time():
        response = await call_next(request)
    REQUEST_COUNT.labels(endpoint, method, response.status_code).inc()
    return response


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    """Liveness probe.  Returns 200 once startup has completed."""
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return JSONResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.post("/jobs/{task_name}")
async def submit_job(task_name: str, body: SubmitRequest, request: Request):
    """Enqueue a new job for the given task."""
    global job_store

    # API key enforcement
    if API_KEY:
        api_key_header = request.headers.get("x-api-key")
        if not api_key_header or api_key_header != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    # Validate the task
    task_name = normalise_task(task_name)
    if task_name not in task_registry:
        raise HTTPException(status_code=404, detail=f"Unknown task '{task_name}'")

    # Build job record
    job_id = uuid.uuid4().hex

    params: Dict[str, Any] = body.dict()
    # Remove known optional fields from the parameters dict
    params.pop("proxy", None)
    params.pop("user_agent", None)
    params.pop("headless", None)

    job = JobRecord(job_id=job_id, task_name=task_name, params=params)

    # Additional meta parameters are stored in the job record itself
    if body.proxy:
        job.params["proxy"] = body.proxy
    if body.user_agent:
        job.params["user_agent"] = body.user_agent
    # Per-job headless override
    if body.headless is not None:
        job.params["_headless_override"] = body.headless

    # Save and enqueue
    assert job_store is not None
    job_store.add(job)
    await job_store.queue.put(job_id)

    service_logger.info(f"Enqueued job {job_id} for task '{task_name}'")
    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Retrieve the status of a previously submitted job."""
    assert job_store is not None
    record = job_store.get(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")

    response_data = record.dict()
    response_data["status_with_elapsed"] = record.status_with_elapsed
    return response_data


# ----------------------------------------------------------------------------
# Startup and shutdown (kept in main)
# ----------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    """Initialise the Playwright runtime and worker pool."""
    global job_store, browser_runtime, worker_pool

    service_logger.info("Starting browser automation service…")

    # State containers
    job_store = JobStore()

    # Browser
    launch_args = ["--disable-blink-features=AutomationControlled"]
    browser_runtime = BrowserRuntime(headless=HEADLESS, args=launch_args, logger=service_logger)
    await browser_runtime.start()

    # Workers
    worker_pool = WorkerPool(
        store=job_store,
        shared_browser=browser_runtime.browser,  # type: ignore[arg-type]
        task_registry=task_registry,
        data_root=DATA_ROOT,
        base_logger=service_logger,
    )
    worker_pool.start(MAX_CONCURRENT_JOBS)
    service_logger.info(f"Spawned {MAX_CONCURRENT_JOBS} worker(s)")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Gracefully stop workers and close the browser."""
    global browser_runtime, worker_pool
    service_logger.info("Shutting down browser automation service…")

    if worker_pool:
        await worker_pool.stop()
    if browser_runtime:
        await browser_runtime.stop()
