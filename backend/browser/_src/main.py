"""Playwright automation micro‑service.

This module implements a FastAPI application that manages a simple
asynchronous job queue for running browser automation tasks.  Clients
submit jobs via ``POST /jobs/{task_name}`` and poll their status via
``GET /jobs/{job_id}``.  Jobs are executed by a pool of workers that
reuse a single Playwright browser instance per worker.

"""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import pathlib
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from playwright.async_api import async_playwright, Browser

from .tasks import task_registry

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

# Read environment variables with sensible defaults
SERVICE_PORT: int = int(os.getenv("SERVICE_PORT", "8000"))
LOG_ROOT: str = os.getenv("LOG_ROOT", "/storage/logs")
DATA_ROOT: str = os.getenv("OUTPUT_ROOT", "/storage/scraped_data")
MAX_CONCURRENT_JOBS: int = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
API_KEY: str = os.getenv("BROWSER_API_KEY", "")
HEADLESS: bool = os.getenv("BROWSER_HEADLESS", "true").lower() != "false"

# ----------------------------------------------------------------------------
# Logging setup
# ----------------------------------------------------------------------------

def init_service_logger() -> logging.Logger:
    """Initialise the base logger for the browser service.

    Logs are written to ``LOG_ROOT/base/browser`` with daily rotation
    and echoed to the console.  This function must be called once
    during module import or application startup.
    """
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


# Initialise the base logger on import
service_logger = init_service_logger()


# ----------------------------------------------------------------------------
# Metrics
# ----------------------------------------------------------------------------

# Counters for API requests
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
# Data structures
# ----------------------------------------------------------------------------

def normalise_task(name: str) -> str:
    """Convert underscores and ASCII hyphens to the canonical registry key."""
    variants = [name, name.replace("_", "-"), name.replace("-", "_")]
    for v in variants:
        if v in task_registry:
            return v
    return name


class JobRecord(BaseModel):
    """Persistent representation of a job's state.

    The API serialises this model to return job status to clients.
    """

    job_id: str
    task_name: str
    params: Dict[str, Any]
    status: str = Field("queued", description="queued | running | finished | error")
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    started_at: Optional[datetime.datetime] = None
    finished_at: Optional[datetime.datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def status_with_elapsed(self) -> str:
        """Return status with elapsed time for running jobs."""
        if self.status != "running" or not self.started_at:
            return self.status
        
        elapsed = datetime.datetime.utcnow() - self.started_at
        elapsed_seconds = int(elapsed.total_seconds())
        
        if elapsed_seconds < 60:
            return f"running {elapsed_seconds}s"
        elif elapsed_seconds < 3600:
            minutes = elapsed_seconds // 60
            seconds = elapsed_seconds % 60
            return f"running {minutes}m {seconds}s"
        else:
            hours = elapsed_seconds // 3600
            minutes = (elapsed_seconds % 3600) // 60
            return f"running {hours}h {minutes}m"


class SubmitRequest(BaseModel):
    """Generic schema for submitting a job.

    The body of a POST request should mirror this model.  Additional keys
    beyond those declared here are passed directly through to the task's
    ``params``.
    """
    proxy: Optional[str] = Field(None, description="Proxy URL to use for network requests")
    user_agent: Optional[str] = Field(None, description="Optional User‑Agent header")
    headless: Optional[bool] = Field(None, description="Override the container headless setting for this job")
    # Additional parameters are accepted via **extra**

    class Config:
        extra = "allow"


# Global state
jobs: Dict[str, JobRecord] = {}
job_queue: asyncio.Queue[str] | None = None
worker_tasks: list[asyncio.Task] = []
playwright_browser: Browser | None = None


# ----------------------------------------------------------------------------
# FastAPI application
# ----------------------------------------------------------------------------

app = FastAPI(title="Browser Automation Service")


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
    """Enqueue a new job for the given task.

    The path segment ``task_name`` must match a registered task.  The
    request body is parsed into a SubmitRequest; any extra keys are
    forwarded into the job's parameter dictionary.  If an API key is
    configured then the caller must supply it in the ``X‑API‑Key`` header.
    """
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
    # Collect parameters from SubmitRequest, including extras
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
    # Record per‑job headless override
    if body.headless is not None:
        job.params["_headless_override"] = body.headless
    # Save and enqueue
    jobs[job_id] = job
    await job_queue.put(job_id)
    service_logger.info(f"Enqueued job {job_id} for task '{task_name}'")
    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Retrieve the status of a previously submitted job."""
    record = jobs.get(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Return a dictionary that includes the computed status_with_elapsed field
    response_data = record.dict()
    response_data["status_with_elapsed"] = record.status_with_elapsed
    return response_data


# ----------------------------------------------------------------------------
# Worker implementation
# ----------------------------------------------------------------------------

async def worker_loop(worker_index: int) -> None:
    """Asynchronous worker that processes jobs from the queue."""
    global playwright_browser
    logger = logging.getLogger(f"browser.worker-{worker_index}")
    # Inherit handlers from service logger
    if not logger.handlers:
        for h in service_logger.handlers:
            logger.addHandler(h)
        logger.setLevel(logging.INFO)
    # Ensure a browser instance exists for this worker
    while not playwright_browser:
        await asyncio.sleep(0.1)
    browser = playwright_browser
    while True:
        job_id = await job_queue.get()
        job = jobs[job_id]
        job.started_at = datetime.datetime.utcnow()
        job.status = "running"
        # Determine output directory for this job
        job_output_dir = os.path.join(DATA_ROOT, job.task_name, job_id)
        os.makedirs(job_output_dir, exist_ok=True)
        # Set up a per‑job logger that writes to its own file in the output dir
        job_log_path = os.path.join(job_output_dir, "job.log")
        job_logger = logging.getLogger(f"browser.job.{job_id}")
        # Add handler only once
        if not job_logger.handlers:
            fh = logging.FileHandler(job_log_path)
            fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
            job_logger.addHandler(fh)
            # Propagate to service logger as well
            for h in service_logger.handlers:
                job_logger.addHandler(h)
            job_logger.setLevel(logging.INFO)
        try:
            # Resolve headless override for this job.  If an override is
            # provided, launch a separate browser via the existing browser's
            # browser_type.  Otherwise reuse the shared browser.
            headless_override = job.params.pop("_headless_override", None)
            if headless_override is not None:
                # ``browser.browser_type`` returns the BrowserType used to
                # create the original Browser instance.  Launching a new
                # browser here respects the same engine (Chromium).
                job_browser = await browser.browser_type.launch(headless=headless_override)
            else:
                job_browser = browser
            # Execute the task
            task_fn = task_registry[job.task_name]
            result = await task_fn(
                browser=job_browser,
                params=job.params,
                job_output_dir=job_output_dir,
                logger=job_logger,
            )
            job.result = result
            job.status = "finished"
        except Exception as exc:
            job.error = str(exc)
            job.status = "error"
            job_logger.error(f"Job {job_id} failed: {exc}")
        finally:
            job.finished_at = datetime.datetime.utcnow()
            job_queue.task_done()
            # If we launched a private browser for the job, close it
            if 'job_browser' in locals() and job_browser is not browser:
                try:
                    await job_browser.close()
                except Exception:
                    pass


# ----------------------------------------------------------------------------
# Startup and shutdown events
# ----------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    """Initialise the Playwright runtime, browser and worker pool."""
    global job_queue, worker_tasks, playwright_browser

    service_logger.info("Starting browser automation service…")
    job_queue = asyncio.Queue()

    playwright = await async_playwright().start()


    launch_args = ["--disable-blink-features=AutomationControlled"]
    playwright_browser = await playwright.chromium.launch(
        headless=HEADLESS,
        args=launch_args,
    )
    service_logger.info(f"Chromium launched (headless={HEADLESS})")
    # Start worker tasks
    for i in range(MAX_CONCURRENT_JOBS):
        task = asyncio.create_task(worker_loop(i))
        worker_tasks.append(task)
    service_logger.info(f"Spawned {MAX_CONCURRENT_JOBS} worker(s)")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Gracefully stop workers and close the browser."""
    service_logger.info("Shutting down browser automation service…")
    # Cancel workers
    for task in worker_tasks:
        task.cancel()
    # Wait briefly for cancellation
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    # Close browser
    if playwright_browser:
        try:
            await playwright_browser.close()
        except Exception:
            pass
