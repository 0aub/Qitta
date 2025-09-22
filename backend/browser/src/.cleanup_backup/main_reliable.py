"""Reliable Playwright automation micro-service.

Phase 1 Reliability Improvements:
- Redis-backed job queue with persistence and recovery
- Browser context isolation with proper cleanup
- Job timeout and cancellation mechanisms
- Worker health monitoring and auto-restart
- Resource leak prevention

This FastAPI app exposes:
- POST /jobs/{task_name} to enqueue a browser task
- GET  /jobs/{job_id}   to poll status
- GET  /stats          to view system statistics
- /metrics for Prometheus and /healthz for liveness

Jobs run on a reliable worker pool with individual browser contexts.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import pathlib
import uuid
from typing import Any, Dict
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from typing import List, Optional

from .config import LOG_ROOT, DATA_ROOT, MAX_CONCURRENT_JOBS, API_KEY, HEADLESS
from .runtime import BrowserRuntime
from .exploration import PageExplorer

# Import reliable infrastructure
from .reliable_jobs import ReliableJobStore, JobManager, ReliableJobRecord
from .reliable_workers import ReliableWorkerPool
from .task_adapter import reliable_task_registry


# ----------------------------------------------------------------------------
# Enhanced Models
# ----------------------------------------------------------------------------

class EnhancedSubmitRequest(BaseModel):
    """Enhanced job submission with reliability features."""

    proxy: Optional[str] = None
    user_agent: Optional[str] = None
    headless: Optional[bool] = None
    timeout_seconds: int = 300  # 5 minutes default
    priority: int = 0  # Higher numbers = higher priority
    max_retries: int = 3

    class Config:
        extra = "allow"


# ----------------------------------------------------------------------------
# Logging (enhanced for reliability)
# ----------------------------------------------------------------------------

def init_service_logger() -> logging.Logger:
    """Initialize enhanced logger for the reliable browser service."""
    today = datetime.date.today().isoformat()
    root = pathlib.Path(LOG_ROOT)
    base_dir = root / "base" / "browser"
    base_dir.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = []
    handlers.append(logging.FileHandler(base_dir / f"{today}.log"))
    handlers.append(logging.StreamHandler())

    logger = logging.getLogger("browser.reliable")
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
# Metrics (enhanced)
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

JOB_COUNT = Counter(
    "browser_job_count",
    "Number of jobs processed",
    labelnames=["task_name", "status"],
)

JOB_DURATION = Histogram(
    "browser_job_duration_seconds",
    "Job processing duration in seconds",
    labelnames=["task_name"],
)

# ----------------------------------------------------------------------------
# App + global runtime (reliable infrastructure)
# ----------------------------------------------------------------------------

app = FastAPI(title="Reliable Browser Automation Service", version="2.0.0")

# Global components
job_manager: JobManager | None = None
browser_runtime: BrowserRuntime | None = None
worker_pool: ReliableWorkerPool | None = None
page_explorer: PageExplorer | None = None


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
    """Enhanced liveness probe with component health checks."""
    components = {}

    if job_manager and job_manager.job_store.redis_client:
        try:
            await job_manager.job_store.redis_client.ping()
            components["redis"] = "ok"
        except Exception:
            components["redis"] = "error"
    else:
        components["redis"] = "not_connected"

    if browser_runtime and browser_runtime.browser:
        components["browser"] = "ok"
    else:
        components["browser"] = "error"

    if worker_pool:
        components["workers"] = "ok"
    else:
        components["workers"] = "error"

    all_healthy = all(status == "ok" for status in components.values())

    return {
        "status": "ok" if all_healthy else "degraded",
        "components": components
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return JSONResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/stats")
async def get_stats():
    """Get comprehensive system statistics."""
    if not job_manager or not worker_pool:
        raise HTTPException(status_code=503, detail="Service not fully initialized")

    try:
        job_stats = await job_manager.get_stats()
        worker_stats = worker_pool.get_stats()

        return {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "jobs": job_stats,
            "workers": worker_stats,
            "service": {
                "version": "2.0.0",
                "mode": "reliable",
                "uptime_seconds": (
                    datetime.datetime.utcnow() - startup_time
                ).total_seconds() if 'startup_time' in globals() else 0
            }
        }
    except Exception as e:
        service_logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving stats")


@app.post("/jobs/{task_name}")
async def submit_job(task_name: str, body: EnhancedSubmitRequest, request: Request):
    """Submit a job with enhanced reliability features."""
    global job_manager

    # API key enforcement
    if API_KEY:
        api_key_header = request.headers.get("x-api-key")
        if not api_key_header or api_key_header != API_KEY:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    # Validate the task
    task_name = normalize_task_name(task_name)
    if task_name not in reliable_task_registry:
        raise HTTPException(status_code=404, detail=f"Unknown task '{task_name}'")

    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    # Build parameters
    params: Dict[str, Any] = body.dict()

    # Remove reliability-specific fields from params
    timeout_seconds = params.pop("timeout_seconds", 300)
    priority = params.pop("priority", 0)
    max_retries = params.pop("max_retries", 3)
    params.pop("proxy", None)
    params.pop("user_agent", None)
    params.pop("headless", None)

    # Add metadata
    if body.proxy:
        params["proxy"] = body.proxy
    if body.user_agent:
        params["user_agent"] = body.user_agent
    if body.headless is not None:
        params["_headless_override"] = body.headless

    try:
        # Submit job with reliability features
        job_id = await job_manager.submit_job(
            task_name=task_name,
            params=params,
            timeout_seconds=timeout_seconds,
            priority=priority,
            max_retries=max_retries
        )

        service_logger.info(
            f"Submitted reliable job {job_id} for task '{task_name}' "
            f"(timeout: {timeout_seconds}s, priority: {priority}, retries: {max_retries})"
        )

        return {"job_id": job_id}

    except Exception as e:
        service_logger.error(f"Error submitting job: {e}")
        raise HTTPException(status_code=500, detail="Error submitting job")


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Get enhanced job status with reliability information."""
    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    try:
        job = await job_manager.get_job_status(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        response_data = job.dict()
        response_data["status_with_elapsed"] = job.status_with_elapsed

        # Add reliability metadata
        response_data["reliability_info"] = {
            "retry_count": job.retry_count,
            "max_retries": job.max_retries,
            "timeout_seconds": job.timeout_seconds,
            "priority": job.priority,
            "worker_id": job.worker_id,
            "can_retry": job.should_retry,
            "is_expired": job.is_expired
        }

        return response_data

    except HTTPException:
        raise
    except Exception as e:
        service_logger.error(f"Error getting job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving job")


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a job."""
    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    try:
        success = await job_manager.cancel_job(job_id)
        if success:
            return {"message": f"Job {job_id} cancelled successfully"}
        else:
            raise HTTPException(status_code=404, detail="Job not found or cannot be cancelled")

    except HTTPException:
        raise
    except Exception as e:
        service_logger.error(f"Error cancelling job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Error cancelling job")


# ----------------------------------------------------------------------------
# Page Exploration API (maintained for compatibility)
# ----------------------------------------------------------------------------

class PageStructureRequest(BaseModel):
    url: str
    wait_timeout: int = 10000

class SelectorTestRequest(BaseModel):
    url: str
    selectors: List[str]
    extract_text: bool = True
    extract_attributes: bool = False
    wait_timeout: int = 10000

class DataExtractionRequest(BaseModel):
    url: str
    extraction_config: Dict[str, Any]
    wait_timeout: int = 10000

class BookingExploreRequest(BaseModel):
    location: str = "Dubai"
    check_in: str = "2025-12-01"
    check_out: str = "2025-12-03"


@app.post("/explore/page-structure")
async def explore_page_structure(request: PageStructureRequest):
    """Analyze the basic DOM structure and elements of a webpage."""
    global page_explorer

    if not page_explorer:
        raise HTTPException(status_code=503, detail="Page explorer not initialized")

    try:
        result = await page_explorer.analyze_page_structure(
            url=request.url,
            wait_timeout=request.wait_timeout
        )
        return result
    except Exception as e:
        service_logger.error(f"Page structure exploration failed: {e}")
        return {"status": "error", "error": str(e)}


@app.post("/explore/selectors")
async def explore_selectors(request: SelectorTestRequest):
    """Test multiple selectors against a webpage and return what they find."""
    global page_explorer

    if not page_explorer:
        raise HTTPException(status_code=503, detail="Page explorer not initialized")

    try:
        result = await page_explorer.test_selectors(
            url=request.url,
            selectors=request.selectors,
            extract_text=request.extract_text,
            extract_attributes=request.extract_attributes,
            wait_timeout=request.wait_timeout
        )
        return result
    except Exception as e:
        service_logger.error(f"Selector testing failed: {e}")
        return {"status": "error", "error": str(e)}


@app.post("/explore/data-extraction")
async def explore_data_extraction(request: DataExtractionRequest):
    """Debug data extraction with detailed logging and validation."""
    global page_explorer

    if not page_explorer:
        raise HTTPException(status_code=503, detail="Page explorer not initialized")

    try:
        result = await page_explorer.extract_data_debug(
            url=request.url,
            extraction_config=request.extraction_config,
            wait_timeout=request.wait_timeout
        )
        return result
    except Exception as e:
        service_logger.error(f"Data extraction debugging failed: {e}")
        return {"status": "error", "error": str(e)}


@app.post("/explore/booking-hotel")
async def explore_booking_hotel(request: BookingExploreRequest):
    """Booking.com specific exploration to understand current page structure."""
    global page_explorer

    if not page_explorer:
        raise HTTPException(status_code=503, detail="Page explorer not initialized")

    try:
        result = await page_explorer.explore_booking_hotel(
            location=request.location,
            check_in=request.check_in,
            check_out=request.check_out
        )
        return result
    except Exception as e:
        service_logger.error(f"Booking.com exploration failed: {e}")
        return {"status": "error", "error": str(e)}


# ----------------------------------------------------------------------------
# Session Management (maintained for compatibility)
# ----------------------------------------------------------------------------

@app.post("/capture/twitter")
async def capture_twitter_session():
    """Capture Twitter session by opening browser for user login."""
    global browser_runtime

    if not browser_runtime:
        raise HTTPException(status_code=503, detail="Browser runtime not initialized")

    try:
        from .session_capture import session_capture
        result = await session_capture.capture_session(browser_runtime.browser)
        return JSONResponse(content=result)
    except Exception as e:
        service_logger.error(f"Session capture failed: {e}")
        raise HTTPException(status_code=500, detail=f"Session capture failed: {str(e)}")

@app.get("/sessions")
async def list_sessions():
    """List available captured sessions."""
    try:
        from .session_capture import session_capture
        return session_capture.list_sessions()
    except Exception as e:
        service_logger.error(f"Could not list sessions: {e}")
        raise HTTPException(status_code=500, detail="Could not list sessions")

@app.delete("/sessions/{filename}")
async def delete_session(filename: str):
    """Delete a captured session."""
    try:
        from .session_capture import session_capture
        result = session_capture.delete_session(filename)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        service_logger.error(f"Could not delete session: {e}")
        raise HTTPException(status_code=500, detail=f"Could not delete session: {str(e)}")


# ----------------------------------------------------------------------------
# Utility Functions
# ----------------------------------------------------------------------------

def normalize_task_name(name: str) -> str:
    """Normalize task name with fallback variants."""
    variants = [name, name.replace("_", "-"), name.replace("-", "_")]
    for variant in variants:
        if variant in reliable_task_registry:
            return variant
    return name


# ----------------------------------------------------------------------------
# Reliable Startup and Shutdown
# ----------------------------------------------------------------------------

startup_time: datetime.datetime

@app.on_event("startup")
async def on_startup() -> None:
    """Initialize the reliable browser automation service."""
    global job_manager, browser_runtime, worker_pool, page_explorer, startup_time

    startup_time = datetime.datetime.utcnow()
    service_logger.info("Starting reliable browser automation service (Phase 1 improvements)...")

    try:
        # Redis configuration
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        # Job management
        job_store = ReliableJobStore(redis_url=redis_url, logger=service_logger)
        job_manager = JobManager(job_store, logger=service_logger)
        await job_manager.start()
        service_logger.info("✅ Reliable job manager started")

        # Browser runtime
        launch_args = ["--disable-blink-features=AutomationControlled"]
        browser_runtime = BrowserRuntime(headless=HEADLESS, args=launch_args, logger=service_logger)
        await browser_runtime.start()
        service_logger.info("✅ Browser runtime started")

        # Page Explorer (for debugging)
        page_explorer = PageExplorer(browser_runtime.browser, service_logger)
        service_logger.info("✅ Page exploration API initialized")

        # Reliable worker pool
        worker_pool = ReliableWorkerPool(
            job_store=job_store,
            browser=browser_runtime.browser,
            task_registry=reliable_task_registry,
            data_root=DATA_ROOT,
            logger=service_logger,
            max_workers=MAX_CONCURRENT_JOBS
        )
        await worker_pool.start()
        service_logger.info(f"✅ Reliable worker pool started with {MAX_CONCURRENT_JOBS} workers")

        service_logger.info("🚀 Reliable browser automation service ready!")

    except Exception as e:
        service_logger.error(f"Failed to start reliable service: {e}")
        raise


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Gracefully shutdown the reliable browser automation service."""
    global browser_runtime, worker_pool, job_manager

    service_logger.info("Shutting down reliable browser automation service...")

    # Stop components in reverse order
    if worker_pool:
        await worker_pool.stop()
        service_logger.info("✅ Worker pool stopped")

    if job_manager:
        await job_manager.stop()
        service_logger.info("✅ Job manager stopped")

    if browser_runtime:
        await browser_runtime.stop()
        service_logger.info("✅ Browser runtime stopped")

    service_logger.info("🔻 Reliable browser automation service shutdown complete")