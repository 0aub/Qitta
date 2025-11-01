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
from fastapi.responses import JSONResponse, HTMLResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from typing import List, Optional

from .config.production import get_config, DeploymentEnvironment
from .runtime import BrowserRuntime
from .exploration import PageExplorer

# Initialize production configuration
config = get_config()

# Import reliable infrastructure
from .jobs import JobStore, JobManager, JobRecord, SubmitRequest
from .workers import WorkerPool
from .tasks import task_registry, normalise_task
from .reliability import MetricsCollector, AlertManager, HealthMonitor, AlertSeverity

# ----------------------------------------------------------------------------
# Logging (kept in main as requested)
# ----------------------------------------------------------------------------

def init_service_logger() -> logging.Logger:
    """Initialize enhanced logger for the reliable browser service."""
    today = datetime.date.today().isoformat()
    root = pathlib.Path(config.system.log_root)
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
# Pydantic models for exploration API
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

# ----------------------------------------------------------------------------
# App + global runtime (reliable infrastructure)
# ----------------------------------------------------------------------------

app = FastAPI(title="Reliable Browser Automation Service", version="2.0.0")

# Global components
job_manager: JobManager | None = None
browser_runtime: BrowserRuntime | None = None
worker_pool: WorkerPool | None = None
page_explorer: PageExplorer | None = None
metrics_collector: MetricsCollector | None = None
alert_manager: AlertManager | None = None
health_monitor: HealthMonitor | None = None


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
        "components": str(components)
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
    """Get comprehensive system statistics with enhanced monitoring."""
    if not job_manager or not worker_pool:
        raise HTTPException(status_code=503, detail="Service not fully initialized")

    try:
        job_stats = await job_manager.get_stats()
        worker_stats = worker_pool.get_stats()

        # Calculate enhanced metrics
        uptime_seconds = (
            datetime.datetime.utcnow() - startup_time
        ).total_seconds() if 'startup_time' in globals() else 0

        # Health score calculation
        health_score = 100.0
        if health_monitor:
            health_score = health_monitor.calculate_health_score(
                worker_stats.get('workers', []),
                job_stats,
                {}  # Error stats would come from error handler
            )

        return {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "jobs": job_stats,
            "workers": worker_stats,
            "service": {
                "version": "2.0.0",
                "mode": "reliable",
                "uptime_seconds": uptime_seconds,
                "health_score": health_score
            },
            "alerts": alert_manager.get_alert_summary() if alert_manager else {},
            "monitoring": {
                "enhanced_error_handling": True,
                "circuit_breakers_enabled": True,
                "health_monitoring": True
            }
        }
    except Exception as e:
        service_logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving stats")


@app.post("/jobs/{task_name}")
async def submit_job(task_name: str, body: SubmitRequest, request: Request):
    """Submit a job with enhanced reliability features."""
    global job_manager

    # API key enforcement
    if config.security.api_key_required:
        api_key_header = request.headers.get("x-api-key")
        if not api_key_header or api_key_header != config.security.api_key:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    # Validate the task
    task_name = normalise_task(task_name)
    if task_name not in task_registry:
        raise HTTPException(status_code=404, detail=f"Unknown task '{task_name}'")

    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    # Build parameters
    params: Dict[str, Any] = body.dict()

    # Remove reliability-specific fields from params
    # Intelligent timeout based on scrape_level and features
    scrape_level = params.get("scrape_level", 1)
    has_social_graph = params.get("scrape_followers") or params.get("scrape_following")

    # Default timeouts by level (with human behavior delays considered)
    if not params.get("timeout_seconds"):
        if scrape_level >= 4:
            default_timeout = 900  # 15 minutes for Level 4 (comprehensive)
        elif scrape_level >= 3:
            if has_social_graph:
                default_timeout = 720  # 12 minutes for Level 3 with social graph
            else:
                default_timeout = 600  # 10 minutes for Level 3
        elif scrape_level >= 2:
            if has_social_graph:
                default_timeout = 600  # 10 minutes for Level 2 with social graph
            else:
                default_timeout = 420  # 7 minutes for Level 2
        else:
            default_timeout = 300  # 5 minutes for Level 1
    else:
        default_timeout = 300

    timeout_seconds = params.pop("timeout_seconds", default_timeout)
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
# Page Exploration API Endpoints
# ----------------------------------------------------------------------------

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
# Session Management Endpoints
# ----------------------------------------------------------------------------

@app.post("/capture/twitter")
async def capture_twitter_session():
    """
    Capture Twitter session by opening browser for user login.
    
    Returns session information and saves cookies to shared storage.
    """
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
# Session Management Endpoints (Session-Only Mode)
# ----------------------------------------------------------------------------

class SessionUpdateRequest(BaseModel):
    """Request to update a session with fresh cookies."""
    session_name: str
    cookies: Dict[str, str]

class SessionValidateRequest(BaseModel):
    """Request to validate a session."""
    session_file: str

@app.post("/sessions/validate")
async def validate_session(body: SessionValidateRequest):
    """Validate if a session is still valid and get expiry info."""
    try:
        import json
        from datetime import datetime
        import os

        session_path = f"/sessions/{body.session_file}"

        if not os.path.exists(session_path):
            return {
                "valid": False,
                "error": "Session file not found",
                "session_file": body.session_file
            }

        # Load session
        with open(session_path, 'r') as f:
            session_data = json.load(f)

        cookies = session_data.get("cookies", {})
        has_auth_token = bool(cookies.get("auth_token"))
        has_ct0 = bool(cookies.get("ct0"))
        has_twid = bool(cookies.get("twid"))

        # Check expiration
        expires_estimate = session_data.get("expires_estimate", 0)
        current_time = datetime.now().timestamp()
        is_expired = current_time > expires_estimate
        time_remaining = max(0, expires_estimate - current_time)

        # CloudFlare token check
        has_cf_token = bool(cookies.get("__cf_bm"))

        valid = has_auth_token and has_ct0 and has_twid and not is_expired

        return {
            "valid": valid,
            "session_file": body.session_file,
            "has_auth_token": has_auth_token,
            "has_ct0": has_ct0,
            "has_twid": has_twid,
            "has_cloudflare_token": has_cf_token,
            "is_expired": is_expired,
            "time_remaining_seconds": int(time_remaining),
            "time_remaining_hours": round(time_remaining / 3600, 2),
            "captured_at": session_data.get("captured_at"),
            "expires_estimate": expires_estimate
        }

    except Exception as e:
        service_logger.error(f"Session validation failed: {e}")
        return {
            "valid": False,
            "error": str(e),
            "session_file": body.session_file
        }

@app.post("/sessions/update")
async def update_session(body: SessionUpdateRequest):
    """Update a session file with fresh cookies."""
    try:
        import json
        from datetime import datetime

        session_path = f"/sessions/{body.session_name}"

        # Calculate expiry (Twitter sessions typically last 30 days)
        current_time = datetime.now()
        expires_estimate = (current_time.timestamp() + (30 * 24 * 3600))

        session_data = {
            "cookies": body.cookies,
            "captured_at": current_time.isoformat(),
            "expires_estimate": expires_estimate,
            "updated_via_api": True
        }

        # Write session file
        with open(session_path, 'w') as f:
            json.dump(session_data, f, indent=2)

        service_logger.info(f"✅ Session updated: {body.session_name}")

        return {
            "success": True,
            "session_name": body.session_name,
            "session_path": session_path,
            "captured_at": session_data["captured_at"],
            "expires_estimate": expires_estimate,
            "cookies_updated": list(body.cookies.keys())
        }

    except Exception as e:
        service_logger.error(f"Session update failed: {e}")
        raise HTTPException(status_code=500, detail=f"Could not update session: {str(e)}")

@app.get("/sessions/status")
async def get_sessions_status():
    """Get status of all session files."""
    try:
        import json
        from datetime import datetime
        import os
        import glob

        session_files = glob.glob("/sessions/*.json")
        sessions_status = []

        for session_path in session_files:
            try:
                with open(session_path, 'r') as f:
                    session_data = json.load(f)

                cookies = session_data.get("cookies", {})
                expires_estimate = session_data.get("expires_estimate", 0)
                current_time = datetime.now().timestamp()
                is_expired = current_time > expires_estimate
                time_remaining = max(0, expires_estimate - current_time)

                valid = (
                    bool(cookies.get("auth_token")) and
                    bool(cookies.get("ct0")) and
                    bool(cookies.get("twid")) and
                    not is_expired
                )

                sessions_status.append({
                    "filename": os.path.basename(session_path),
                    "valid": valid,
                    "is_expired": is_expired,
                    "time_remaining_hours": round(time_remaining / 3600, 2),
                    "captured_at": session_data.get("captured_at"),
                    "has_cloudflare_token": bool(cookies.get("__cf_bm"))
                })
            except Exception as e:
                sessions_status.append({
                    "filename": os.path.basename(session_path),
                    "valid": False,
                    "error": str(e)
                })

        valid_count = sum(1 for s in sessions_status if s.get("valid"))

        return {
            "total_sessions": len(sessions_status),
            "valid_sessions": valid_count,
            "invalid_sessions": len(sessions_status) - valid_count,
            "sessions": sessions_status
        }

    except Exception as e:
        service_logger.error(f"Could not get sessions status: {e}")
        raise HTTPException(status_code=500, detail=f"Could not get sessions status: {str(e)}")

@app.get("/sessions/harvester", response_class=HTMLResponse)
async def get_session_harvester():
    """Serve the session harvester HTML page."""
    try:
        import os
        html_path = os.path.join(os.path.dirname(__file__), "session_harvester_bookmarklet.html")
        with open(html_path, 'r') as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except Exception as e:
        service_logger.error(f"Could not load session harvester: {e}")
        raise HTTPException(status_code=500, detail=f"Could not load session harvester: {str(e)}")


# ----------------------------------------------------------------------------
# Enhanced Monitoring Endpoints (Phase 2.2)
# ----------------------------------------------------------------------------

@app.get("/monitoring/alerts")
async def get_alerts():
    """Get active alerts and alert summary."""
    if not alert_manager:
        raise HTTPException(status_code=503, detail="Alert manager not initialized")

    return {
        "active_alerts": [alert.__dict__ for alert in alert_manager.get_active_alerts()],
        "summary": alert_manager.get_alert_summary()
    }

@app.get("/monitoring/health")
async def get_health_details():
    """Get detailed health information."""
    if not health_monitor:
        raise HTTPException(status_code=503, detail="Health monitor not initialized")

    try:
        health_checks = await health_monitor.run_health_checks()

        return {
            "overall_score": health_monitor.last_health_score,
            "health_checks": health_checks,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        service_logger.error(f"Error getting health details: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving health details")

@app.get("/monitoring/performance")
async def get_performance_metrics():
    """Get performance trend analysis."""
    if not metrics_collector:
        raise HTTPException(status_code=503, detail="Metrics collector not initialized")

    # This would integrate with performance monitor
    return {
        "message": "Performance monitoring active",
        "features": [
            "Real-time throughput tracking",
            "Response time monitoring",
            "Resource usage analysis",
            "Trend detection"
        ]
    }

@app.post("/monitoring/test-alert")
async def test_alert():
    """Test alert system (for development)."""
    if not alert_manager:
        raise HTTPException(status_code=503, detail="Alert manager not initialized")

    # Trigger a test alert
    test_metrics = {"test_metric": 999}
    alert_manager.check_alerts(test_metrics)

    return {"message": "Test alert triggered if rules are configured"}

@app.get("/monitoring/resources")
async def get_resource_metrics():
    """Get detailed resource utilization metrics."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        # Get worker pool stats which now include resource optimization
        worker_stats = worker_pool.get_stats()

        return {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "resource_optimization": worker_stats.get("resource_optimization", {}),
            "service_throttling": worker_stats.get("service_throttling", {}),
            "scaling_status": {
                "enabled": worker_stats.get("scaling_enabled", False),
                "active": worker_stats.get("optimization_active", False),
                "current_workers": worker_stats.get("worker_count", 0),
                "max_workers": worker_stats.get("max_workers", 0)
            }
        }
    except Exception as e:
        service_logger.error(f"Error getting resource metrics: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving resource metrics")

@app.post("/monitoring/scale")
async def manual_scaling(action: str):
    """Manual scaling control (for testing/emergencies)."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    if action not in ["up", "down"]:
        raise HTTPException(status_code=400, detail="Action must be 'up' or 'down'")

    try:
        if action == "up":
            await worker_pool._scale_up()
            return {"message": "Scaling up worker pool"}
        else:
            await worker_pool._scale_down()
            return {"message": "Scaling down worker pool"}
    except Exception as e:
        service_logger.error(f"Error in manual scaling: {e}")
        raise HTTPException(status_code=500, detail=f"Scaling failed: {str(e)}")


# ----------------------------------------------------------------------------
# Phase 2.4: Dead Letter Queue Endpoints
# ----------------------------------------------------------------------------

@app.get("/dlq/jobs")
async def get_dead_letter_jobs(start: int = 0, count: int = 10):
    """Get jobs from the dead letter queue."""
    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    try:
        dlq_jobs = await job_manager.job_store.get_dead_letter_jobs(start, count)
        return {
            "dead_letter_jobs": dlq_jobs,
            "start": start,
            "count": len(dlq_jobs),
            "requested_count": count
        }
    except Exception as e:
        service_logger.error(f"Error retrieving dead letter jobs: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving dead letter jobs")

@app.post("/dlq/replay/{job_id}")
async def replay_dead_letter_job(job_id: str, reset_retries: bool = True):
    """Replay a job from the dead letter queue."""
    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    try:
        success = await job_manager.job_store.replay_dead_letter_job(job_id, reset_retries)
        if success:
            return {"message": f"Job {job_id} replayed successfully", "reset_retries": reset_retries}
        else:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found in dead letter queue")
    except Exception as e:
        service_logger.error(f"Error replaying dead letter job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error replaying job: {str(e)}")

@app.get("/dlq/stats")
async def get_dead_letter_stats():
    """Get dead letter queue statistics."""
    if not job_manager:
        raise HTTPException(status_code=503, detail="Job manager not initialized")

    try:
        stats = await job_manager.job_store.get_dlq_stats()
        return {"dead_letter_statistics": stats}
    except Exception as e:
        service_logger.error(f"Error retrieving dead letter stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving dead letter statistics")


# ----------------------------------------------------------------------------
# Phase 2.5: Circuit Breaker Endpoints
# ----------------------------------------------------------------------------

@app.get("/circuit-breakers")
async def get_circuit_breakers():
    """Get status of all circuit breakers."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        metrics = worker_pool.circuit_breaker_manager.get_all_metrics()
        summary = worker_pool.circuit_breaker_manager.get_summary()

        return {
            "circuit_breakers": metrics,
            "summary": summary
        }
    except Exception as e:
        service_logger.error(f"Error retrieving circuit breaker status: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving circuit breaker status")

@app.post("/circuit-breakers/{breaker_name}/reset")
async def reset_circuit_breaker(breaker_name: str):
    """Reset a specific circuit breaker."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        success = await worker_pool.circuit_breaker_manager.reset_breaker(breaker_name)
        if success:
            return {"message": f"Circuit breaker '{breaker_name}' reset successfully"}
        else:
            raise HTTPException(status_code=404, detail=f"Circuit breaker '{breaker_name}' not found")
    except Exception as e:
        service_logger.error(f"Error resetting circuit breaker {breaker_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error resetting circuit breaker: {str(e)}")

@app.post("/circuit-breakers/reset-all")
async def reset_all_circuit_breakers():
    """Reset all circuit breakers."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        await worker_pool.circuit_breaker_manager.reset_all_breakers()
        return {"message": "All circuit breakers reset successfully"}
    except Exception as e:
        service_logger.error(f"Error resetting all circuit breakers: {e}")
        raise HTTPException(status_code=500, detail="Error resetting circuit breakers")


# ----------------------------------------------------------------------------
# Phase 2.6: Fallback and Degradation Endpoints
# ----------------------------------------------------------------------------
@app.get("/fallback/health")
async def get_fallback_health():
    """Get fallback manager health summary."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        health_summary = worker_pool.fallback_manager.get_health_summary()
        return {
            "fallback_health": health_summary,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        service_logger.error(f"Error retrieving fallback health: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving fallback health")


@app.get("/fallback/metrics")
async def get_fallback_metrics():
    """Get fallback execution metrics."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        metrics = worker_pool.fallback_manager.get_fallback_metrics()
        return {
            "fallback_metrics": metrics,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        service_logger.error(f"Error retrieving fallback metrics: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving fallback metrics")


@app.get("/fallback/service-level")
async def get_service_level():
    """Get current service level and degradation status."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        service_level = worker_pool.fallback_manager.service_level
        health_summary = worker_pool.fallback_manager.get_health_summary()

        return {
            "current_service_level": service_level.value,
            "service_availability": health_summary.get("overall_availability", 0),
            "healthy_services": health_summary.get("healthy_services", 0),
            "total_services": health_summary.get("total_services", 0),
            "degradation_events": health_summary.get("recent_degradation_events", []),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        service_logger.error(f"Error retrieving service level: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving service level")


@app.post("/fallback/cache/{service_name}")
async def update_service_cache(service_name: str, cache_data: dict):
    """Update cache for a specific service."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        worker_pool.fallback_manager.update_cache(service_name, cache_data)
        return {"message": f"Cache updated for service '{service_name}'"}
    except Exception as e:
        service_logger.error(f"Error updating cache for {service_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Error updating cache: {str(e)}")


# ----------------------------------------------------------------------------
# Phase 4.3: Stealth and Anti-Detection Endpoints
# ----------------------------------------------------------------------------

@app.get("/stealth/metrics")
async def get_stealth_metrics():
    """Get stealth operation metrics."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        stealth_metrics = worker_pool.stealth_manager.get_stealth_metrics()
        return {
            "stealth_metrics": stealth_metrics,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        service_logger.error(f"Error retrieving stealth metrics: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving stealth metrics")


@app.get("/stealth/status")
async def get_stealth_status():
    """Get comprehensive stealth system status."""
    if not worker_pool:
        raise HTTPException(status_code=503, detail="Worker pool not initialized")

    try:
        stealth_metrics = worker_pool.stealth_manager.get_stealth_metrics()

        # Get worker stealth integration status
        worker_stealth_status = []
        for worker_id, worker in worker_pool._workers.items():
            worker_stealth_status.append({
                "worker_id": worker_id,
                "stealth_enabled": worker.stealth_manager is not None,
                "context_manager_stealth": worker.context_manager.stealth_manager is not None
            })

        return {
            "stealth_system": stealth_metrics,
            "worker_integration": worker_stealth_status,
            "total_workers": len(worker_pool._workers),
            "stealth_enabled_workers": len([w for w in worker_stealth_status if w["stealth_enabled"]]),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
    except Exception as e:
        service_logger.error(f"Error retrieving stealth status: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving stealth status")


# ----------------------------------------------------------------------------
# Production Health Checks and Readiness Probes (Phase 4.4b)
# ----------------------------------------------------------------------------

# Import production health checker
from .health import ProductionHealthChecker

# Global health checker instance
production_health_checker: Optional[ProductionHealthChecker] = None


@app.get("/health")
async def health_check():
    """Basic health check endpoint for load balancers and monitoring."""
    try:
        global production_health_checker
        if not production_health_checker:
            # Create health checker if not exists
            production_health_checker = ProductionHealthChecker(
                redis_client=getattr(job_manager.job_store, 'redis_client', None) if 'job_manager' in globals() else None,
                browser_runtime=browser_runtime if 'browser_runtime' in globals() else None,
                worker_pool=worker_pool if 'worker_pool' in globals() else None,
                logger=service_logger
            )

        # Perform liveness check
        liveness_result = await production_health_checker.perform_liveness_check()

        if liveness_result.status.value == "healthy":
            return JSONResponse(
                status_code=200,
                content={
                    "status": "healthy",
                    "message": liveness_result.message,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }
            )
        else:
            return JSONResponse(
                status_code=503,
                content={
                    "status": liveness_result.status.value,
                    "message": liveness_result.message,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }
            )

    except Exception as e:
        service_logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "message": f"Health check failed: {str(e)}",
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        )


@app.get("/ready")
async def readiness_check():
    """Comprehensive readiness check for Kubernetes and production deployment."""
    try:
        global production_health_checker
        if not production_health_checker:
            # Create health checker if not exists
            production_health_checker = ProductionHealthChecker(
                redis_client=getattr(job_manager.job_store, 'redis_client', None) if 'job_manager' in globals() else None,
                browser_runtime=browser_runtime if 'browser_runtime' in globals() else None,
                worker_pool=worker_pool if 'worker_pool' in globals() else None,
                circuit_breaker_manager=getattr(worker_pool, 'circuit_breaker_manager', None) if 'worker_pool' in globals() else None,
                fallback_manager=getattr(worker_pool, 'fallback_manager', None) if 'worker_pool' in globals() else None,
                logger=service_logger
            )

        # Perform comprehensive readiness check
        readiness_summary = await production_health_checker.perform_readiness_check()

        status_code = 200 if readiness_summary.ready_for_traffic else 503

        return JSONResponse(
            status_code=status_code,
            content=readiness_summary.to_dict()
        )

    except Exception as e:
        service_logger.error(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "overall_status": "unhealthy",
                "ready_for_traffic": False,
                "error": str(e),
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        )


@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check with component breakdown for debugging."""
    try:
        global production_health_checker
        if not production_health_checker:
            # Create health checker if not exists
            production_health_checker = ProductionHealthChecker(
                redis_client=getattr(job_manager.job_store, 'redis_client', None) if 'job_manager' in globals() else None,
                browser_runtime=browser_runtime if 'browser_runtime' in globals() else None,
                worker_pool=worker_pool if 'worker_pool' in globals() else None,
                circuit_breaker_manager=getattr(worker_pool, 'circuit_breaker_manager', None) if 'worker_pool' in globals() else None,
                fallback_manager=getattr(worker_pool, 'fallback_manager', None) if 'worker_pool' in globals() else None,
                logger=service_logger
            )

        # Perform comprehensive health check
        health_summary = await production_health_checker.perform_readiness_check()

        # Add additional debugging information
        debug_info = {
            "service_uptime_seconds": (datetime.datetime.utcnow() - startup_time).total_seconds() if 'startup_time' in globals() else 0,
            "active_jobs": len(getattr(worker_pool, 'active_jobs', {})) if 'worker_pool' in globals() else 0,
            "worker_count": len(getattr(worker_pool, 'workers', [])) if 'worker_pool' in globals() else 0,
            "redis_connected": hasattr(job_manager.job_store, 'redis_client') if 'job_manager' in globals() else False,
            "browser_connected": getattr(browser_runtime, 'browser', None) is not None if 'browser_runtime' in globals() else False
        }

        result = health_summary.to_dict()
        result["debug_info"] = debug_info

        return JSONResponse(
            status_code=200,
            content=result
        )

    except Exception as e:
        service_logger.error(f"Detailed health check failed: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "Detailed health check failed",
                "message": str(e),
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        )


# ----------------------------------------------------------------------------
# Startup and shutdown (kept in main)
# ----------------------------------------------------------------------------

startup_time: datetime.datetime

@app.on_event("startup")
async def on_startup() -> None:
    """Initialize the reliable browser automation service with enhanced monitoring."""
    global job_manager, browser_runtime, worker_pool, page_explorer, startup_time
    global metrics_collector, alert_manager, health_monitor

    startup_time = datetime.datetime.utcnow()
    service_logger.info("Starting reliable browser automation service (Phase 2 enhancements)...")

    try:
        # Enhanced monitoring infrastructure (Phase 2.2)
        metrics_collector = MetricsCollector()
        alert_manager = AlertManager(service_logger)
        service_logger.info("✅ Enhanced monitoring system initialized")

        # Configure alert rules
        alert_manager.add_alert_rule(
            name="high_queue_size",
            metric_name="queue_size",
            threshold=20,
            severity=AlertSeverity.MEDIUM,
            condition="gt"
        )
        alert_manager.add_alert_rule(
            name="low_health_score",
            metric_name="health_score",
            threshold=70,
            severity=AlertSeverity.HIGH,
            condition="lt"
        )

        # Redis configuration
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        # Job management
        job_store = JobStore(redis_url=redis_url, logger=service_logger)
        job_manager = JobManager(job_store, logger=service_logger)
        await job_manager.start()
        service_logger.info("✅ Reliable job manager started")

        # Browser runtime
        launch_args = ["--disable-blink-features=AutomationControlled"]
        browser_runtime = BrowserRuntime(headless=config.browser.headless, args=launch_args, logger=service_logger)
        await browser_runtime.start()
        service_logger.info("✅ Browser runtime started")

        # Page Explorer (for debugging)
        page_explorer = PageExplorer(browser_runtime.browser, service_logger)
        service_logger.info("✅ Page exploration API initialized")

        # Health monitoring with checks
        health_monitor = HealthMonitor(metrics_collector, alert_manager)

        # Add health checks
        health_monitor.add_health_check(
            "redis_connection",
            lambda: job_store.redis_client is not None
        )
        health_monitor.add_health_check(
            "browser_connection",
            lambda: browser_runtime.browser.is_connected()
        )

        # Reliable worker pool with automatic browser restart capability
        worker_pool = WorkerPool(
            job_store=job_store,
            browser=browser_runtime.browser,
            task_registry=task_registry,
            data_root=config.system.data_root,
            logger=service_logger,
            max_workers=config.scaling.max_workers,
            browser_runtime=browser_runtime  # Pass runtime for automatic browser restart
        )
        await worker_pool.start()
        service_logger.info(f"✅ Reliable worker pool started with {config.scaling.max_workers} workers")
        service_logger.info("🔄 Automatic browser restart enabled")

        service_logger.info("🚀 Enhanced reliable browser automation service ready!")
        service_logger.info("📊 Phase 2 features: Enhanced error handling, monitoring, alerting")

    except Exception as e:
        service_logger.error(f"Failed to start enhanced service: {e}")
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
