from __future__ import annotations

import asyncio
import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class JobRecord(BaseModel):
    """Persistent representation of a job's state (API-visible)."""

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
    """Generic schema for submitting a job (extra keys passed through to params)."""

    proxy: Optional[str] = Field(None, description="Proxy URL to use for network requests")
    user_agent: Optional[str] = Field(None, description="Optional User-Agent header")
    headless: Optional[bool] = Field(None, description="Override the container headless setting for this job")

    class Config:
        extra = "allow"


class JobStore:
    """In-memory job registry + async queue."""

    def __init__(self) -> None:
        self.jobs: Dict[str, JobRecord] = {}
        self.queue: asyncio.Queue[str] = asyncio.Queue()

    def add(self, job: JobRecord) -> None:
        self.jobs[job.job_id] = job

    def get(self, job_id: str) -> Optional[JobRecord]:
        return self.jobs.get(job_id)
