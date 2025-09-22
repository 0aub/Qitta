"""Reliable job queue system with Redis backing and proper error handling.

This module implements Phase 1.1 of the reliability improvement plan:
- Redis-backed job persistence
- Job timeout and cancellation mechanisms
- Proper job lifecycle management
- Error recovery and retry logic
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import uuid
from typing import Any, Dict, Optional, List
from enum import Enum

import redis.asyncio as redis
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Job status enumeration with clear lifecycle states."""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ReliableJobRecord(BaseModel):
    """Enhanced job record with reliability features."""

    job_id: str
    task_name: str
    params: Dict[str, Any]
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    started_at: Optional[datetime.datetime] = None
    finished_at: Optional[datetime.datetime] = None
    last_heartbeat: Optional[datetime.datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300  # 5 minutes default
    worker_id: Optional[str] = None
    priority: int = 0  # Higher numbers = higher priority

    @property
    def status_with_elapsed(self) -> str:
        """Return status with elapsed time for running jobs."""
        if self.status != JobStatus.RUNNING or not self.started_at:
            return self.status.value

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

    @property
    def is_expired(self) -> bool:
        """Check if the job has exceeded its timeout."""
        if self.status != JobStatus.RUNNING or not self.started_at:
            return False

        elapsed = datetime.datetime.utcnow() - self.started_at
        return elapsed.total_seconds() > self.timeout_seconds

    @property
    def should_retry(self) -> bool:
        """Check if the job should be retried."""
        return self.retry_count < self.max_retries and self.status in [JobStatus.FAILED, JobStatus.TIMEOUT]


class ReliableJobStore:
    """Redis-backed job store with persistence and recovery capabilities."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0", logger: Optional[logging.Logger] = None):
        self.redis_url = redis_url
        self.logger = logger or logging.getLogger(__name__)
        self.redis_client: Optional[redis.Redis] = None

        # Redis key patterns
        self.job_key_pattern = "job:{job_id}"
        self.queue_key = "job_queue"
        self.priority_queue_key = "priority_job_queue"
        self.running_jobs_key = "running_jobs"
        self.worker_heartbeat_key = "worker_heartbeat:{worker_id}"

    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            self.logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None

    async def add_job(self, job: ReliableJobRecord) -> None:
        """Add a new job to the store and queue."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        # Store job data
        job_key = self.job_key_pattern.format(job_id=job.job_id)
        job_data = job.json()
        await self.redis_client.set(job_key, job_data)

        # Add to appropriate queue based on priority
        if job.priority > 0:
            await self.redis_client.zadd(self.priority_queue_key, {job.job_id: job.priority})
        else:
            await self.redis_client.lpush(self.queue_key, job.job_id)

        self.logger.info(f"Added job {job.job_id} to queue with priority {job.priority}")

    async def get_job(self, job_id: str) -> Optional[ReliableJobRecord]:
        """Retrieve a job by ID."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job_key = self.job_key_pattern.format(job_id=job_id)
        job_data = await self.redis_client.get(job_key)

        if job_data:
            return ReliableJobRecord.parse_raw(job_data)
        return None

    async def update_job(self, job: ReliableJobRecord) -> None:
        """Update job data in Redis."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job_key = self.job_key_pattern.format(job_id=job.job_id)
        job_data = job.json()
        await self.redis_client.set(job_key, job_data)

    async def get_next_job(self) -> Optional[str]:
        """Get the next job ID from the queue (priority first, then FIFO)."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        # Check priority queue first
        priority_jobs = await self.redis_client.zrevrange(self.priority_queue_key, 0, 0)
        if priority_jobs:
            job_id = priority_jobs[0]
            await self.redis_client.zrem(self.priority_queue_key, job_id)
            return job_id

        # Then check regular queue
        job_id = await self.redis_client.rpop(self.queue_key)
        return job_id

    async def mark_job_running(self, job_id: str, worker_id: str) -> None:
        """Mark a job as running and track the worker."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job = await self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.status = JobStatus.RUNNING
        job.started_at = datetime.datetime.utcnow()
        job.last_heartbeat = datetime.datetime.utcnow()
        job.worker_id = worker_id

        await self.update_job(job)
        await self.redis_client.sadd(self.running_jobs_key, job_id)

    async def mark_job_completed(self, job_id: str, result: Dict[str, Any]) -> None:
        """Mark a job as completed with results."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job = await self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.status = JobStatus.COMPLETED
        job.finished_at = datetime.datetime.utcnow()
        job.result = result

        await self.update_job(job)
        await self.redis_client.srem(self.running_jobs_key, job_id)

    async def mark_job_failed(self, job_id: str, error: str, should_retry: bool = True) -> None:
        """Mark a job as failed and optionally retry."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job = await self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.error = error
        job.finished_at = datetime.datetime.utcnow()
        job.retry_count += 1

        if should_retry and job.should_retry:
            job.status = JobStatus.QUEUED
            job.started_at = None
            job.worker_id = None

            # Re-queue the job with exponential backoff delay
            delay_seconds = min(60 * (2 ** job.retry_count), 600)  # Max 10 minutes
            await asyncio.sleep(delay_seconds)

            if job.priority > 0:
                await self.redis_client.zadd(self.priority_queue_key, {job.job_id: job.priority})
            else:
                await self.redis_client.lpush(self.queue_key, job.job_id)

            self.logger.info(f"Retrying job {job_id} (attempt {job.retry_count}/{job.max_retries})")
        else:
            job.status = JobStatus.FAILED
            self.logger.error(f"Job {job_id} failed permanently: {error}")

        await self.update_job(job)
        await self.redis_client.srem(self.running_jobs_key, job_id)

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job if it's not already completed."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job = await self.get_job(job_id)
        if not job:
            return False

        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            return False

        job.status = JobStatus.CANCELLED
        job.finished_at = datetime.datetime.utcnow()

        await self.update_job(job)
        await self.redis_client.srem(self.running_jobs_key, job_id)

        # Remove from queues if not started
        if job.status == JobStatus.QUEUED:
            await self.redis_client.lrem(self.queue_key, 0, job_id)
            await self.redis_client.zrem(self.priority_queue_key, job_id)

        self.logger.info(f"Cancelled job {job_id}")
        return True

    async def update_heartbeat(self, job_id: str, worker_id: str) -> None:
        """Update job heartbeat to indicate worker is alive."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        job = await self.get_job(job_id)
        if job and job.status == JobStatus.RUNNING:
            job.last_heartbeat = datetime.datetime.utcnow()
            await self.update_job(job)

            # Update worker heartbeat
            worker_key = self.worker_heartbeat_key.format(worker_id=worker_id)
            await self.redis_client.setex(worker_key, 60, datetime.datetime.utcnow().isoformat())

    async def cleanup_expired_jobs(self) -> List[str]:
        """Find and cleanup expired/stuck jobs."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        running_job_ids = await self.redis_client.smembers(self.running_jobs_key)
        expired_jobs = []

        for job_id in running_job_ids:
            job = await self.get_job(job_id)
            if not job:
                # Job data missing but still in running set
                await self.redis_client.srem(self.running_jobs_key, job_id)
                continue

            if job.is_expired:
                job.status = JobStatus.TIMEOUT
                job.finished_at = datetime.datetime.utcnow()
                job.error = f"Job timed out after {job.timeout_seconds} seconds"

                await self.update_job(job)
                await self.redis_client.srem(self.running_jobs_key, job_id)
                expired_jobs.append(job_id)

                self.logger.warning(f"Job {job_id} timed out and was cleaned up")

        return expired_jobs

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics for monitoring."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        regular_queue_size = await self.redis_client.llen(self.queue_key)
        priority_queue_size = await self.redis_client.zcard(self.priority_queue_key)
        running_jobs_count = await self.redis_client.scard(self.running_jobs_key)

        return {
            "regular_queue_size": regular_queue_size,
            "priority_queue_size": priority_queue_size,
            "running_jobs_count": running_jobs_count,
            "total_queued": regular_queue_size + priority_queue_size,
        }

    async def list_running_jobs(self) -> List[ReliableJobRecord]:
        """Get list of currently running jobs."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        running_job_ids = await self.redis_client.smembers(self.running_jobs_key)
        jobs = []

        for job_id in running_job_ids:
            job = await self.get_job(job_id)
            if job:
                jobs.append(job)

        return jobs


class JobManager:
    """High-level job management interface."""

    def __init__(self, job_store: ReliableJobStore, logger: Optional[logging.Logger] = None):
        self.job_store = job_store
        self.logger = logger or logging.getLogger(__name__)
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the job manager and cleanup task."""
        await self.job_store.connect()

        # Start periodic cleanup task
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.logger.info("Job manager started with periodic cleanup")

    async def stop(self) -> None:
        """Stop the job manager and cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        await self.job_store.disconnect()
        self.logger.info("Job manager stopped")

    async def submit_job(
        self,
        task_name: str,
        params: Dict[str, Any],
        timeout_seconds: int = 300,
        priority: int = 0,
        max_retries: int = 3
    ) -> str:
        """Submit a new job and return job ID."""
        job_id = uuid.uuid4().hex

        job = ReliableJobRecord(
            job_id=job_id,
            task_name=task_name,
            params=params,
            timeout_seconds=timeout_seconds,
            priority=priority,
            max_retries=max_retries
        )

        await self.job_store.add_job(job)
        self.logger.info(f"Submitted job {job_id} for task {task_name}")
        return job_id

    async def get_job_status(self, job_id: str) -> Optional[ReliableJobRecord]:
        """Get current job status."""
        return await self.job_store.get_job(job_id)

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job."""
        return await self.job_store.cancel_job(job_id)

    async def get_stats(self) -> Dict[str, Any]:
        """Get system statistics."""
        queue_stats = await self.job_store.get_queue_stats()
        running_jobs = await self.job_store.list_running_jobs()

        return {
            **queue_stats,
            "running_jobs": [
                {
                    "job_id": job.job_id,
                    "task_name": job.task_name,
                    "worker_id": job.worker_id,
                    "started_at": job.started_at.isoformat() if job.started_at else None,
                    "elapsed_seconds": (
                        (datetime.datetime.utcnow() - job.started_at).total_seconds()
                        if job.started_at else 0
                    ),
                    "status": job.status_with_elapsed
                }
                for job in running_jobs
            ]
        }

    async def _periodic_cleanup(self) -> None:
        """Periodic cleanup of expired jobs."""
        while True:
            try:
                await asyncio.sleep(30)  # Run every 30 seconds
                expired_jobs = await self.job_store.cleanup_expired_jobs()

                if expired_jobs:
                    self.logger.info(f"Cleaned up {len(expired_jobs)} expired jobs")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")
                await asyncio.sleep(30)