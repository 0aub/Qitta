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

# Optional Redis dependency with graceful fallback
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    # Create placeholder redis module when not available
    class RedisPlaceholder:
        @staticmethod
        def from_url(*args, **kwargs):
            raise ImportError("Redis module not available - install with: pip install redis")
    redis = RedisPlaceholder()
    REDIS_AVAILABLE = False

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Job status enumeration with clear lifecycle states."""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class JobRecord(BaseModel):
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


class SubmitRequest(BaseModel):
    """Enhanced job submission with reliability features."""

    proxy: Optional[str] = None
    user_agent: Optional[str] = None
    headless: Optional[bool] = None
    timeout_seconds: int = 300  # 5 minutes default
    priority: int = 0  # Higher numbers = higher priority
    max_retries: int = 3

    class Config:
        extra = "allow"


class JobStore:
    """Job store with Redis backing and graceful in-memory fallback."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0", logger: Optional[logging.Logger] = None):
        self.redis_url = redis_url
        self.logger = logger or logging.getLogger(__name__)
        self.redis_client: Optional[redis.Redis] = None
        self.use_redis = REDIS_AVAILABLE

        # Redis key patterns
        self.job_key_pattern = "job:{job_id}"
        self.queue_key = "job_queue"
        self.priority_queue_key = "priority_job_queue"
        self.running_jobs_key = "running_jobs"
        self.worker_heartbeat_key = "worker_heartbeat:{worker_id}"
        # Phase 2.4: Dead Letter Queue
        self.dlq_key = "dead_letter_queue"
        self.dlq_job_key_pattern = "dlq_job:{job_id}"

        # In-memory fallback storage when Redis unavailable
        self.memory_jobs: Dict[str, JobRecord] = {}
        self.memory_queue: List[str] = []
        self.memory_running: Dict[str, str] = {}  # job_id -> worker_id
        self.memory_dlq: List[str] = []

    async def connect(self) -> None:
        """Initialize Redis connection with graceful fallback."""
        if not self.use_redis:
            self.logger.warning("Redis not available - using in-memory fallback job storage")
            return

        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            self.logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            self.logger.warning(f"Redis connection failed ({e}) - falling back to in-memory storage")
            self.use_redis = False
            self.redis_client = None

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None

    async def add_job(self, job: JobRecord) -> None:
        """Add a new job to the store and queue."""
        if self.use_redis and self.redis_client:
            # Redis-based storage
            job_key = self.job_key_pattern.format(job_id=job.job_id)
            job_data = job.json()
            await self.redis_client.set(job_key, job_data)

            # Add to appropriate queue based on priority
            if job.priority > 0:
                await self.redis_client.zadd(self.priority_queue_key, {job.job_id: job.priority})
            else:
                await self.redis_client.lpush(self.queue_key, job.job_id)
        else:
            # In-memory fallback storage
            self.memory_jobs[job.job_id] = job
            # Simple priority queue implementation for in-memory
            if job.priority > 0:
                # Insert in priority order (higher priority first)
                inserted = False
                for i, existing_job_id in enumerate(self.memory_queue):
                    existing_job = self.memory_jobs.get(existing_job_id)
                    if existing_job and existing_job.priority < job.priority:
                        self.memory_queue.insert(i, job.job_id)
                        inserted = True
                        break
                if not inserted:
                    self.memory_queue.append(job.job_id)
            else:
                self.memory_queue.append(job.job_id)

        self.logger.info(f"Added job {job.job_id} to queue with priority {job.priority}")

    async def get_job(self, job_id: str) -> Optional[JobRecord]:
        """Retrieve a job by ID."""
        if self.use_redis and self.redis_client:
            # Redis-based retrieval
            job_key = self.job_key_pattern.format(job_id=job_id)
            job_data = await self.redis_client.get(job_key)
            if job_data:
                return JobRecord.parse_raw(job_data)
            return None
        else:
            # In-memory fallback retrieval
            return self.memory_jobs.get(job_id)

    async def update_job(self, job: JobRecord) -> None:
        """Update job data in storage."""
        if self.use_redis and self.redis_client:
            # Redis-based update
            job_key = self.job_key_pattern.format(job_id=job.job_id)
            job_data = job.json()
            await self.redis_client.set(job_key, job_data)
        else:
            # In-memory fallback update
            self.memory_jobs[job.job_id] = job

    async def get_next_job(self) -> Optional[str]:
        """Get the next job ID from the queue (priority first, then FIFO)."""
        if self.use_redis and self.redis_client:
            # Redis-based queue retrieval
            # Check priority queue first
            priority_jobs = await self.redis_client.zrevrange(self.priority_queue_key, 0, 0)
            if priority_jobs:
                job_id = priority_jobs[0]
                await self.redis_client.zrem(self.priority_queue_key, job_id)
                return job_id
            # Then check regular queue
            job_id = await self.redis_client.rpop(self.queue_key)
            return job_id
        else:
            # In-memory fallback queue retrieval
            if self.memory_queue:
                return self.memory_queue.pop(0)
            return None

    async def mark_job_running(self, job_id: str, worker_id: str) -> None:
        """Mark a job as running and track the worker."""
        job = await self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.status = JobStatus.RUNNING
        job.started_at = datetime.datetime.utcnow()
        job.last_heartbeat = datetime.datetime.utcnow()
        job.worker_id = worker_id

        await self.update_job(job)

        if self.use_redis and self.redis_client:
            await self.redis_client.sadd(self.running_jobs_key, job_id)
        else:
            # In-memory fallback tracking
            self.memory_running[job_id] = worker_id

    async def mark_job_completed(self, job_id: str, result: Dict[str, Any]) -> None:
        """Mark a job as completed with results."""
        job = await self.get_job(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.status = JobStatus.COMPLETED
        job.finished_at = datetime.datetime.utcnow()
        job.result = result

        await self.update_job(job)

        if self.use_redis and self.redis_client:
            await self.redis_client.srem(self.running_jobs_key, job_id)
        else:
            # In-memory fallback cleanup
            self.memory_running.pop(job_id, None)

    async def mark_job_failed(self, job_id: str, error: str, should_retry: bool = True) -> None:
        """Mark a job as failed and optionally retry."""
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

            if self.use_redis and self.redis_client:
                if job.priority > 0:
                    await self.redis_client.zadd(self.priority_queue_key, {job.job_id: job.priority})
                else:
                    await self.redis_client.lpush(self.queue_key, job.job_id)
            else:
                # In-memory re-queueing with priority
                if job.priority > 0:
                    # Insert in priority order
                    inserted = False
                    for i, existing_job_id in enumerate(self.memory_queue):
                        existing_job = self.memory_jobs.get(existing_job_id)
                        if existing_job and existing_job.priority < job.priority:
                            self.memory_queue.insert(i, job.job_id)
                            inserted = True
                            break
                    if not inserted:
                        self.memory_queue.append(job.job_id)
                else:
                    self.memory_queue.append(job.job_id)

            self.logger.info(f"Retrying job {job_id} (attempt {job.retry_count}/{job.max_retries})")
        else:
            job.status = JobStatus.FAILED
            self.logger.error(f"Job {job_id} failed permanently: {error}")

            # Phase 2.4: Add to Dead Letter Queue
            await self._add_to_dead_letter_queue(job)

        await self.update_job(job)

        if self.use_redis and self.redis_client:
            await self.redis_client.srem(self.running_jobs_key, job_id)
        else:
            # In-memory cleanup
            self.memory_running.pop(job_id, None)

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job if it's not already completed."""
        job = await self.get_job(job_id)
        if not job:
            return False

        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            return False

        job.status = JobStatus.CANCELLED
        job.finished_at = datetime.datetime.utcnow()

        await self.update_job(job)

        if self.use_redis and self.redis_client:
            await self.redis_client.srem(self.running_jobs_key, job_id)
            # Remove from queues if not started
            if job.status == JobStatus.QUEUED:
                await self.redis_client.lrem(self.queue_key, 0, job_id)
                await self.redis_client.zrem(self.priority_queue_key, job_id)
        else:
            # In-memory cleanup
            self.memory_running.pop(job_id, None)
            # Remove from in-memory queue if not started
            if job_id in self.memory_queue:
                self.memory_queue.remove(job_id)

        self.logger.info(f"Cancelled job {job_id}")
        return True

    async def update_heartbeat(self, job_id: str, worker_id: str) -> None:
        """Update job heartbeat to indicate worker is alive."""
        job = await self.get_job(job_id)
        if job and job.status == JobStatus.RUNNING:
            job.last_heartbeat = datetime.datetime.utcnow()
            await self.update_job(job)

            if self.use_redis and self.redis_client:
                # Update worker heartbeat in Redis
                worker_key = self.worker_heartbeat_key.format(worker_id=worker_id)
                await self.redis_client.setex(worker_key, 60, datetime.datetime.utcnow().isoformat())
            # Note: In-memory mode doesn't need persistent worker heartbeat tracking

    async def cleanup_expired_jobs(self) -> List[str]:
        """Find and cleanup expired/stuck jobs."""
        if self.use_redis and self.redis_client:
            running_job_ids = await self.redis_client.smembers(self.running_jobs_key)
        else:
            # In-memory fallback - get running job IDs from memory
            running_job_ids = list(self.memory_running.keys())

        expired_jobs = []

        for job_id in running_job_ids:
            job = await self.get_job(job_id)
            if not job:
                # Job data missing but still in running set
                if self.use_redis and self.redis_client:
                    await self.redis_client.srem(self.running_jobs_key, job_id)
                else:
                    self.memory_running.pop(job_id, None)
                continue

            if job.is_expired:
                job.status = JobStatus.TIMEOUT
                job.finished_at = datetime.datetime.utcnow()
                job.error = f"Job timed out after {job.timeout_seconds} seconds"

                await self.update_job(job)

                if self.use_redis and self.redis_client:
                    await self.redis_client.srem(self.running_jobs_key, job_id)
                else:
                    self.memory_running.pop(job_id, None)

                expired_jobs.append(job_id)
                self.logger.warning(f"Job {job_id} timed out and was cleaned up")

        return expired_jobs

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics for monitoring."""
        if self.use_redis and self.redis_client:
            regular_queue_size = await self.redis_client.llen(self.queue_key)
            priority_queue_size = await self.redis_client.zcard(self.priority_queue_key)
            running_jobs_count = await self.redis_client.scard(self.running_jobs_key)
            # Phase 2.4: Include DLQ stats
            dlq_stats = await self.get_dlq_stats()
        else:
            # In-memory fallback stats
            # Count priority jobs in memory queue
            priority_count = sum(1 for job_id in self.memory_queue
                               if job_id in self.memory_jobs and self.memory_jobs[job_id].priority > 0)
            regular_queue_size = len(self.memory_queue) - priority_count
            priority_queue_size = priority_count
            running_jobs_count = len(self.memory_running)
            # Simple DLQ stats for memory mode
            dlq_stats = {
                "total_jobs": len(self.memory_dlq),
                "oldest_timestamp": None,
                "newest_timestamp": None
            }

        return {
            "regular_queue_size": regular_queue_size,
            "priority_queue_size": priority_queue_size,
            "running_jobs_count": running_jobs_count,
            "total_queued": regular_queue_size + priority_queue_size,
            "dead_letter_queue": dlq_stats
        }

    async def list_running_jobs(self) -> List[JobRecord]:
        """Get list of currently running jobs."""
        if self.use_redis and self.redis_client:
            running_job_ids = await self.redis_client.smembers(self.running_jobs_key)
        else:
            # In-memory fallback - get running job IDs from memory
            running_job_ids = list(self.memory_running.keys())

        jobs = []
        for job_id in running_job_ids:
            job = await self.get_job(job_id)
            if job:
                jobs.append(job)

        return jobs

    # Phase 2.4: Dead Letter Queue implementation
    async def _add_to_dead_letter_queue(self, job: JobRecord) -> None:
        """Add a permanently failed job to the dead letter queue."""
        dlq_entry = {
            "job_id": job.job_id,
            "task_name": job.task_name,
            "params": job.params,
            "error": job.error,
            "retry_count": job.retry_count,
            "failed_at": job.finished_at.isoformat() if job.finished_at else None,
            "created_at": job.created_at.isoformat(),
            "worker_id": job.worker_id,
            "reason": "max_retries_exceeded"
        }

        if self.use_redis and self.redis_client:
            # Store in DLQ with timestamp as score for ordering
            timestamp = job.finished_at.timestamp() if job.finished_at else datetime.datetime.utcnow().timestamp()
            await self.redis_client.zadd(self.dlq_key, {job.job_id: timestamp})

            # Store detailed DLQ entry
            dlq_job_key = self.dlq_job_key_pattern.format(job_id=job.job_id)
            await self.redis_client.set(dlq_job_key, json.dumps(dlq_entry))
        else:
            # In-memory DLQ fallback - simple append
            self.memory_dlq.append(job.job_id)
            # Store DLQ entry in memory jobs with a special DLQ marker
            dlq_job = job.copy(deep=True)
            dlq_job.status = JobStatus.FAILED
            self.memory_jobs[f"dlq_{job.job_id}"] = dlq_job

        self.logger.warning(f"📮 Job {job.job_id} added to Dead Letter Queue (reason: max_retries_exceeded)")

    async def get_dead_letter_jobs(self, start: int = 0, count: int = 10) -> List[Dict[str, Any]]:
        """Get jobs from the dead letter queue."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        # Get job IDs from DLQ (newest first)
        job_ids = await self.redis_client.zrevrange(self.dlq_key, start, start + count - 1)

        dlq_jobs = []
        for job_id in job_ids:
            dlq_job_key = self.dlq_job_key_pattern.format(job_id=job_id)
            dlq_data = await self.redis_client.get(dlq_job_key)
            if dlq_data:
                dlq_jobs.append(json.loads(dlq_data))

        return dlq_jobs

    async def replay_dead_letter_job(self, job_id: str, reset_retries: bool = True) -> bool:
        """Replay a job from the dead letter queue."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        # Get DLQ entry
        dlq_job_key = self.dlq_job_key_pattern.format(job_id=job_id)
        dlq_data = await self.redis_client.get(dlq_job_key)

        if not dlq_data:
            return False

        dlq_entry = json.loads(dlq_data)

        # Create new job from DLQ entry
        new_job = JobRecord(
            job_id=str(uuid.uuid4()),  # New job ID
            task_name=dlq_entry["task_name"],
            params=dlq_entry["params"],
            retry_count=0 if reset_retries else dlq_entry["retry_count"]
        )

        # Add to queue
        await self.add_job(new_job)

        self.logger.info(f"🔁 Replayed dead letter job {job_id} as new job {new_job.job_id}")
        return True

    async def get_dlq_stats(self) -> Dict[str, Any]:
        """Get dead letter queue statistics."""
        if not self.redis_client:
            raise RuntimeError("Redis client not connected")

        total_dlq_jobs = await self.redis_client.zcard(self.dlq_key)

        # Get recent failures (last 24 hours)
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        recent_failures = await self.redis_client.zcount(
            self.dlq_key,
            yesterday.timestamp(),
            datetime.datetime.utcnow().timestamp()
        )

        return {
            "total_dead_letter_jobs": total_dlq_jobs,
            "recent_failures_24h": recent_failures,
            "oldest_failure": await self._get_oldest_dlq_timestamp(),
            "newest_failure": await self._get_newest_dlq_timestamp()
        }

    async def _get_oldest_dlq_timestamp(self) -> Optional[str]:
        """Get timestamp of oldest DLQ entry."""
        if not self.redis_client:
            return None

        oldest = await self.redis_client.zrange(self.dlq_key, 0, 0, withscores=True)
        if oldest:
            timestamp = oldest[0][1]
            return datetime.datetime.fromtimestamp(timestamp).isoformat()
        return None

    async def _get_newest_dlq_timestamp(self) -> Optional[str]:
        """Get timestamp of newest DLQ entry."""
        if not self.redis_client:
            return None

        newest = await self.redis_client.zrevrange(self.dlq_key, 0, 0, withscores=True)
        if newest:
            timestamp = newest[0][1]
            return datetime.datetime.fromtimestamp(timestamp).isoformat()
        return None


class JobManager:
    """High-level job management interface."""

    def __init__(self, job_store: JobStore, logger: Optional[logging.Logger] = None):
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

        job = JobRecord(
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

    async def get_job_status(self, job_id: str) -> Optional[JobRecord]:
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
