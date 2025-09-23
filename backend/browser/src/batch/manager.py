"""Intelligent Batching System for Large-Scale Data Extraction.

Phase 6.1: Batch Management for 1000+ Post Requirements
- Automatic job splitting based on proven capacity limits
- Intelligent scheduling and resource management
- Progress tracking and failure recovery
- Data consolidation and deduplication
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import json
import uuid


class BatchStatus(str, Enum):
    """Status of batch processing."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


@dataclass
class BatchJob:
    """Individual batch job within a larger extraction."""
    batch_id: str
    parent_job_id: str
    job_payload: Dict[str, Any]
    status: BatchStatus = BatchStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    posts_extracted: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0
    job_id: Optional[str] = None  # Actual extraction job ID


@dataclass
class BatchExtractionRequest:
    """Request for large-scale batch extraction."""
    parent_job_id: str
    username: str
    total_posts_needed: int
    batch_strategy: str = "intelligent"
    max_posts_per_batch: int = 150  # Proven safe limit
    max_concurrent_batches: int = 3
    retry_failed_batches: bool = True
    max_retries: int = 2
    batch_delay_seconds: int = 5  # Delay between batch starts
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class BatchProgress:
    """Progress tracking for batch extraction."""
    parent_job_id: str
    total_batches: int
    completed_batches: int
    failed_batches: int
    total_posts_extracted: int
    total_posts_needed: int
    estimated_completion: Optional[datetime]
    average_batch_time: float
    success_rate: float


class BatchManager:
    """Manages large-scale extractions through intelligent batching."""

    def __init__(self, job_submitter, logger: Optional[logging.Logger] = None):
        self.job_submitter = job_submitter
        self.logger = logger or logging.getLogger(__name__)

        # Active batch tracking
        self.active_extractions: Dict[str, BatchExtractionRequest] = {}
        self.batch_jobs: Dict[str, List[BatchJob]] = {}
        self.running_jobs: Dict[str, BatchJob] = {}

        # Performance tracking
        self.batch_performance_history: List[Dict[str, Any]] = []

        # Configuration from testing results
        self.PROVEN_SAFE_LIMIT = 150
        self.PERFORMANCE_CEILING = 300
        self.MAX_CONCURRENT_SAFE = 3
        self.EXPECTED_SPEED = 0.8  # posts per second

    async def submit_large_extraction(self, request: BatchExtractionRequest) -> str:
        """Submit a large extraction request and return tracking ID."""
        self.logger.info(f"Submitting large extraction: {request.total_posts_needed} posts for @{request.username}")

        # Generate batches
        batches = self._create_batches(request)

        # Store request and batches
        self.active_extractions[request.parent_job_id] = request
        self.batch_jobs[request.parent_job_id] = batches

        # Start processing
        asyncio.create_task(self._process_batch_extraction(request.parent_job_id))

        self.logger.info(f"Created {len(batches)} batches for extraction {request.parent_job_id}")
        return request.parent_job_id

    def _create_batches(self, request: BatchExtractionRequest) -> List[BatchJob]:
        """Create batch jobs based on intelligent batching strategy."""
        batches = []

        # Calculate optimal batch size based on request and proven limits
        if request.batch_strategy == "intelligent":
            # Use proven safe limit for reliability
            batch_size = min(request.max_posts_per_batch, self.PROVEN_SAFE_LIMIT)
        elif request.batch_strategy == "aggressive":
            # Use performance ceiling for speed (higher risk)
            batch_size = min(request.max_posts_per_batch, self.PERFORMANCE_CEILING)
        else:
            # Conservative approach
            batch_size = min(request.max_posts_per_batch, 50)

        # Create batch jobs
        posts_remaining = request.total_posts_needed
        batch_number = 1
        posts_offset = 0

        while posts_remaining > 0:
            posts_in_batch = min(batch_size, posts_remaining)

            # Create job payload for this batch
            job_payload = {
                "username": request.username,
                "scrape_posts": True,
                "max_posts": posts_in_batch,
                "scrape_level": 4,
                "batch_info": {
                    "batch_number": batch_number,
                    "posts_offset": posts_offset,
                    "parent_job_id": request.parent_job_id
                }
            }

            batch_job = BatchJob(
                batch_id=f"{request.parent_job_id}_batch_{batch_number}",
                parent_job_id=request.parent_job_id,
                job_payload=job_payload
            )

            batches.append(batch_job)

            posts_remaining -= posts_in_batch
            posts_offset += posts_in_batch
            batch_number += 1

        return batches

    async def _process_batch_extraction(self, parent_job_id: str) -> None:
        """Process all batches for a large extraction."""
        request = self.active_extractions[parent_job_id]
        batches = self.batch_jobs[parent_job_id]

        self.logger.info(f"Starting batch processing for {parent_job_id}: {len(batches)} batches")

        # Track performance
        start_time = time.time()
        completed_batches = 0
        failed_batches = 0
        total_posts_extracted = 0

        # Process batches with concurrency control
        semaphore = asyncio.Semaphore(request.max_concurrent_batches)

        async def process_single_batch(batch: BatchJob) -> Tuple[bool, int]:
            """Process a single batch job."""
            async with semaphore:
                return await self._execute_batch(batch, request)

        # Execute all batches
        tasks = [process_single_batch(batch) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Analyze results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Batch {i+1} failed with exception: {result}")
                failed_batches += 1
            else:
                success, posts_count = result
                if success:
                    completed_batches += 1
                    total_posts_extracted += posts_count
                else:
                    failed_batches += 1

        # Handle failed batches with retries
        if failed_batches > 0 and request.retry_failed_batches:
            self.logger.info(f"Retrying {failed_batches} failed batches...")
            await self._retry_failed_batches(parent_job_id, request)

        # Generate final results
        total_time = time.time() - start_time
        success_rate = completed_batches / len(batches) * 100

        self.logger.info(f"Batch extraction {parent_job_id} completed:")
        self.logger.info(f"  Total batches: {len(batches)}")
        self.logger.info(f"  Completed: {completed_batches}")
        self.logger.info(f"  Failed: {failed_batches}")
        self.logger.info(f"  Posts extracted: {total_posts_extracted}/{request.total_posts_needed}")
        self.logger.info(f"  Success rate: {success_rate:.1f}%")
        self.logger.info(f"  Total time: {total_time:.1f} seconds")

        # Store performance data
        self._record_batch_performance(parent_job_id, {
            'total_batches': len(batches),
            'completed_batches': completed_batches,
            'failed_batches': failed_batches,
            'total_posts_extracted': total_posts_extracted,
            'total_posts_needed': request.total_posts_needed,
            'success_rate': success_rate,
            'total_time': total_time,
            'posts_per_second': total_posts_extracted / total_time if total_time > 0 else 0
        })

    async def _execute_batch(self, batch: BatchJob, request: BatchExtractionRequest) -> Tuple[bool, int]:
        """Execute a single batch job."""
        batch.status = BatchStatus.RUNNING
        batch.started_at = datetime.now()

        try:
            # Add delay to avoid overwhelming external services
            if batch.retry_count == 0:  # Only delay on first attempt
                await asyncio.sleep(request.batch_delay_seconds)

            self.logger.info(f"Executing batch {batch.batch_id}")

            # Submit job through existing job system
            job_id = await self.job_submitter.submit_job(batch.job_payload)
            batch.job_id = job_id

            # Monitor job completion
            posts_extracted = await self._monitor_batch_job(job_id, batch)

            if posts_extracted > 0:
                batch.status = BatchStatus.COMPLETED
                batch.posts_extracted = posts_extracted
                batch.completed_at = datetime.now()

                self.logger.info(f"Batch {batch.batch_id} completed: {posts_extracted} posts")
                return True, posts_extracted
            else:
                batch.status = BatchStatus.FAILED
                batch.error_message = "No posts extracted"
                return False, 0

        except Exception as e:
            batch.status = BatchStatus.FAILED
            batch.error_message = str(e)
            self.logger.error(f"Batch {batch.batch_id} failed: {e}")
            return False, 0

    async def _monitor_batch_job(self, job_id: str, batch: BatchJob) -> int:
        """Monitor a batch job until completion."""
        max_wait_time = 600  # 10 minutes max per batch
        check_interval = 5   # Check every 5 seconds

        for _ in range(max_wait_time // check_interval):
            try:
                # Check job status through existing system
                status = await self.job_submitter.get_job_status(job_id)

                if status == "finished":
                    result = await self.job_submitter.get_job_result(job_id)
                    return self._extract_posts_count(result)
                elif status == "error":
                    batch.error_message = "Job execution error"
                    return 0

                await asyncio.sleep(check_interval)

            except Exception as e:
                self.logger.error(f"Error monitoring batch job {job_id}: {e}")
                return 0

        # Timeout
        batch.error_message = f"Batch timeout after {max_wait_time} seconds"
        return 0

    def _extract_posts_count(self, job_result: Dict[str, Any]) -> int:
        """Extract the number of posts from a job result."""
        try:
            if 'data' in job_result and len(job_result['data']) > 0:
                first_result = job_result['data'][0]
                if 'posts' in first_result:
                    return len(first_result['posts'])
            return 0
        except Exception:
            return 0

    async def _retry_failed_batches(self, parent_job_id: str, request: BatchExtractionRequest) -> None:
        """Retry failed batches with exponential backoff."""
        batches = self.batch_jobs[parent_job_id]
        failed_batches = [b for b in batches if b.status == BatchStatus.FAILED]

        for batch in failed_batches:
            if batch.retry_count < request.max_retries:
                batch.retry_count += 1
                batch.status = BatchStatus.RETRYING

                # Exponential backoff
                delay = 2 ** batch.retry_count * 10  # 20s, 40s, 80s
                await asyncio.sleep(delay)

                success, posts_count = await self._execute_batch(batch, request)
                if success:
                    self.logger.info(f"Retry successful for batch {batch.batch_id}")

    def _record_batch_performance(self, parent_job_id: str, performance_data: Dict[str, Any]) -> None:
        """Record batch performance for optimization."""
        performance_record = {
            'parent_job_id': parent_job_id,
            'timestamp': datetime.now().isoformat(),
            **performance_data
        }

        self.batch_performance_history.append(performance_record)

        # Keep only last 100 records for memory efficiency
        if len(self.batch_performance_history) > 100:
            self.batch_performance_history = self.batch_performance_history[-100:]

    def get_batch_progress(self, parent_job_id: str) -> Optional[BatchProgress]:
        """Get current progress of a batch extraction."""
        if parent_job_id not in self.batch_jobs:
            return None

        batches = self.batch_jobs[parent_job_id]
        request = self.active_extractions[parent_job_id]

        completed_batches = len([b for b in batches if b.status == BatchStatus.COMPLETED])
        failed_batches = len([b for b in batches if b.status == BatchStatus.FAILED])
        total_posts_extracted = sum(b.posts_extracted for b in batches)

        # Calculate estimates
        if completed_batches > 0:
            completed_times = [
                (b.completed_at - b.started_at).total_seconds()
                for b in batches
                if b.completed_at and b.started_at
            ]
            avg_batch_time = sum(completed_times) / len(completed_times) if completed_times else 0

            remaining_batches = len(batches) - completed_batches - failed_batches
            estimated_completion = datetime.now() + timedelta(seconds=remaining_batches * avg_batch_time)
        else:
            avg_batch_time = 0
            estimated_completion = None

        success_rate = completed_batches / len(batches) * 100 if batches else 0

        return BatchProgress(
            parent_job_id=parent_job_id,
            total_batches=len(batches),
            completed_batches=completed_batches,
            failed_batches=failed_batches,
            total_posts_extracted=total_posts_extracted,
            total_posts_needed=request.total_posts_needed,
            estimated_completion=estimated_completion,
            average_batch_time=avg_batch_time,
            success_rate=success_rate
        )

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary of batch operations."""
        if not self.batch_performance_history:
            return {"message": "No batch performance data available"}

        recent_records = self.batch_performance_history[-10:]  # Last 10 extractions

        avg_success_rate = sum(r['success_rate'] for r in recent_records) / len(recent_records)
        avg_posts_per_second = sum(r['posts_per_second'] for r in recent_records) / len(recent_records)
        total_posts_extracted = sum(r['total_posts_extracted'] for r in recent_records)

        return {
            'total_extractions': len(self.batch_performance_history),
            'recent_extractions': len(recent_records),
            'average_success_rate': avg_success_rate,
            'average_posts_per_second': avg_posts_per_second,
            'total_posts_extracted_recently': total_posts_extracted,
            'proven_safe_limit': self.PROVEN_SAFE_LIMIT,
            'performance_ceiling': self.PERFORMANCE_CEILING,
            'max_concurrent_safe': self.MAX_CONCURRENT_SAFE
        }


# Global batch manager instance
_batch_manager: Optional[BatchManager] = None


def get_batch_manager(job_submitter=None) -> BatchManager:
    """Get or create global batch manager instance."""
    global _batch_manager

    if _batch_manager is None:
        _batch_manager = BatchManager(job_submitter)

    return _batch_manager