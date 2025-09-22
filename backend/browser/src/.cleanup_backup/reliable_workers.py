"""Reliable worker system with proper resource management and error handling.

This module implements Phase 1.2-1.3 of the reliability improvement plan:
- Browser context management with proper cleanup
- Job timeout and cancellation mechanisms
- Worker health monitoring and restart capabilities
- Resource leak prevention
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional
from contextlib import asynccontextmanager

from playwright.async_api import Browser, BrowserContext, Page

from .reliable_jobs import ReliableJobStore, ReliableJobRecord, JobStatus


TaskFunc = Callable[..., Awaitable[Dict[str, Any]]]


class WorkerError(Exception):
    """Base exception for worker-related errors."""
    pass


class JobTimeoutError(WorkerError):
    """Raised when a job exceeds its timeout."""
    pass


class BrowserContextManager:
    """Manages browser contexts with proper lifecycle and cleanup."""

    def __init__(self, browser: Browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        self._active_contexts: Dict[str, BrowserContext] = {}

    @asynccontextmanager
    async def get_context(self, job_id: str, **context_options):
        """Get a dedicated browser context for a job with guaranteed cleanup."""
        context = None
        try:
            # Create new context with options
            context_options.setdefault('user_agent',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            context = await self.browser.new_context(**context_options)
            self._active_contexts[job_id] = context

            self.logger.debug(f"Created browser context for job {job_id}")
            yield context

        except Exception as e:
            self.logger.error(f"Error in browser context for job {job_id}: {e}")
            raise
        finally:
            # Guaranteed cleanup
            if context:
                try:
                    await context.close()
                    self.logger.debug(f"Closed browser context for job {job_id}")
                except Exception as e:
                    self.logger.warning(f"Error closing context for job {job_id}: {e}")
                finally:
                    self._active_contexts.pop(job_id, None)

    async def cleanup_all_contexts(self) -> None:
        """Emergency cleanup of all active contexts."""
        if not self._active_contexts:
            return

        self.logger.warning(f"Emergency cleanup of {len(self._active_contexts)} active contexts")

        cleanup_tasks = []
        for job_id, context in self._active_contexts.items():
            cleanup_tasks.append(self._safe_close_context(job_id, context))

        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        self._active_contexts.clear()

    async def _safe_close_context(self, job_id: str, context: BrowserContext) -> None:
        """Safely close a browser context with error handling."""
        try:
            await context.close()
            self.logger.debug(f"Emergency closed context for job {job_id}")
        except Exception as e:
            self.logger.warning(f"Error during emergency cleanup of context {job_id}: {e}")

    def get_active_context_count(self) -> int:
        """Get the number of active browser contexts."""
        return len(self._active_contexts)


class ReliableWorker:
    """Enhanced worker with proper resource management and error handling."""

    def __init__(
        self,
        worker_id: str,
        *,
        job_store: ReliableJobStore,
        browser: Browser,
        task_registry: Dict[str, TaskFunc],
        data_root: str,
        logger: logging.Logger,
        heartbeat_interval: int = 30
    ):
        self.worker_id = worker_id
        self.job_store = job_store
        self.browser = browser
        self.task_registry = task_registry
        self.data_root = data_root
        self.logger = logger.getChild(f"worker-{worker_id}")
        self.heartbeat_interval = heartbeat_interval

        self.context_manager = BrowserContextManager(browser, self.logger)
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._current_job: Optional[ReliableJobRecord] = None
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the worker."""
        self._task = asyncio.create_task(self._run_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self.logger.info(f"Worker {self.worker_id} started")

    async def stop(self) -> None:
        """Stop the worker gracefully."""
        self.logger.info(f"Stopping worker {self.worker_id}")
        self._shutdown_event.set()

        # Cancel current job if running
        if self._current_job:
            await self.job_store.cancel_job(self._current_job.job_id)

        # Stop tasks
        if self._task:
            self._task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

        # Wait for tasks to complete
        tasks = [t for t in [self._task, self._heartbeat_task] if t]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Emergency cleanup
        await self.context_manager.cleanup_all_contexts()
        self.logger.info(f"Worker {self.worker_id} stopped")

    async def _run_loop(self) -> None:
        """Main worker loop."""
        while not self._shutdown_event.is_set():
            try:
                # Get next job
                job_id = await self.job_store.get_next_job()
                if not job_id:
                    await asyncio.sleep(1)  # No jobs available
                    continue

                # Get job details
                job = await self.job_store.get_job(job_id)
                if not job:
                    self.logger.warning(f"Job {job_id} not found in store")
                    continue

                # Mark job as running
                await self.job_store.mark_job_running(job_id, self.worker_id)
                self._current_job = job

                # Process the job with timeout
                try:
                    await asyncio.wait_for(
                        self._process_job(job),
                        timeout=job.timeout_seconds
                    )
                except asyncio.TimeoutError:
                    await self.job_store.mark_job_failed(
                        job_id,
                        f"Job timed out after {job.timeout_seconds} seconds",
                        should_retry=True
                    )
                    self.logger.error(f"Job {job_id} timed out")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in worker loop: {e}")
                await asyncio.sleep(5)  # Back off on errors
            finally:
                self._current_job = None

    async def _process_job(self, job: ReliableJobRecord) -> None:
        """Process a single job with proper resource management."""
        job_id = job.job_id
        self.logger.info(f"Processing job {job_id} (task: {job.task_name})")

        # Create job output directory
        job_output_dir = os.path.join(self.data_root, job.task_name, job_id)
        os.makedirs(job_output_dir, exist_ok=True)

        # Setup job logger
        job_logger = self._setup_job_logger(job_id, job_output_dir)

        try:
            # Get task function
            if job.task_name not in self.task_registry:
                raise WorkerError(f"Unknown task '{job.task_name}'")

            task_fn = self.task_registry[job.task_name]

            # Process with dedicated browser context
            async with self.context_manager.get_context(job_id) as context:
                # Execute the task
                result = await task_fn(
                    browser=self.browser,
                    context=context,  # Pass context instead of browser for isolation
                    params=job.params,
                    job_output_dir=job_output_dir,
                    logger=job_logger,
                )

                # Mark as completed
                await self.job_store.mark_job_completed(job_id, result)
                self.logger.info(f"Job {job_id} completed successfully")

        except Exception as e:
            error_msg = str(e)
            await self.job_store.mark_job_failed(job_id, error_msg, should_retry=True)
            self.logger.error(f"Job {job_id} failed: {error_msg}")
            job_logger.error(f"Job failed: {error_msg}")

    def _setup_job_logger(self, job_id: str, job_output_dir: str) -> logging.Logger:
        """Setup a dedicated logger for the job."""
        job_log_path = os.path.join(job_output_dir, "job.log")
        job_logger = logging.getLogger(f"browser.job.{job_id}")

        # Remove existing handlers to avoid duplicates
        for handler in job_logger.handlers[:]:
            job_logger.removeHandler(handler)

        # File handler
        fh = logging.FileHandler(job_log_path)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        job_logger.addHandler(fh)

        # Console handler
        for handler in self.logger.handlers:
            job_logger.addHandler(handler)

        job_logger.setLevel(logging.INFO)
        return job_logger

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats for the current job."""
        while not self._shutdown_event.is_set():
            try:
                if self._current_job:
                    await self.job_store.update_heartbeat(
                        self._current_job.job_id,
                        self.worker_id
                    )

                await asyncio.sleep(self.heartbeat_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(self.heartbeat_interval)

    def get_status(self) -> Dict[str, Any]:
        """Get worker status information."""
        return {
            "worker_id": self.worker_id,
            "current_job": self._current_job.job_id if self._current_job else None,
            "current_task": self._current_job.task_name if self._current_job else None,
            "active_contexts": self.context_manager.get_active_context_count(),
            "started_at": self._current_job.started_at.isoformat() if self._current_job and self._current_job.started_at else None
        }


class ReliableWorkerPool:
    """Enhanced worker pool with health monitoring and auto-restart."""

    def __init__(
        self,
        *,
        job_store: ReliableJobStore,
        browser: Browser,
        task_registry: Dict[str, TaskFunc],
        data_root: str,
        logger: logging.Logger,
        max_workers: int = 2
    ):
        self.job_store = job_store
        self.browser = browser
        self.task_registry = task_registry
        self.data_root = data_root
        self.logger = logger
        self.max_workers = max_workers

        self._workers: Dict[str, ReliableWorker] = {}
        self._health_monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """Start the worker pool."""
        self.logger.info(f"Starting worker pool with {self.max_workers} workers")

        # Start workers
        for i in range(self.max_workers):
            await self._start_worker(f"worker-{i}")

        # Start health monitor
        self._health_monitor_task = asyncio.create_task(self._health_monitor_loop())

        self.logger.info(f"Worker pool started with {len(self._workers)} workers")

    async def stop(self) -> None:
        """Stop all workers and the pool."""
        self.logger.info("Stopping worker pool")
        self._shutdown_event.set()

        # Stop health monitor
        if self._health_monitor_task:
            self._health_monitor_task.cancel()

        # Stop all workers
        stop_tasks = [worker.stop() for worker in self._workers.values()]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

        self._workers.clear()
        self.logger.info("Worker pool stopped")

    async def _start_worker(self, worker_id: str) -> None:
        """Start a single worker."""
        worker = ReliableWorker(
            worker_id=worker_id,
            job_store=self.job_store,
            browser=self.browser,
            task_registry=self.task_registry,
            data_root=self.data_root,
            logger=self.logger
        )

        await worker.start()
        self._workers[worker_id] = worker
        self.logger.info(f"Started worker {worker_id}")

    async def _restart_worker(self, worker_id: str) -> None:
        """Restart a failed worker."""
        self.logger.warning(f"Restarting worker {worker_id}")

        # Stop the old worker
        old_worker = self._workers.get(worker_id)
        if old_worker:
            try:
                await old_worker.stop()
            except Exception as e:
                self.logger.error(f"Error stopping worker {worker_id}: {e}")

        # Start new worker
        try:
            await self._start_worker(worker_id)
            self.logger.info(f"Successfully restarted worker {worker_id}")
        except Exception as e:
            self.logger.error(f"Failed to restart worker {worker_id}: {e}")

    async def _health_monitor_loop(self) -> None:
        """Monitor worker health and restart if needed."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)  # Check every minute

                # Check if we have the right number of workers
                if len(self._workers) < self.max_workers:
                    self.logger.warning(f"Worker count below target: {len(self._workers)}/{self.max_workers}")

                    # Find missing workers and restart them
                    expected_workers = {f"worker-{i}" for i in range(self.max_workers)}
                    missing_workers = expected_workers - set(self._workers.keys())

                    for worker_id in missing_workers:
                        await self._start_worker(worker_id)

                # Check worker health (could be extended with more checks)
                unhealthy_workers = []
                for worker_id, worker in self._workers.items():
                    if worker._task and worker._task.done():
                        exception = worker._task.exception()
                        if exception:
                            self.logger.error(f"Worker {worker_id} died with exception: {exception}")
                            unhealthy_workers.append(worker_id)

                # Restart unhealthy workers
                for worker_id in unhealthy_workers:
                    await self._restart_worker(worker_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health monitor: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get worker pool statistics."""
        worker_stats = []
        for worker in self._workers.values():
            worker_stats.append(worker.get_status())

        return {
            "worker_count": len(self._workers),
            "max_workers": self.max_workers,
            "workers": worker_stats
        }