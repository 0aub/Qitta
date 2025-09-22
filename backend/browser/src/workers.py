"""Reliable worker system with proper resource management and error handling.

This module implements Phase 1.2-1.3 and Phase 2.1 improvements:
- Browser context management with proper cleanup
- Job timeout and cancellation mechanisms
- Worker health monitoring and restart capabilities
- Resource leak prevention
- Enhanced error handling and recovery (Phase 2.1)
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional
from contextlib import asynccontextmanager
import contextlib

from playwright.async_api import Browser, BrowserContext, Page

from .jobs import JobStore, JobRecord, JobStatus, JobManager
from .reliability import (
    ErrorHandler, ErrorContext, EnhancedError,
    NetworkError, BrowserError, TimeoutError,
    ErrorSeverity, RecoveryStrategy, ErrorCategory,
    ResourceMonitor, AdaptiveWorkerPool, ConcurrencyThrottler,
    ResourceOptimizer, ResourceState, ScalingAction,
    CircuitBreakerManager, CircuitBreakerConfig, CircuitBreakerError,
    FallbackManager, FallbackConfig, FallbackStrategy, ServiceLevel
)


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


class Worker:
    """Enhanced worker with proper resource management and error handling."""

    def __init__(
        self,
        worker_id: str,
        *,
        job_store: JobStore,
        browser: Browser,
        task_registry: Dict[str, TaskFunc],
        data_root: str,
        logger: logging.Logger,
        heartbeat_interval: int = 30,
        circuit_breaker_manager: Optional[CircuitBreakerManager] = None,
        fallback_manager: Optional[FallbackManager] = None
    ):
        self.worker_id = worker_id
        self.job_store = job_store
        self.browser = browser
        self.task_registry = task_registry
        self.data_root = data_root
        self.logger = logger.getChild(f"worker-{worker_id}")
        self.heartbeat_interval = heartbeat_interval

        self.context_manager = BrowserContextManager(browser, self.logger)
        self.error_handler = ErrorHandler(self.logger)
        self._task: Optional[asyncio.Task] = None

        # Phase 2.5: Circuit breaker support
        self.circuit_breaker_manager = circuit_breaker_manager

        # Phase 2.6: Fallback manager support
        self.fallback_manager = fallback_manager

        self._heartbeat_task: Optional[asyncio.Task] = None
        self._current_job: Optional[JobRecord] = None
        self._shutdown_event = asyncio.Event()
        self._consecutive_failures = 0

    def _get_circuit_breaker_name(self, task_name: str) -> str:
        """Map task names to appropriate circuit breakers."""
        if task_name.startswith("twitter"):
            return "twitter_navigation"
        else:
            return "browser_navigation"

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

    async def _process_job(self, job: JobRecord) -> None:
        """Process a single job with enhanced error handling and recovery."""
        job_id = job.job_id
        self.logger.info(f"Processing job {job_id} (task: {job.task_name})")

        # Create job output directory
        job_output_dir = os.path.join(self.data_root, job.task_name, job_id)
        os.makedirs(job_output_dir, exist_ok=True)

        # Setup job logger
        job_logger = self._setup_job_logger(job_id, job_output_dir)

        # Create error context
        error_context = ErrorContext(
            timestamp=datetime.datetime.utcnow(),
            job_id=job_id,
            task_name=job.task_name,
            worker_id=self.worker_id,
            parameters=job.params
        )

        try:
            # Get task function
            if job.task_name not in self.task_registry:
                raise WorkerError(f"Unknown task '{job.task_name}'")

            task_fn = self.task_registry[job.task_name]

            # Process with dedicated browser context
            async with self.context_manager.get_context(job_id) as context:
                # Update error context with browser state
                error_context.browser_state = {
                    "browser_connected": self.browser.is_connected(),
                    "context_id": id(context)
                }

                # Phase 2.5: Execute task through circuit breaker
                circuit_breaker_name = self._get_circuit_breaker_name(job.task_name)

                async def execute_task():
                    # Check if task function expects context parameter
                    import inspect
                    sig = inspect.signature(task_fn)
                    if 'context' in sig.parameters:
                        # New context-aware task function
                        return await task_fn(
                            browser=self.browser,
                            context=context,
                            params=job.params,
                            job_output_dir=job_output_dir,
                            logger=job_logger,
                        )
                    else:
                        # Legacy task function - pass browser
                        return await task_fn(
                            browser=self.browser,
                            params=job.params,
                            job_output_dir=job_output_dir,
                            logger=job_logger,
                        )

                try:
                    # Phase 2.6: Execute with fallback support
                    if self.fallback_manager:
                        # Prepare fallback data
                        fallback_data = {
                            "task_name": job.task_name,
                            "params": job.params,
                            "worker_id": self.worker_id,
                            "job_id": job_id
                        }

                        # Execute with fallback mechanisms
                        fallback_execution = await self.fallback_manager.execute_with_fallback(
                            service_name=circuit_breaker_name,
                            primary_func=execute_task,
                            fallback_data=fallback_data
                        )

                        if not fallback_execution.success:
                            # Fallback also failed - raise error
                            raise EnhancedError(
                                f"Task execution failed: {fallback_execution.error}",
                                category=ErrorCategory.EXTERNAL_SERVICE,
                                severity=ErrorSeverity.HIGH,
                                recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF
                            )

                        # Use fallback result
                        result = fallback_execution.data

                        # Log degradation information
                        if fallback_execution.is_degraded:
                            self.logger.warning(
                                f"Job {job_id} completed with degraded service "
                                f"(strategy: {fallback_execution.strategy_used}, "
                                f"response_time: {fallback_execution.response_time_ms:.1f}ms)"
                            )

                        # Update cache if successful
                        if fallback_execution.success and result:
                            self.fallback_manager.update_cache(circuit_breaker_name, result)

                    elif self.circuit_breaker_manager:
                        # Only circuit breaker available
                        result = await self.circuit_breaker_manager.call_with_breaker(
                            circuit_breaker_name,
                            execute_task
                        )
                    else:
                        # Direct execution fallback
                        result = await execute_task()

                except CircuitBreakerError as e:
                    # Circuit breaker is open - try fallback if available
                    if self.fallback_manager:
                        self.logger.warning(f"Circuit breaker {e.circuit_name} is {e.state} - attempting fallback for job {job_id}")

                        fallback_data = {
                            "task_name": job.task_name,
                            "params": job.params,
                            "worker_id": self.worker_id,
                            "job_id": job_id,
                            "circuit_breaker_state": e.state
                        }

                        fallback_execution = await self.fallback_manager._execute_fallback(
                            circuit_breaker_name,
                            fallback_data,
                            0,  # start_time
                            f"Circuit breaker {e.circuit_name} is {e.state}"
                        )

                        if fallback_execution.success:
                            result = fallback_execution.data
                            self.logger.info(f"Job {job_id} completed using fallback strategy: {fallback_execution.strategy_used}")
                        else:
                            # Both circuit breaker and fallback failed
                            self.logger.error(f"Job {job_id} failed - circuit breaker open and fallback unsuccessful")
                            raise EnhancedError(
                                f"Service temporarily unavailable: {e.circuit_name} circuit breaker is {e.state} and fallback failed",
                                category=ErrorCategory.EXTERNAL_SERVICE,
                                severity=ErrorSeverity.HIGH,
                                recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF
                            )
                    else:
                        # No fallback available
                        self.logger.error(f"Circuit breaker {e.circuit_name} is {e.state} - rejecting job {job_id}")
                        raise EnhancedError(
                            f"Service temporarily unavailable: {e.circuit_name} circuit breaker is {e.state}",
                            category=ErrorCategory.EXTERNAL_SERVICE,
                            severity=ErrorSeverity.HIGH,
                            recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF
                        )

                # Mark as completed
                await self.job_store.mark_job_completed(job_id, result)
                self.logger.info(f"Job {job_id} completed successfully")

                # Reset failure count on success
                self._consecutive_failures = 0
                self._last_success = datetime.datetime.utcnow()

        except Exception as e:
            # Enhanced error handling
            try:
                # Update error context
                error_context.attempt_number = job.retry_count + 1
                error_context.max_attempts = job.max_retries

                # Handle error through enhanced handler
                recovery_strategy = await self.error_handler.handle_error(e, error_context)

                # Track consecutive failures
                self._consecutive_failures += 1

                # Determine retry strategy
                should_retry = self._should_retry_job(job, e, recovery_strategy)

                if should_retry:
                    await self.job_store.mark_job_failed(job_id, str(e), should_retry=True)
                    self.logger.warning(f"Job {job_id} failed, will retry: {e}")
                else:
                    await self.job_store.mark_job_failed(job_id, str(e), should_retry=False)
                    self.logger.error(f"Job {job_id} failed permanently: {e}")

                job_logger.error(f"Job failed: {e}")

                # Handle specific recovery strategies
                if recovery_strategy == RecoveryStrategy.RESTART_BROWSER:
                    await self._request_browser_restart()
                elif recovery_strategy == RecoveryStrategy.REDUCE_LOAD:
                    await self._reduce_worker_load()

            except Exception as handler_error:
                # Fallback if error handler fails
                self.logger.critical(f"Error handler failed for job {job_id}: {handler_error}")
                await self.job_store.mark_job_failed(job_id, str(e), should_retry=False)

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

    def _should_retry_job(self, job: JobRecord, error: Exception, recovery_strategy: RecoveryStrategy) -> bool:
        """Determine if a job should be retried based on error type and worker state."""

        # Don't retry if max retries reached
        if job.retry_count >= job.max_retries:
            return False

        # Don't retry if too many consecutive failures
        if self._consecutive_failures > 5:
            self.logger.warning(f"Worker {self.worker_id} has too many consecutive failures")
            return False

        # Don't retry certain recovery strategies
        if recovery_strategy in [RecoveryStrategy.FAIL, RecoveryStrategy.ALERT_HUMAN]:
            return False

        # Always retry network and timeout errors
        if recovery_strategy in [
            RecoveryStrategy.RETRY_IMMEDIATE,
            RecoveryStrategy.RETRY_BACKOFF,
            RecoveryStrategy.RETRY_EXPONENTIAL
        ]:
            return True

        return job.retry_count < job.max_retries

    async def _request_browser_restart(self) -> None:
        """Request browser restart from worker pool."""
        self.logger.warning(f"Worker {self.worker_id} requesting browser restart")
        # Set flag that worker pool can check
        self._needs_restart = True

    async def _reduce_worker_load(self) -> None:
        """Reduce worker load in response to resource errors."""
        self.logger.warning(f"Worker {self.worker_id} reducing load due to resource pressure")
        # Add delay before next job
        await asyncio.sleep(30)

    def get_status(self) -> Dict[str, Any]:
        """Get enhanced worker status information."""
        return {
            "worker_id": self.worker_id,
            "current_job": self._current_job.job_id if self._current_job else None,
            "current_task": self._current_job.task_name if self._current_job else None,
            "active_contexts": self.context_manager.get_active_context_count(),
            "started_at": self._current_job.started_at.isoformat() if self._current_job and self._current_job.started_at else None,
            "consecutive_failures": self._consecutive_failures,
            "last_success": self._last_success.isoformat(),
            "needs_restart": getattr(self, '_needs_restart', False),
            "error_stats": self.error_handler.get_error_stats()
        }


class WorkerPool:
    """Enhanced worker pool with intelligent scaling and resource management."""

    def __init__(
        self,
        *,
        job_store: JobStore,
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

        self._workers: Dict[str, Worker] = {}
        self._health_monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Phase 2.3: Resource management components
        self.resource_monitor = ResourceMonitor(logger)
        self.adaptive_pool = AdaptiveWorkerPool(self.resource_monitor, logger)
        self.throttler = ConcurrencyThrottler(logger)
        self.resource_optimizer: Optional[ResourceOptimizer] = None

        # Phase 2.5: Circuit breaker management
        self.circuit_breaker_manager = CircuitBreakerManager(logger)
        self._setup_circuit_breakers()

        # Phase 2.6: Fallback management
        self.fallback_manager = FallbackManager(self.circuit_breaker_manager, logger)
        self._setup_fallback_strategies()

        # Configure throttling for external services
        self.throttler.add_service_limit("twitter", max_concurrent=3, rate_limit_per_minute=100)
        self.throttler.add_service_limit("browser_navigation", max_concurrent=5, rate_limit_per_minute=200)

    def _setup_circuit_breakers(self) -> None:
        """Configure circuit breakers for external services."""
        # Twitter/X service circuit breaker
        self.circuit_breaker_manager.get_or_create_breaker(
            "twitter_navigation",
            CircuitBreakerConfig(
                failure_threshold=3,      # Open after 3 failures
                recovery_timeout=30.0,    # Try half-open after 30 seconds
                success_threshold=2,      # Close after 2 successes
                timeout=15.0             # 15 second request timeout
            )
        )

        # General browser navigation circuit breaker
        self.circuit_breaker_manager.get_or_create_breaker(
            "browser_navigation",
            CircuitBreakerConfig(
                failure_threshold=5,      # More tolerant for general navigation
                recovery_timeout=60.0,
                success_threshold=3,
                timeout=30.0
            )
        )

        # External API circuit breaker (for any external API calls)
        self.circuit_breaker_manager.get_or_create_breaker(
            "external_api",
            CircuitBreakerConfig(
                failure_threshold=2,      # Strict for external APIs
                recovery_timeout=120.0,   # Longer recovery time
                success_threshold=1,
                timeout=10.0
            )
        )

        self.logger.info("🔌 Circuit breakers configured for external services")

    def _setup_fallback_strategies(self) -> None:
        """Configure fallback strategies for services."""
        # Twitter fallback strategy - use cached responses
        twitter_fallback = FallbackConfig(
            strategy=FallbackStrategy.CACHED_RESPONSE,
            timeout_seconds=5.0,
            cache_ttl_seconds=300.0,  # 5 minutes
            mock_response_template={
                "status": "degraded",
                "message": "Twitter service is temporarily unavailable, using cached data",
                "data": [],
                "cached": True,
                "degraded": True
            }
        )
        self.fallback_manager.register_fallback("twitter_navigation", twitter_fallback)

        # Browser navigation fallback - reduced quality
        browser_fallback = FallbackConfig(
            strategy=FallbackStrategy.REDUCED_QUALITY,
            timeout_seconds=10.0,
            quality_reduction_factor=0.3,  # Reduce to 30% of normal
            mock_response_template={
                "status": "degraded",
                "message": "Browser service is running in reduced mode",
                "data": [],
                "reduced_quality": True,
                "degraded": True
            }
        )
        self.fallback_manager.register_fallback("browser_navigation", browser_fallback)

        # External API fallback - fail fast
        api_fallback = FallbackConfig(
            strategy=FallbackStrategy.FAIL_FAST,
            timeout_seconds=2.0
        )
        self.fallback_manager.register_fallback("external_api", api_fallback)

        self.logger.info("🔄 Fallback strategies configured for services")

    def _get_circuit_breaker_name(self, task_name: str) -> str:
        """Map task names to appropriate circuit breakers."""
        if task_name.startswith("twitter"):
            return "twitter_navigation"
        else:
            return "browser_navigation"

    async def start(self) -> None:
        """Start the enhanced worker pool with resource optimization."""
        self.logger.info(f"Starting enhanced worker pool with {self.max_workers} workers")

        # Start initial workers
        for i in range(self.max_workers):
            await self._start_worker(f"worker-{i}")

        # Start resource optimizer
        self.resource_optimizer = ResourceOptimizer(
            self.resource_monitor,
            self.adaptive_pool,
            self.throttler,
            self.logger
        )
        await self.resource_optimizer.start_optimization()

        # Start fallback manager monitoring
        await self.fallback_manager.start_monitoring()

        # Start enhanced health monitor
        self._health_monitor_task = asyncio.create_task(self._enhanced_health_monitor_loop())

        self.logger.info(f"✅ Enhanced worker pool started with {len(self._workers)} workers")
        self.logger.info("🚀 Resource optimization and dynamic scaling enabled")
        self.logger.info("🔄 Graceful degradation and fallback mechanisms active")

    async def stop(self) -> None:
        """Stop all workers and the enhanced pool."""
        self.logger.info("Stopping enhanced worker pool")
        self._shutdown_event.set()

        # Stop resource optimizer
        if self.resource_optimizer:
            await self.resource_optimizer.stop_optimization()

        # Stop fallback manager monitoring
        await self.fallback_manager.stop_monitoring()

        # Stop health monitor
        if self._health_monitor_task:
            self._health_monitor_task.cancel()

        # Stop all workers
        stop_tasks = [worker.stop() for worker in self._workers.values()]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

        self._workers.clear()
        self.logger.info("Enhanced worker pool stopped")

    async def _start_worker(self, worker_id: str) -> None:
        """Start a single worker."""
        worker = Worker(
            worker_id=worker_id,
            job_store=self.job_store,
            browser=self.browser,
            task_registry=self.task_registry,
            data_root=self.data_root,
            logger=self.logger,
            circuit_breaker_manager=self.circuit_breaker_manager,
            fallback_manager=self.fallback_manager
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

    async def _enhanced_health_monitor_loop(self) -> None:
        """Enhanced health monitoring with resource optimization."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                # Get current metrics
                queue_stats = await self.job_store.get_queue_stats()
                active_workers = len(self._workers)
                total_contexts = sum(
                    worker.context_manager.get_active_context_count()
                    for worker in self._workers.values()
                )

                current_metrics = self.resource_monitor.get_current_metrics(
                    active_workers=active_workers,
                    active_contexts=total_contexts,
                    queue_size=queue_stats.get('total_queued', 0)
                )

                # Update adaptive pool with worker metrics
                for worker_id, worker in self._workers.items():
                    worker_status = worker.get_status()
                    self.adaptive_pool.update_worker_metrics(worker_id, {
                        'current_jobs': 1 if worker_status.get('current_job') else 0,
                        'active_contexts': worker_status.get('active_contexts', 0),
                        'consecutive_failures': worker_status.get('consecutive_failures', 0)
                    })

                # Check scaling decisions
                scaling_action = self.adaptive_pool.should_scale(current_metrics)

                if scaling_action == ScalingAction.SCALE_UP:
                    await self._scale_up()
                elif scaling_action == ScalingAction.SCALE_DOWN:
                    await self._scale_down()
                elif scaling_action == ScalingAction.THROTTLE:
                    await self._apply_throttling()

                # Traditional health checks
                await self._check_worker_health()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in enhanced health monitor: {e}")

    async def _check_worker_health(self):
        """Traditional worker health checks."""
        # Check if we have the minimum number of workers
        if len(self._workers) < max(1, self.max_workers // 2):
            self.logger.warning(f"Worker count critically low: {len(self._workers)}")

        # Check worker health
        unhealthy_workers = []
        for worker_id, worker in self._workers.items():
            if worker._task and worker._task.done():
                exception = worker._task.exception()
                if exception:
                    self.logger.error(f"Worker {worker_id} died with exception: {exception}")
                    unhealthy_workers.append(worker_id)

            # Check if worker needs restart flag
            if getattr(worker, '_needs_restart', False):
                self.logger.warning(f"Worker {worker_id} requested restart")
                unhealthy_workers.append(worker_id)

        # Restart unhealthy workers
        for worker_id in unhealthy_workers:
            await self._restart_worker(worker_id)

    async def _scale_up(self):
        """Scale up worker pool."""
        if len(self._workers) >= self.max_workers:
            return

        new_worker_id = f"worker-{len(self._workers)}"
        await self._start_worker(new_worker_id)
        self.logger.info(f"🔼 Scaled up: Added worker {new_worker_id}")

    async def _scale_down(self):
        """Scale down worker pool."""
        if len(self._workers) <= 1:
            return

        # Find the least efficient worker to remove
        least_efficient_id = None
        lowest_score = float('inf')

        for worker_id in self._workers:
            if worker_id in self.adaptive_pool.worker_metrics:
                score = self.adaptive_pool.worker_metrics[worker_id].efficiency_score
                if score < lowest_score:
                    lowest_score = score
                    least_efficient_id = worker_id

        if least_efficient_id:
            await self._stop_worker(least_efficient_id)
            self.logger.info(f"🔽 Scaled down: Removed worker {least_efficient_id}")

    async def _apply_throttling(self):
        """Apply temporary throttling to reduce load."""
        self.logger.warning("🚨 Applying throttling due to resource pressure")
        await asyncio.sleep(10)  # Brief pause to allow resources to recover

    async def _stop_worker(self, worker_id: str):
        """Stop a specific worker."""
        if worker_id in self._workers:
            worker = self._workers[worker_id]
            await worker.stop()
            del self._workers[worker_id]
            self.logger.info(f"Stopped worker {worker_id}")

    def get_stats(self) -> Dict[str, Any]:
        """Get enhanced worker pool statistics with resource optimization data."""
        worker_stats = []
        for worker in self._workers.values():
            worker_stats.append(worker.get_status())

        # Get resource optimization stats
        optimization_stats = {}
        if self.resource_optimizer:
            optimization_stats = self.resource_optimizer.get_optimization_stats()

        # Get throttling status
        throttling_stats = {
            "twitter": self.throttler.get_service_status("twitter"),
            "browser_navigation": self.throttler.get_service_status("browser_navigation")
        }

        return {
            "worker_count": len(self._workers),
            "max_workers": self.max_workers,
            "workers": worker_stats,
            "resource_optimization": optimization_stats,
            "service_throttling": throttling_stats,
            "scaling_enabled": True,
            "optimization_active": self.resource_optimizer is not None and self.resource_optimizer.running
        }
