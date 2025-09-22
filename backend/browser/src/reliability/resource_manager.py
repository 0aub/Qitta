"""Advanced resource management and concurrency optimization.

Phase 2.3 Implementation:
- Dynamic worker scaling based on load
- Memory and CPU monitoring with adaptive limits
- Intelligent job scheduling and load balancing
- Resource pooling and optimization
- Concurrency throttling for external services
"""

from __future__ import annotations

import asyncio
import logging
import psutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
import weakref

from playwright.async_api import Browser, BrowserContext


class ResourceState(str, Enum):
    """Resource utilization states."""
    OPTIMAL = "optimal"      # 0-60% usage
    HIGH = "high"           # 60-80% usage
    CRITICAL = "critical"   # 80-95% usage
    OVERLOAD = "overload"   # 95%+ usage


class ScalingAction(str, Enum):
    """Scaling actions for dynamic resource management."""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"
    THROTTLE = "throttle"


@dataclass
class ResourceMetrics:
    """Current system resource metrics."""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_available_mb: float = 0.0
    disk_usage_percent: float = 0.0
    active_workers: int = 0
    active_contexts: int = 0
    jobs_per_minute: float = 0.0
    queue_size: int = 0


@dataclass
class ResourceLimits:
    """Configurable resource limits."""
    max_cpu_percent: float = 80.0
    max_memory_percent: float = 75.0
    max_workers: int = 4
    max_contexts_per_worker: int = 3
    max_concurrent_jobs: int = 10
    min_available_memory_mb: float = 512.0


@dataclass
class WorkerLoadMetrics:
    """Per-worker load metrics."""
    worker_id: str
    current_jobs: int = 0
    contexts_active: int = 0
    cpu_usage: float = 0.0
    memory_mb: float = 0.0
    last_job_completion: Optional[datetime] = None
    consecutive_failures: int = 0
    efficiency_score: float = 100.0


class ResourceMonitor:
    """Advanced system resource monitoring."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.metrics_history: List[ResourceMetrics] = []
        self.max_history = 100
        self.limits = ResourceLimits()

    def get_current_metrics(self,
                          active_workers: int = 0,
                          active_contexts: int = 0,
                          queue_size: int = 0) -> ResourceMetrics:
        """Get current system resource metrics."""

        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=0.1)

        # Memory metrics
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_available_mb = memory.available / (1024 * 1024)

        # Disk metrics
        disk = psutil.disk_usage('/')
        disk_usage_percent = disk.percent

        # Calculate jobs per minute from recent history
        jobs_per_minute = self._calculate_throughput()

        metrics = ResourceMetrics(
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_available_mb=memory_available_mb,
            disk_usage_percent=disk_usage_percent,
            active_workers=active_workers,
            active_contexts=active_contexts,
            jobs_per_minute=jobs_per_minute,
            queue_size=queue_size
        )

        # Store in history
        self.metrics_history.append(metrics)
        if len(self.metrics_history) > self.max_history:
            self.metrics_history.pop(0)

        return metrics

    def get_resource_state(self, metrics: ResourceMetrics) -> ResourceState:
        """Determine current resource state."""

        # Check critical resources
        max_usage = max(
            metrics.cpu_percent,
            metrics.memory_percent,
            metrics.disk_usage_percent
        )

        if max_usage >= 95:
            return ResourceState.OVERLOAD
        elif max_usage >= 80:
            return ResourceState.CRITICAL
        elif max_usage >= 60:
            return ResourceState.HIGH
        else:
            return ResourceState.OPTIMAL

    def get_resource_trends(self, window_minutes: int = 5) -> Dict[str, float]:
        """Get resource usage trends over time window."""

        if len(self.metrics_history) < 2:
            return {"cpu_trend": 0.0, "memory_trend": 0.0, "load_trend": 0.0}

        cutoff_time = datetime.utcnow() - timedelta(minutes=window_minutes)
        recent_metrics = [m for m in self.metrics_history if m.timestamp > cutoff_time]

        if len(recent_metrics) < 2:
            return {"cpu_trend": 0.0, "memory_trend": 0.0, "load_trend": 0.0}

        # Calculate trends (positive = increasing usage)
        cpu_trend = self._calculate_trend([m.cpu_percent for m in recent_metrics])
        memory_trend = self._calculate_trend([m.memory_percent for m in recent_metrics])
        load_trend = self._calculate_trend([m.active_workers + m.active_contexts for m in recent_metrics])

        return {
            "cpu_trend": cpu_trend,
            "memory_trend": memory_trend,
            "load_trend": load_trend
        }

    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend direction (-1 to 1)."""
        if len(values) < 2:
            return 0.0

        # Simple linear trend calculation
        n = len(values)
        x_sum = sum(range(n))
        y_sum = sum(values)
        xy_sum = sum(i * values[i] for i in range(n))
        x2_sum = sum(i * i for i in range(n))

        if n * x2_sum - x_sum * x_sum == 0:
            return 0.0

        slope = (n * xy_sum - x_sum * y_sum) / (n * x2_sum - x_sum * x_sum)

        # Normalize to -1 to 1 range
        return max(-1.0, min(1.0, slope / 10.0))

    def _calculate_throughput(self) -> float:
        """Calculate jobs per minute from recent completions."""
        # This would be updated by job completion events
        # For now, return a placeholder
        return 0.0


class AdaptiveWorkerPool:
    """Dynamic worker pool with intelligent scaling."""

    def __init__(self,
                 resource_monitor: ResourceMonitor,
                 logger: logging.Logger):
        self.resource_monitor = resource_monitor
        self.logger = logger
        self.worker_metrics: Dict[str, WorkerLoadMetrics] = {}
        self.scaling_cooldown = timedelta(minutes=2)
        self.last_scaling_action = datetime.utcnow()

    def should_scale(self, current_metrics: ResourceMetrics) -> ScalingAction:
        """Determine if worker pool should scale."""

        # Check cooldown
        if datetime.utcnow() - self.last_scaling_action < self.scaling_cooldown:
            return ScalingAction.MAINTAIN

        resource_state = self.resource_monitor.get_resource_state(current_metrics)
        trends = self.resource_monitor.get_resource_trends()

        # Scale down conditions
        if resource_state == ResourceState.OVERLOAD:
            return ScalingAction.SCALE_DOWN

        if (resource_state == ResourceState.CRITICAL and
            trends["cpu_trend"] > 0.3):  # Rapidly increasing CPU
            return ScalingAction.THROTTLE

        # Scale up conditions
        if (resource_state == ResourceState.OPTIMAL and
            current_metrics.queue_size > current_metrics.active_workers * 3 and
            current_metrics.active_workers < self.resource_monitor.limits.max_workers):
            return ScalingAction.SCALE_UP

        return ScalingAction.MAINTAIN

    def get_optimal_worker_count(self, current_metrics: ResourceMetrics) -> int:
        """Calculate optimal worker count based on current conditions."""

        # Base calculation on queue size and resource availability
        queue_based = min(
            max(1, current_metrics.queue_size // 2),
            self.resource_monitor.limits.max_workers
        )

        # Resource-based calculation
        cpu_capacity = max(1, int((100 - current_metrics.cpu_percent) / 25))
        memory_capacity = max(1, int((100 - current_metrics.memory_percent) / 20))

        resource_based = min(cpu_capacity, memory_capacity)

        # Take the minimum to be conservative
        optimal = min(queue_based, resource_based)

        return max(1, optimal)

    def update_worker_metrics(self, worker_id: str, metrics: Dict[str, Any]):
        """Update metrics for a specific worker."""

        if worker_id not in self.worker_metrics:
            self.worker_metrics[worker_id] = WorkerLoadMetrics(worker_id=worker_id)

        worker_metrics = self.worker_metrics[worker_id]
        worker_metrics.current_jobs = metrics.get('current_jobs', 0)
        worker_metrics.contexts_active = metrics.get('active_contexts', 0)
        worker_metrics.consecutive_failures = metrics.get('consecutive_failures', 0)

        # Calculate efficiency score
        worker_metrics.efficiency_score = self._calculate_efficiency(worker_metrics)

    def _calculate_efficiency(self, metrics: WorkerLoadMetrics) -> float:
        """Calculate worker efficiency score (0-100)."""

        base_score = 100.0

        # Penalize for failures
        if metrics.consecutive_failures > 0:
            base_score -= min(metrics.consecutive_failures * 15, 60)

        # Penalize for too many contexts (resource usage)
        if metrics.contexts_active > 2:
            base_score -= (metrics.contexts_active - 2) * 10

        # Bonus for recent activity
        if metrics.last_job_completion:
            time_since_last = datetime.utcnow() - metrics.last_job_completion
            if time_since_last.total_seconds() < 300:  # Last 5 minutes
                base_score += 10

        return max(0.0, min(100.0, base_score))

    def get_least_loaded_worker(self) -> Optional[str]:
        """Get the worker ID with the lowest current load."""

        if not self.worker_metrics:
            return None

        # Score workers by load (lower is better)
        def load_score(metrics: WorkerLoadMetrics) -> float:
            return (metrics.current_jobs * 50 +
                   metrics.contexts_active * 20 +
                   metrics.consecutive_failures * 30 -
                   metrics.efficiency_score)

        best_worker = min(
            self.worker_metrics.values(),
            key=load_score
        )

        return best_worker.worker_id


class ConcurrencyThrottler:
    """Intelligent concurrency throttling for external services."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.service_limits: Dict[str, Dict[str, Any]] = {}
        self.active_requests: Dict[str, int] = {}
        self.request_times: Dict[str, List[float]] = {}

    def add_service_limit(self,
                         service_name: str,
                         max_concurrent: int = 5,
                         rate_limit_per_minute: int = 60,
                         burst_allowance: int = 10):
        """Add throttling limits for an external service."""

        self.service_limits[service_name] = {
            "max_concurrent": max_concurrent,
            "rate_limit_per_minute": rate_limit_per_minute,
            "burst_allowance": burst_allowance,
            "last_reset": time.time()
        }
        self.active_requests[service_name] = 0
        self.request_times[service_name] = []

    async def acquire_slot(self, service_name: str) -> bool:
        """Acquire a concurrency slot for a service."""

        if service_name not in self.service_limits:
            # No limits configured, allow
            return True

        limits = self.service_limits[service_name]

        # Check concurrent limit
        if self.active_requests[service_name] >= limits["max_concurrent"]:
            return False

        # Check rate limit
        current_time = time.time()
        request_times = self.request_times[service_name]

        # Clean old requests (older than 1 minute)
        request_times[:] = [t for t in request_times if current_time - t < 60]

        if len(request_times) >= limits["rate_limit_per_minute"]:
            return False

        # Acquire slot
        self.active_requests[service_name] += 1
        request_times.append(current_time)

        return True

    def release_slot(self, service_name: str):
        """Release a concurrency slot for a service."""

        if service_name in self.active_requests:
            self.active_requests[service_name] = max(
                0,
                self.active_requests[service_name] - 1
            )

    def get_service_status(self, service_name: str) -> Dict[str, Any]:
        """Get current status for a service."""

        if service_name not in self.service_limits:
            return {"error": "Service not configured"}

        limits = self.service_limits[service_name]
        current_time = time.time()

        # Clean old request times
        request_times = self.request_times[service_name]
        recent_requests = [t for t in request_times if current_time - t < 60]

        return {
            "active_requests": self.active_requests[service_name],
            "max_concurrent": limits["max_concurrent"],
            "requests_last_minute": len(recent_requests),
            "rate_limit": limits["rate_limit_per_minute"],
            "utilization_percent": (
                self.active_requests[service_name] / limits["max_concurrent"] * 100
            )
        }


class ResourceOptimizer:
    """High-level resource optimization coordinator."""

    def __init__(self,
                 resource_monitor: ResourceMonitor,
                 adaptive_pool: AdaptiveWorkerPool,
                 throttler: ConcurrencyThrottler,
                 logger: logging.Logger):
        self.resource_monitor = resource_monitor
        self.adaptive_pool = adaptive_pool
        self.throttler = throttler
        self.logger = logger
        self.optimization_task: Optional[asyncio.Task] = None
        self.running = False

    async def start_optimization(self, interval_seconds: int = 30):
        """Start continuous resource optimization."""

        self.running = True
        self.optimization_task = asyncio.create_task(
            self._optimization_loop(interval_seconds)
        )
        self.logger.info("🚀 Resource optimizer started")

    async def stop_optimization(self):
        """Stop resource optimization."""

        self.running = False
        if self.optimization_task:
            self.optimization_task.cancel()
            try:
                await self.optimization_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Resource optimizer stopped")

    async def _optimization_loop(self, interval_seconds: int):
        """Main optimization loop."""

        while self.running:
            try:
                await self._run_optimization_cycle()
                await asyncio.sleep(interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in optimization cycle: {e}")
                await asyncio.sleep(interval_seconds)

    async def _run_optimization_cycle(self):
        """Run a single optimization cycle."""

        # Get current metrics (this would be updated with real data)
        metrics = self.resource_monitor.get_current_metrics()

        # Analyze scaling needs
        scaling_action = self.adaptive_pool.should_scale(metrics)

        if scaling_action != ScalingAction.MAINTAIN:
            self.logger.info(f"🔧 Resource optimization: {scaling_action.value} recommended")

        # Log resource state for monitoring
        resource_state = self.resource_monitor.get_resource_state(metrics)
        if resource_state in [ResourceState.CRITICAL, ResourceState.OVERLOAD]:
            self.logger.warning(
                f"⚠️ Resource state: {resource_state.value} "
                f"(CPU: {metrics.cpu_percent:.1f}%, Memory: {metrics.memory_percent:.1f}%)"
            )

    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get current optimization statistics."""

        current_metrics = self.resource_monitor.get_current_metrics()
        resource_state = self.resource_monitor.get_resource_state(current_metrics)
        trends = self.resource_monitor.get_resource_trends()

        return {
            "resource_state": resource_state.value,
            "current_metrics": {
                "cpu_percent": current_metrics.cpu_percent,
                "memory_percent": current_metrics.memory_percent,
                "memory_available_mb": current_metrics.memory_available_mb,
                "active_workers": current_metrics.active_workers,
                "active_contexts": current_metrics.active_contexts,
                "queue_size": current_metrics.queue_size
            },
            "trends": trends,
            "optimization_active": self.running,
            "worker_efficiency": {
                worker_id: metrics.efficiency_score
                for worker_id, metrics in self.adaptive_pool.worker_metrics.items()
            }
        }