"""Production observability and metrics enhancements.

Phase 4.4c: Metrics and Observability Enhancements
- Comprehensive Prometheus metrics
- Structured logging with correlation IDs
- Performance monitoring and tracing
- Custom business metrics
- Distributed tracing support
"""

from __future__ import annotations

import time
import uuid
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum
from contextlib import asynccontextmanager
import asyncio

from prometheus_client import Counter, Histogram, Gauge, Info, Enum as PrometheusEnum
from prometheus_client import CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST


class MetricType(str, Enum):
    """Types of metrics to collect."""
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    INFO = "info"


class LogLevel(str, Enum):
    """Structured logging levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class TraceContext:
    """Distributed tracing context."""
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    span_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_span_id: Optional[str] = None
    operation_name: str = ""
    start_time: float = field(default_factory=time.time)
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)

    def add_tag(self, key: str, value: Any) -> None:
        """Add a tag to the trace context."""
        self.tags[key] = value

    def log(self, level: LogLevel, message: str, **kwargs) -> None:
        """Add a log entry to the trace."""
        self.logs.append({
            "timestamp": time.time(),
            "level": level.value,
            "message": message,
            **kwargs
        })

    def finish(self) -> float:
        """Finish the trace and return duration."""
        return time.time() - self.start_time


class ProductionMetrics:
    """Production-grade metrics collector with Prometheus integration."""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self._initialize_metrics()

    def _initialize_metrics(self) -> None:
        """Initialize all production metrics."""

        # HTTP Request Metrics
        self.http_requests_total = Counter(
            'http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status_code'],
            registry=self.registry
        )

        self.http_request_duration = Histogram(
            'http_request_duration_seconds',
            'HTTP request duration in seconds',
            ['method', 'endpoint'],
            registry=self.registry
        )

        # Job Processing Metrics
        self.jobs_total = Counter(
            'jobs_total',
            'Total jobs processed',
            ['task_name', 'status'],
            registry=self.registry
        )

        self.job_duration = Histogram(
            'job_duration_seconds',
            'Job processing duration in seconds',
            ['task_name'],
            buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1200],
            registry=self.registry
        )

        self.job_queue_size = Gauge(
            'job_queue_size',
            'Current number of jobs in queue',
            registry=self.registry
        )

        self.active_jobs = Gauge(
            'active_jobs',
            'Current number of active jobs',
            registry=self.registry
        )

        # Worker Pool Metrics
        self.worker_pool_size = Gauge(
            'worker_pool_size',
            'Current worker pool size',
            registry=self.registry
        )

        self.healthy_workers = Gauge(
            'healthy_workers',
            'Number of healthy workers',
            registry=self.registry
        )

        self.worker_job_count = Counter(
            'worker_job_count',
            'Jobs processed per worker',
            ['worker_id'],
            registry=self.registry
        )

        # Browser Metrics
        self.browser_contexts_created = Counter(
            'browser_contexts_created_total',
            'Total browser contexts created',
            registry=self.registry
        )

        self.browser_contexts_active = Gauge(
            'browser_contexts_active',
            'Currently active browser contexts',
            registry=self.registry
        )

        self.page_load_duration = Histogram(
            'page_load_duration_seconds',
            'Page load duration in seconds',
            ['domain'],
            buckets=[0.5, 1, 2, 5, 10, 20, 30, 60],
            registry=self.registry
        )

        # System Resource Metrics
        self.memory_usage_bytes = Gauge(
            'memory_usage_bytes',
            'Current memory usage in bytes',
            registry=self.registry
        )

        self.cpu_usage_percent = Gauge(
            'cpu_usage_percent',
            'Current CPU usage percentage',
            registry=self.registry
        )

        # Circuit Breaker Metrics
        self.circuit_breaker_state = PrometheusEnum(
            'circuit_breaker_state',
            'Circuit breaker state',
            ['service'],
            states=['closed', 'open', 'half_open'],
            registry=self.registry
        )

        self.circuit_breaker_failures = Counter(
            'circuit_breaker_failures_total',
            'Circuit breaker failures',
            ['service'],
            registry=self.registry
        )

        # Business Metrics
        self.data_extracted_items = Counter(
            'data_extracted_items_total',
            'Total data items extracted',
            ['source', 'type'],
            registry=self.registry
        )

        self.extraction_success_rate = Gauge(
            'extraction_success_rate',
            'Data extraction success rate',
            ['source'],
            registry=self.registry
        )

        # Error Metrics
        self.errors_total = Counter(
            'errors_total',
            'Total errors',
            ['error_type', 'component'],
            registry=self.registry
        )

        self.external_service_errors = Counter(
            'external_service_errors_total',
            'External service errors',
            ['service', 'error_code'],
            registry=self.registry
        )

        # Health Metrics
        self.health_check_duration = Histogram(
            'health_check_duration_seconds',
            'Health check duration',
            ['component'],
            registry=self.registry
        )

        self.component_health_score = Gauge(
            'component_health_score',
            'Component health score (0-1)',
            ['component'],
            registry=self.registry
        )

    def record_http_request(self, method: str, endpoint: str, status_code: int, duration: float) -> None:
        """Record HTTP request metrics."""
        self.http_requests_total.labels(method=method, endpoint=endpoint, status_code=str(status_code)).inc()
        self.http_request_duration.labels(method=method, endpoint=endpoint).observe(duration)

    def record_job_completion(self, task_name: str, status: str, duration: float) -> None:
        """Record job completion metrics."""
        self.jobs_total.labels(task_name=task_name, status=status).inc()
        self.job_duration.labels(task_name=task_name).observe(duration)

    def set_queue_size(self, size: int) -> None:
        """Set current queue size."""
        self.job_queue_size.set(size)

    def set_active_jobs(self, count: int) -> None:
        """Set current active jobs count."""
        self.active_jobs.set(count)

    def set_worker_metrics(self, total_workers: int, healthy_workers: int) -> None:
        """Set worker pool metrics."""
        self.worker_pool_size.set(total_workers)
        self.healthy_workers.set(healthy_workers)

    def record_worker_job(self, worker_id: str) -> None:
        """Record job processed by worker."""
        self.worker_job_count.labels(worker_id=worker_id).inc()

    def record_browser_context_created(self) -> None:
        """Record browser context creation."""
        self.browser_contexts_created.inc()

    def set_active_browser_contexts(self, count: int) -> None:
        """Set active browser contexts count."""
        self.browser_contexts_active.set(count)

    def record_page_load(self, domain: str, duration: float) -> None:
        """Record page load duration."""
        self.page_load_duration.labels(domain=domain).observe(duration)

    def set_resource_usage(self, memory_bytes: int, cpu_percent: float) -> None:
        """Set resource usage metrics."""
        self.memory_usage_bytes.set(memory_bytes)
        self.cpu_usage_percent.set(cpu_percent)

    def set_circuit_breaker_state(self, service: str, state: str) -> None:
        """Set circuit breaker state."""
        self.circuit_breaker_state.labels(service=service).state(state)

    def record_circuit_breaker_failure(self, service: str) -> None:
        """Record circuit breaker failure."""
        self.circuit_breaker_failures.labels(service=service).inc()

    def record_data_extraction(self, source: str, data_type: str, count: int = 1) -> None:
        """Record data extraction."""
        self.data_extracted_items.labels(source=source, type=data_type).inc(count)

    def set_extraction_success_rate(self, source: str, rate: float) -> None:
        """Set extraction success rate."""
        self.extraction_success_rate.labels(source=source).set(rate)

    def record_error(self, error_type: str, component: str) -> None:
        """Record error occurrence."""
        self.errors_total.labels(error_type=error_type, component=component).inc()

    def record_external_service_error(self, service: str, error_code: str) -> None:
        """Record external service error."""
        self.external_service_errors.labels(service=service, error_code=error_code).inc()

    def record_health_check(self, component: str, duration: float, score: float) -> None:
        """Record health check metrics."""
        self.health_check_duration.labels(component=component).observe(duration)
        self.component_health_score.labels(component=component).set(score)

    def get_metrics_text(self) -> str:
        """Get metrics in Prometheus text format."""
        return generate_latest(self.registry).decode('utf-8')


class StructuredLogger:
    """Production structured logger with correlation IDs and tracing."""

    def __init__(self, name: str, level: str = "INFO"):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Set up structured logging formatter
        self._setup_formatter()

    def _setup_formatter(self) -> None:
        """Setup structured JSON formatter."""
        formatter = logging.Formatter(
            json.dumps({
                "timestamp": "%(asctime)s",
                "level": "%(levelname)s",
                "logger": "%(name)s",
                "message": "%(message)s",
                "module": "%(module)s",
                "function": "%(funcName)s",
                "line": "%(lineno)d"
            })
        )

        # Clear existing handlers and add structured handler
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _log_with_context(self, level: str, message: str, trace_context: Optional[TraceContext] = None, **kwargs) -> None:
        """Log with structured context."""
        log_data = {
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "logger": self.name,
            **kwargs
        }

        if trace_context:
            log_data.update({
                "trace_id": trace_context.trace_id,
                "span_id": trace_context.span_id,
                "parent_span_id": trace_context.parent_span_id,
                "operation": trace_context.operation_name
            })

        getattr(self.logger, level.lower())(json.dumps(log_data))

    def debug(self, message: str, trace_context: Optional[TraceContext] = None, **kwargs) -> None:
        """Log debug message."""
        self._log_with_context("DEBUG", message, trace_context, **kwargs)

    def info(self, message: str, trace_context: Optional[TraceContext] = None, **kwargs) -> None:
        """Log info message."""
        self._log_with_context("INFO", message, trace_context, **kwargs)

    def warning(self, message: str, trace_context: Optional[TraceContext] = None, **kwargs) -> None:
        """Log warning message."""
        self._log_with_context("WARNING", message, trace_context, **kwargs)

    def error(self, message: str, trace_context: Optional[TraceContext] = None, **kwargs) -> None:
        """Log error message."""
        self._log_with_context("ERROR", message, trace_context, **kwargs)

    def critical(self, message: str, trace_context: Optional[TraceContext] = None, **kwargs) -> None:
        """Log critical message."""
        self._log_with_context("CRITICAL", message, trace_context, **kwargs)


class PerformanceMonitor:
    """Performance monitoring and tracing."""

    def __init__(self, metrics: ProductionMetrics, logger: StructuredLogger):
        self.metrics = metrics
        self.logger = logger
        self.active_traces: Dict[str, TraceContext] = {}

    @asynccontextmanager
    async def trace_operation(self, operation_name: str, **tags):
        """Async context manager for tracing operations."""
        trace_context = TraceContext(operation_name=operation_name)
        trace_context.tags.update(tags)

        self.active_traces[trace_context.trace_id] = trace_context

        self.logger.info(
            f"Started operation: {operation_name}",
            trace_context=trace_context,
            **tags
        )

        start_time = time.time()
        try:
            yield trace_context
        except Exception as e:
            trace_context.add_tag("error", True)
            trace_context.add_tag("error_message", str(e))
            trace_context.log(LogLevel.ERROR, f"Operation failed: {str(e)}")
            raise
        finally:
            duration = trace_context.finish()

            self.logger.info(
                f"Completed operation: {operation_name}",
                trace_context=trace_context,
                duration=duration,
                **trace_context.tags
            )

            # Clean up
            self.active_traces.pop(trace_context.trace_id, None)

    async def monitor_resource_usage(self) -> None:
        """Monitor system resource usage."""
        try:
            import psutil

            # Get system metrics
            memory = psutil.virtual_memory()
            cpu_percent = psutil.cpu_percent(interval=1)

            # Update metrics
            self.metrics.set_resource_usage(
                memory_bytes=memory.used,
                cpu_percent=cpu_percent
            )

            # Log if usage is high
            if cpu_percent > 80:
                self.logger.warning(
                    "High CPU usage detected",
                    cpu_percent=cpu_percent
                )

            if memory.percent > 80:
                self.logger.warning(
                    "High memory usage detected",
                    memory_percent=memory.percent,
                    memory_used_gb=memory.used / (1024**3)
                )

        except ImportError:
            self.logger.warning("psutil not available for resource monitoring")
        except Exception as e:
            self.logger.error(f"Resource monitoring failed: {e}")


class ObservabilityManager:
    """Central observability management."""

    def __init__(self, service_name: str = "browser-automation", log_level: str = "INFO"):
        self.service_name = service_name
        self.metrics = ProductionMetrics()
        self.logger = StructuredLogger(service_name, log_level)
        self.performance_monitor = PerformanceMonitor(self.metrics, self.logger)

        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start observability monitoring."""
        self.logger.info(f"Starting observability for {self.service_name}")

        # Start background monitoring
        self._monitoring_task = asyncio.create_task(self._background_monitoring())

    async def stop(self) -> None:
        """Stop observability monitoring."""
        self.logger.info(f"Stopping observability for {self.service_name}")

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

    async def _background_monitoring(self) -> None:
        """Background monitoring loop."""
        while True:
            try:
                await self.performance_monitor.monitor_resource_usage()
                await asyncio.sleep(30)  # Monitor every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Background monitoring error: {e}")
                await asyncio.sleep(60)  # Wait longer on error

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get observability metrics summary."""
        return {
            "service_name": self.service_name,
            "active_traces": len(self.performance_monitor.active_traces),
            "metrics_registry_size": len(list(self.metrics.registry._collector_to_names.keys())),
            "logger_name": self.logger.name,
            "timestamp": datetime.utcnow().isoformat()
        }