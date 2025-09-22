"""Advanced monitoring and alerting system for browser automation service.

Phase 2.2 Implementation:
- Comprehensive metrics collection
- Real-time performance monitoring
- Alerting and notification system
- Health scoring and trend analysis
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

from prometheus_client import Counter, Histogram, Gauge, Summary, CollectorRegistry


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class MetricType(str, Enum):
    """Types of metrics we collect."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class Alert:
    """Alert definition and state."""
    name: str
    severity: AlertSeverity
    message: str
    timestamp: datetime
    metric_name: str
    current_value: float
    threshold: float
    resolved: bool = False
    resolution_timestamp: Optional[datetime] = None


@dataclass
class HealthCheck:
    """Health check definition."""
    name: str
    check_function: Callable[[], bool]
    last_check: Optional[datetime] = None
    last_result: bool = True
    failure_count: int = 0
    max_failures: int = 3


class MetricsCollector:
    """Enhanced metrics collector with custom metrics."""

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self._initialize_metrics()

    def _initialize_metrics(self):
        """Initialize all metrics."""

        # Job metrics
        self.job_total = Counter(
            'browser_jobs_total',
            'Total number of jobs processed',
            ['task_name', 'status'],
            registry=self.registry
        )

        self.job_duration = Histogram(
            'browser_job_duration_seconds',
            'Job processing duration',
            ['task_name'],
            registry=self.registry
        )

        self.job_queue_size = Gauge(
            'browser_job_queue_size',
            'Number of jobs in queue',
            ['queue_type'],
            registry=self.registry
        )

        # Worker metrics
        self.worker_active = Gauge(
            'browser_workers_active',
            'Number of active workers',
            registry=self.registry
        )

        self.worker_failures = Counter(
            'browser_worker_failures_total',
            'Worker failure count',
            ['worker_id', 'failure_type'],
            registry=self.registry
        )

        self.worker_contexts = Gauge(
            'browser_worker_contexts',
            'Active browser contexts per worker',
            ['worker_id'],
            registry=self.registry
        )

        # Browser metrics
        self.browser_pages = Gauge(
            'browser_pages_active',
            'Number of active browser pages',
            registry=self.registry
        )

        self.browser_memory_mb = Gauge(
            'browser_memory_mb',
            'Browser memory usage in MB',
            registry=self.registry
        )

        # Error metrics
        self.errors_total = Counter(
            'browser_errors_total',
            'Total errors by category',
            ['category', 'severity'],
            registry=self.registry
        )

        self.circuit_breakers = Gauge(
            'browser_circuit_breakers_open',
            'Number of open circuit breakers',
            ['category'],
            registry=self.registry
        )

        # Performance metrics
        self.response_time = Histogram(
            'browser_response_time_seconds',
            'API response time',
            ['endpoint'],
            registry=self.registry
        )

        self.throughput = Gauge(
            'browser_throughput_jobs_per_minute',
            'Current throughput in jobs per minute',
            registry=self.registry
        )

        # Resource metrics
        self.redis_connections = Gauge(
            'browser_redis_connections',
            'Redis connection count',
            registry=self.registry
        )

        self.system_health_score = Gauge(
            'browser_system_health_score',
            'Overall system health score (0-100)',
            registry=self.registry
        )

    def record_job_completion(self, task_name: str, duration: float, status: str):
        """Record job completion metrics."""
        self.job_total.labels(task_name=task_name, status=status).inc()
        self.job_duration.labels(task_name=task_name).observe(duration)

    def update_queue_size(self, regular_size: int, priority_size: int):
        """Update job queue size metrics."""
        self.job_queue_size.labels(queue_type='regular').set(regular_size)
        self.job_queue_size.labels(queue_type='priority').set(priority_size)

    def update_worker_metrics(self, worker_stats: List[Dict[str, Any]]):
        """Update worker-related metrics."""
        self.worker_active.set(len(worker_stats))

        for worker in worker_stats:
            worker_id = worker['worker_id']
            contexts = worker.get('active_contexts', 0)
            failures = worker.get('consecutive_failures', 0)

            self.worker_contexts.labels(worker_id=worker_id).set(contexts)

            if failures > 0:
                self.worker_failures.labels(
                    worker_id=worker_id,
                    failure_type='consecutive'
                ).inc()

    def record_error(self, category: str, severity: str):
        """Record error occurrence."""
        self.errors_total.labels(category=category, severity=severity).inc()

    def update_circuit_breaker(self, category: str, is_open: bool):
        """Update circuit breaker status."""
        self.circuit_breakers.labels(category=category).set(1 if is_open else 0)

    def update_system_health(self, score: float):
        """Update overall system health score."""
        self.system_health_score.set(score)


class PerformanceMonitor:
    """Real-time performance monitoring."""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.performance_history: Dict[str, List[float]] = {}
        self.window_size = 100  # Keep last 100 measurements

    def record_performance_metric(self, metric_name: str, value: float):
        """Record a performance metric with history."""
        if metric_name not in self.performance_history:
            self.performance_history[metric_name] = []

        history = self.performance_history[metric_name]
        history.append(value)

        # Keep only recent measurements
        if len(history) > self.window_size:
            history.pop(0)

    def get_performance_trend(self, metric_name: str) -> Dict[str, float]:
        """Get performance trend analysis."""
        if metric_name not in self.performance_history:
            return {"trend": 0.0, "average": 0.0, "recent_average": 0.0}

        history = self.performance_history[metric_name]
        if len(history) < 2:
            return {"trend": 0.0, "average": history[0] if history else 0.0, "recent_average": 0.0}

        # Calculate overall average
        avg = sum(history) / len(history)

        # Calculate recent average (last 20% of measurements)
        recent_count = max(1, len(history) // 5)
        recent_avg = sum(history[-recent_count:]) / recent_count

        # Calculate trend (positive = improving, negative = degrading)
        mid_point = len(history) // 2
        first_half_avg = sum(history[:mid_point]) / mid_point
        second_half_avg = sum(history[mid_point:]) / (len(history) - mid_point)
        trend = (second_half_avg - first_half_avg) / first_half_avg if first_half_avg > 0 else 0

        return {
            "trend": trend,
            "average": avg,
            "recent_average": recent_avg
        }

    def calculate_throughput(self, completed_jobs: int, time_window_minutes: int = 5) -> float:
        """Calculate current throughput in jobs per minute."""
        return completed_jobs / time_window_minutes if time_window_minutes > 0 else 0


class AlertManager:
    """Intelligent alerting system."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.alert_rules: List[Dict[str, Any]] = []
        self.notification_callbacks: List[Callable[[Alert], None]] = []

    def add_alert_rule(self,
                      name: str,
                      metric_name: str,
                      threshold: float,
                      severity: AlertSeverity,
                      condition: str = "gt",  # gt, lt, eq
                      cooldown_minutes: int = 5):
        """Add an alert rule."""
        self.alert_rules.append({
            "name": name,
            "metric_name": metric_name,
            "threshold": threshold,
            "severity": severity,
            "condition": condition,
            "cooldown_minutes": cooldown_minutes,
            "last_triggered": None
        })

    def add_notification_callback(self, callback: Callable[[Alert], None]):
        """Add a notification callback function."""
        self.notification_callbacks.append(callback)

    def check_alerts(self, metrics: Dict[str, float]):
        """Check all alert rules against current metrics."""
        current_time = datetime.utcnow()

        for rule in self.alert_rules:
            metric_value = metrics.get(rule["metric_name"])
            if metric_value is None:
                continue

            # Check if alert condition is met
            condition_met = False
            if rule["condition"] == "gt" and metric_value > rule["threshold"]:
                condition_met = True
            elif rule["condition"] == "lt" and metric_value < rule["threshold"]:
                condition_met = True
            elif rule["condition"] == "eq" and abs(metric_value - rule["threshold"]) < 0.001:
                condition_met = True

            alert_key = rule["name"]

            if condition_met:
                # Check cooldown
                if rule["last_triggered"]:
                    time_since_last = current_time - rule["last_triggered"]
                    if time_since_last.total_seconds() < rule["cooldown_minutes"] * 60:
                        continue

                # Trigger alert
                if alert_key not in self.active_alerts:
                    alert = Alert(
                        name=rule["name"],
                        severity=rule["severity"],
                        message=f"{rule['metric_name']} is {metric_value}, threshold: {rule['threshold']}",
                        timestamp=current_time,
                        metric_name=rule["metric_name"],
                        current_value=metric_value,
                        threshold=rule["threshold"]
                    )

                    self.active_alerts[alert_key] = alert
                    self.alert_history.append(alert)
                    rule["last_triggered"] = current_time

                    # Send notifications
                    self._send_notifications(alert)

            else:
                # Resolve alert if it was active
                if alert_key in self.active_alerts:
                    alert = self.active_alerts[alert_key]
                    alert.resolved = True
                    alert.resolution_timestamp = current_time
                    del self.active_alerts[alert_key]

                    self.logger.info(f"Alert resolved: {alert.name}")

    def _send_notifications(self, alert: Alert):
        """Send notifications for an alert."""
        self.logger.warning(f"ALERT: {alert.name} - {alert.message}")

        for callback in self.notification_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Notification callback failed: {e}")

    def get_active_alerts(self) -> List[Alert]:
        """Get list of active alerts."""
        return list(self.active_alerts.values())

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary statistics."""
        total_alerts = len(self.alert_history)
        active_count = len(self.active_alerts)

        # Count by severity
        severity_counts = {}
        for alert in self.active_alerts.values():
            severity_counts[alert.severity.value] = severity_counts.get(alert.severity.value, 0) + 1

        return {
            "total_alerts_all_time": total_alerts,
            "active_alerts": active_count,
            "alerts_by_severity": severity_counts,
            "last_alert": self.alert_history[-1].timestamp.isoformat() if self.alert_history else None
        }


class HealthMonitor:
    """System health monitoring with scoring."""

    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.metrics = metrics_collector
        self.alerts = alert_manager
        self.health_checks: Dict[str, HealthCheck] = {}
        self.last_health_score = 100.0

    def add_health_check(self, name: str, check_function: Callable[[], bool], max_failures: int = 3):
        """Add a health check."""
        self.health_checks[name] = HealthCheck(
            name=name,
            check_function=check_function,
            max_failures=max_failures
        )

    async def run_health_checks(self) -> Dict[str, bool]:
        """Run all health checks."""
        results = {}

        for name, check in self.health_checks.items():
            try:
                result = check.check_function()
                check.last_result = result
                check.last_check = datetime.utcnow()

                if not result:
                    check.failure_count += 1
                else:
                    check.failure_count = 0

                results[name] = result

            except Exception as e:
                self.logger.error(f"Health check {name} failed: {e}")
                check.last_result = False
                check.failure_count += 1
                results[name] = False

        return results

    def calculate_health_score(self,
                             worker_stats: List[Dict[str, Any]],
                             queue_stats: Dict[str, int],
                             error_stats: Dict[str, int]) -> float:
        """Calculate overall system health score (0-100)."""

        score = 100.0

        # Worker health (30% weight)
        worker_score = self._calculate_worker_health(worker_stats)
        score = score * 0.7 + worker_score * 0.3

        # Queue health (20% weight)
        queue_score = self._calculate_queue_health(queue_stats)
        score = score * 0.8 + queue_score * 0.2

        # Error rate health (25% weight)
        error_score = self._calculate_error_health(error_stats)
        score = score * 0.75 + error_score * 0.25

        # Health checks (25% weight)
        health_check_score = self._calculate_health_check_score()
        score = score * 0.75 + health_check_score * 0.25

        self.last_health_score = max(0, min(100, score))
        self.metrics.update_system_health(self.last_health_score)

        return self.last_health_score

    def _calculate_worker_health(self, worker_stats: List[Dict[str, Any]]) -> float:
        """Calculate worker health score."""
        if not worker_stats:
            return 0.0

        total_score = 0.0
        for worker in worker_stats:
            worker_score = 100.0

            # Penalize for failures
            failures = worker.get('consecutive_failures', 0)
            if failures > 0:
                worker_score -= min(failures * 10, 50)

            # Penalize for too many contexts
            contexts = worker.get('active_contexts', 0)
            if contexts > 5:
                worker_score -= (contexts - 5) * 5

            total_score += worker_score

        return total_score / len(worker_stats)

    def _calculate_queue_health(self, queue_stats: Dict[str, int]) -> float:
        """Calculate queue health score."""
        total_queued = queue_stats.get('total_queued', 0)
        running = queue_stats.get('running_jobs_count', 0)

        if total_queued > 50:
            return max(0, 100 - (total_queued - 50) * 2)
        elif total_queued > 20:
            return 100 - (total_queued - 20) * 1
        else:
            return 100.0

    def _calculate_error_health(self, error_stats: Dict[str, int]) -> float:
        """Calculate error health score."""
        total_errors = sum(error_stats.values())
        if total_errors == 0:
            return 100.0
        elif total_errors < 5:
            return 90.0
        elif total_errors < 10:
            return 70.0
        else:
            return max(0, 70 - (total_errors - 10) * 5)

    def _calculate_health_check_score(self) -> float:
        """Calculate health check score."""
        if not self.health_checks:
            return 100.0

        passing_checks = sum(1 for check in self.health_checks.values() if check.last_result)
        return (passing_checks / len(self.health_checks)) * 100