"""Production health checks and readiness probes.

Phase 4.4b: Comprehensive Health Checks and Readiness Probes
- Kubernetes-compatible health endpoints
- Deep health validation of all components
- Graceful degradation indicators
- Performance-based health scoring
- Integration with circuit breakers and fallback systems
"""

from __future__ import annotations

import asyncio
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Optional Redis dependency with graceful fallback
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    # Create placeholder redis module when not available
    class RedisPlaceholder:
        @staticmethod
        def from_url(*args, **kwargs):
            raise ImportError("Redis module not available - install with: pip install redis")
    redis = RedisPlaceholder()
    REDIS_AVAILABLE = False

from playwright.async_api import Browser


class HealthStatus(str, Enum):
    """Health check status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ComponentType(str, Enum):
    """Types of components to monitor."""
    BROWSER_RUNTIME = "browser_runtime"
    REDIS_CONNECTION = "redis_connection"
    WORKER_POOL = "worker_pool"
    CIRCUIT_BREAKERS = "circuit_breakers"
    FALLBACK_SYSTEM = "fallback_system"
    RESOURCE_USAGE = "resource_usage"
    EXTERNAL_DEPENDENCIES = "external_dependencies"


@dataclass
class HealthCheckResult:
    """Result of a health check operation."""
    component: ComponentType
    status: HealthStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    response_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "component": self.component.value,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "response_time_ms": self.response_time_ms,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class SystemHealthSummary:
    """Overall system health summary."""
    overall_status: HealthStatus
    component_results: List[HealthCheckResult]
    health_score: float  # 0.0 to 1.0
    ready_for_traffic: bool
    degraded_components: List[str] = field(default_factory=list)
    failed_components: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "overall_status": self.overall_status.value,
            "health_score": self.health_score,
            "ready_for_traffic": self.ready_for_traffic,
            "degraded_components": self.degraded_components,
            "failed_components": self.failed_components,
            "component_results": [result.to_dict() for result in self.component_results],
            "timestamp": datetime.utcnow().isoformat()
        }


class ProductionHealthChecker:
    """Comprehensive health checker for production deployment."""

    def __init__(self,
                 redis_client: Optional = None,  # Optional Redis client (any type when Redis unavailable)
                 browser_runtime = None,
                 worker_pool = None,
                 circuit_breaker_manager = None,
                 fallback_manager = None,
                 logger: Optional[logging.Logger] = None):
        self.redis_client = redis_client
        self.browser_runtime = browser_runtime
        self.worker_pool = worker_pool
        self.circuit_breaker_manager = circuit_breaker_manager
        self.fallback_manager = fallback_manager
        self.logger = logger or logging.getLogger(__name__)

        # Health check configuration
        self.check_timeout_seconds = 5.0
        self.degraded_threshold_ms = 1000.0  # 1 second
        self.unhealthy_threshold_ms = 5000.0  # 5 seconds

        # Component weights for health score calculation
        self.component_weights = {
            ComponentType.BROWSER_RUNTIME: 0.25,
            ComponentType.REDIS_CONNECTION: 0.20,
            ComponentType.WORKER_POOL: 0.20,
            ComponentType.CIRCUIT_BREAKERS: 0.15,
            ComponentType.FALLBACK_SYSTEM: 0.10,
            ComponentType.RESOURCE_USAGE: 0.10
        }

    async def perform_liveness_check(self) -> HealthCheckResult:
        """Perform basic liveness check - is the service alive?"""
        start_time = time.time()

        try:
            # Basic service liveness - just check if we can respond
            response_time = (time.time() - start_time) * 1000

            if response_time > self.unhealthy_threshold_ms:
                status = HealthStatus.UNHEALTHY
                message = f"Service responding slowly ({response_time:.1f}ms)"
            elif response_time > self.degraded_threshold_ms:
                status = HealthStatus.DEGRADED
                message = f"Service response time elevated ({response_time:.1f}ms)"
            else:
                status = HealthStatus.HEALTHY
                message = "Service is alive and responding"

            return HealthCheckResult(
                component=ComponentType.EXTERNAL_DEPENDENCIES,
                status=status,
                message=message,
                response_time_ms=response_time,
                details={"check_type": "liveness"}
            )

        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.EXTERNAL_DEPENDENCIES,
                status=HealthStatus.UNHEALTHY,
                message=f"Liveness check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e), "check_type": "liveness"}
            )

    async def perform_readiness_check(self) -> SystemHealthSummary:
        """Perform comprehensive readiness check - is the service ready for traffic?"""
        self.logger.info("Starting comprehensive readiness check")

        # Perform all component health checks
        results = await self._check_all_components()

        # Calculate overall health
        overall_status, health_score = self._calculate_overall_health(results)

        # Determine if ready for traffic
        ready_for_traffic = self._determine_traffic_readiness(results, overall_status)

        # Categorize component issues
        degraded_components = [r.component.value for r in results if r.status == HealthStatus.DEGRADED]
        failed_components = [r.component.value for r in results if r.status == HealthStatus.UNHEALTHY]

        summary = SystemHealthSummary(
            overall_status=overall_status,
            component_results=results,
            health_score=health_score,
            ready_for_traffic=ready_for_traffic,
            degraded_components=degraded_components,
            failed_components=failed_components
        )

        self.logger.info(f"Readiness check complete: {overall_status.value}, score: {health_score:.2f}")
        return summary

    async def _check_all_components(self) -> List[HealthCheckResult]:
        """Check health of all system components."""
        check_tasks = [
            self._check_redis_connection(),
            self._check_browser_runtime(),
            self._check_worker_pool(),
            self._check_circuit_breakers(),
            self._check_fallback_system(),
            self._check_resource_usage()
        ]

        # Run all checks concurrently with timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*check_tasks, return_exceptions=True),
                timeout=self.check_timeout_seconds
            )
        except asyncio.TimeoutError:
            self.logger.warning("Health checks timed out")
            return [HealthCheckResult(
                component=ComponentType.EXTERNAL_DEPENDENCIES,
                status=HealthStatus.UNHEALTHY,
                message="Health checks timed out"
            )]

        # Process results and handle exceptions
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append(HealthCheckResult(
                    component=ComponentType.EXTERNAL_DEPENDENCIES,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Health check exception: {str(result)}"
                ))
            else:
                processed_results.append(result)

        return processed_results

    async def _check_redis_connection(self) -> HealthCheckResult:
        """Check Redis connection health."""
        start_time = time.time()

        if not self.redis_client:
            return HealthCheckResult(
                component=ComponentType.REDIS_CONNECTION,
                status=HealthStatus.UNKNOWN,
                message="Redis client not configured",
                response_time_ms=(time.time() - start_time) * 1000
            )

        try:
            # Test Redis connectivity with ping
            await asyncio.wait_for(
                asyncio.to_thread(self.redis_client.ping),
                timeout=2.0
            )

            # Test read/write operations
            test_key = "health_check_test"
            test_value = str(int(time.time()))

            await asyncio.to_thread(self.redis_client.set, test_key, test_value, ex=60)
            stored_value = await asyncio.to_thread(self.redis_client.get, test_key)

            response_time = (time.time() - start_time) * 1000

            if stored_value and stored_value.decode() == test_value:
                # Get connection pool info
                pool_info = {}
                if hasattr(self.redis_client, 'connection_pool'):
                    pool = self.redis_client.connection_pool
                    pool_info = {
                        "created_connections": getattr(pool, '_created_connections', 0),
                        "available_connections": len(getattr(pool, '_available_connections', [])),
                        "in_use_connections": len(getattr(pool, '_in_use_connections', []))
                    }

                status = HealthStatus.DEGRADED if response_time > self.degraded_threshold_ms else HealthStatus.HEALTHY
                message = f"Redis connection healthy (response: {response_time:.1f}ms)"

                return HealthCheckResult(
                    component=ComponentType.REDIS_CONNECTION,
                    status=status,
                    message=message,
                    response_time_ms=response_time,
                    details={
                        "ping_successful": True,
                        "read_write_test": True,
                        "connection_pool": pool_info
                    }
                )
            else:
                return HealthCheckResult(
                    component=ComponentType.REDIS_CONNECTION,
                    status=HealthStatus.UNHEALTHY,
                    message="Redis read/write test failed",
                    response_time_ms=response_time,
                    details={"ping_successful": True, "read_write_test": False}
                )

        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.REDIS_CONNECTION,
                status=HealthStatus.UNHEALTHY,
                message=f"Redis connection failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def _check_browser_runtime(self) -> HealthCheckResult:
        """Check browser runtime health."""
        start_time = time.time()

        if not self.browser_runtime:
            return HealthCheckResult(
                component=ComponentType.BROWSER_RUNTIME,
                status=HealthStatus.UNKNOWN,
                message="Browser runtime not configured",
                response_time_ms=(time.time() - start_time) * 1000
            )

        try:
            # Check if browser is available
            browser = getattr(self.browser_runtime, 'browser', None)
            if not browser:
                return HealthCheckResult(
                    component=ComponentType.BROWSER_RUNTIME,
                    status=HealthStatus.UNHEALTHY,
                    message="Browser not initialized",
                    response_time_ms=(time.time() - start_time) * 1000
                )

            # Test browser connectivity by getting version
            version = await browser.version()
            response_time = (time.time() - start_time) * 1000

            # Check if we can create a simple context
            context = await browser.new_context()
            await context.close()

            total_response_time = (time.time() - start_time) * 1000

            status = HealthStatus.DEGRADED if total_response_time > self.degraded_threshold_ms else HealthStatus.HEALTHY
            message = f"Browser runtime healthy (response: {total_response_time:.1f}ms)"

            return HealthCheckResult(
                component=ComponentType.BROWSER_RUNTIME,
                status=status,
                message=message,
                response_time_ms=total_response_time,
                details={
                    "browser_version": version,
                    "context_creation_test": True
                }
            )

        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.BROWSER_RUNTIME,
                status=HealthStatus.UNHEALTHY,
                message=f"Browser runtime check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def _check_worker_pool(self) -> HealthCheckResult:
        """Check worker pool health."""
        start_time = time.time()

        if not self.worker_pool:
            return HealthCheckResult(
                component=ComponentType.WORKER_POOL,
                status=HealthStatus.UNKNOWN,
                message="Worker pool not configured",
                response_time_ms=(time.time() - start_time) * 1000
            )

        try:
            # Get worker pool status
            workers = getattr(self.worker_pool, 'workers', [])
            active_workers = len([w for w in workers if getattr(w, 'is_healthy', lambda: False)()])
            total_workers = len(workers)

            response_time = (time.time() - start_time) * 1000

            if total_workers == 0:
                status = HealthStatus.UNHEALTHY
                message = "No workers available"
            elif active_workers == 0:
                status = HealthStatus.UNHEALTHY
                message = f"No healthy workers (0/{total_workers})"
            elif active_workers < total_workers * 0.5:
                status = HealthStatus.DEGRADED
                message = f"Low worker availability ({active_workers}/{total_workers})"
            else:
                status = HealthStatus.HEALTHY
                message = f"Worker pool healthy ({active_workers}/{total_workers})"

            return HealthCheckResult(
                component=ComponentType.WORKER_POOL,
                status=status,
                message=message,
                response_time_ms=response_time,
                details={
                    "total_workers": total_workers,
                    "active_workers": active_workers,
                    "worker_utilization": active_workers / max(total_workers, 1)
                }
            )

        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.WORKER_POOL,
                status=HealthStatus.UNHEALTHY,
                message=f"Worker pool check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def _check_circuit_breakers(self) -> HealthCheckResult:
        """Check circuit breaker system health."""
        start_time = time.time()

        if not self.circuit_breaker_manager:
            return HealthCheckResult(
                component=ComponentType.CIRCUIT_BREAKERS,
                status=HealthStatus.UNKNOWN,
                message="Circuit breaker manager not configured",
                response_time_ms=(time.time() - start_time) * 1000
            )

        try:
            # Get circuit breaker states
            breakers = getattr(self.circuit_breaker_manager, 'circuit_breakers', {})
            breaker_states = {}
            open_breakers = 0

            for name, breaker in breakers.items():
                state = getattr(breaker, 'state', 'unknown')
                breaker_states[name] = state
                if state == 'open':
                    open_breakers += 1

            response_time = (time.time() - start_time) * 1000

            total_breakers = len(breakers)
            if total_breakers == 0:
                status = HealthStatus.HEALTHY
                message = "No circuit breakers configured"
            elif open_breakers == 0:
                status = HealthStatus.HEALTHY
                message = f"All circuit breakers closed ({total_breakers})"
            elif open_breakers < total_breakers * 0.5:
                status = HealthStatus.DEGRADED
                message = f"Some circuit breakers open ({open_breakers}/{total_breakers})"
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Many circuit breakers open ({open_breakers}/{total_breakers})"

            return HealthCheckResult(
                component=ComponentType.CIRCUIT_BREAKERS,
                status=status,
                message=message,
                response_time_ms=response_time,
                details={
                    "total_breakers": total_breakers,
                    "open_breakers": open_breakers,
                    "breaker_states": breaker_states
                }
            )

        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.CIRCUIT_BREAKERS,
                status=HealthStatus.UNHEALTHY,
                message=f"Circuit breaker check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def _check_fallback_system(self) -> HealthCheckResult:
        """Check fallback system health."""
        start_time = time.time()

        if not self.fallback_manager:
            return HealthCheckResult(
                component=ComponentType.FALLBACK_SYSTEM,
                status=HealthStatus.UNKNOWN,
                message="Fallback manager not configured",
                response_time_ms=(time.time() - start_time) * 1000
            )

        try:
            # Get fallback system status
            health_data = getattr(self.fallback_manager, 'get_health_status', lambda: {})()
            response_time = (time.time() - start_time) * 1000

            service_level = health_data.get('service_level', 'unknown')
            total_services = health_data.get('total_services', 0)
            degraded_services = health_data.get('degraded_services', 0)

            if service_level == 'full':
                status = HealthStatus.HEALTHY
                message = "Fallback system healthy - full service level"
            elif service_level == 'degraded':
                status = HealthStatus.DEGRADED
                message = f"Fallback system degraded ({degraded_services}/{total_services} services)"
            else:
                status = HealthStatus.UNHEALTHY
                message = f"Fallback system unhealthy - {service_level} service level"

            return HealthCheckResult(
                component=ComponentType.FALLBACK_SYSTEM,
                status=status,
                message=message,
                response_time_ms=response_time,
                details={
                    "service_level": service_level,
                    "total_services": total_services,
                    "degraded_services": degraded_services,
                    "health_data": health_data
                }
            )

        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.FALLBACK_SYSTEM,
                status=HealthStatus.UNHEALTHY,
                message=f"Fallback system check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def _check_resource_usage(self) -> HealthCheckResult:
        """Check system resource usage."""
        start_time = time.time()

        try:
            import psutil

            # Get system metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            response_time = (time.time() - start_time) * 1000

            # Determine status based on resource usage
            critical_thresholds = {'cpu': 90, 'memory': 90, 'disk': 95}
            warning_thresholds = {'cpu': 80, 'memory': 80, 'disk': 85}

            issues = []
            if cpu_percent > critical_thresholds['cpu']:
                issues.append(f"CPU usage critical ({cpu_percent:.1f}%)")
            elif cpu_percent > warning_thresholds['cpu']:
                issues.append(f"CPU usage high ({cpu_percent:.1f}%)")

            if memory.percent > critical_thresholds['memory']:
                issues.append(f"Memory usage critical ({memory.percent:.1f}%)")
            elif memory.percent > warning_thresholds['memory']:
                issues.append(f"Memory usage high ({memory.percent:.1f}%)")

            if disk.percent > critical_thresholds['disk']:
                issues.append(f"Disk usage critical ({disk.percent:.1f}%)")
            elif disk.percent > warning_thresholds['disk']:
                issues.append(f"Disk usage high ({disk.percent:.1f}%)")

            if any('critical' in issue for issue in issues):
                status = HealthStatus.UNHEALTHY
                message = f"Critical resource usage: {'; '.join(issues)}"
            elif issues:
                status = HealthStatus.DEGRADED
                message = f"High resource usage: {'; '.join(issues)}"
            else:
                status = HealthStatus.HEALTHY
                message = "Resource usage normal"

            return HealthCheckResult(
                component=ComponentType.RESOURCE_USAGE,
                status=status,
                message=message,
                response_time_ms=response_time,
                details={
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "disk_percent": disk.percent,
                    "disk_free_gb": disk.free / (1024**3)
                }
            )

        except ImportError:
            return HealthCheckResult(
                component=ComponentType.RESOURCE_USAGE,
                status=HealthStatus.UNKNOWN,
                message="psutil not available for resource monitoring",
                response_time_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            return HealthCheckResult(
                component=ComponentType.RESOURCE_USAGE,
                status=HealthStatus.UNHEALTHY,
                message=f"Resource check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    def _calculate_overall_health(self, results: List[HealthCheckResult]) -> Tuple[HealthStatus, float]:
        """Calculate overall system health status and score."""
        if not results:
            return HealthStatus.UNKNOWN, 0.0

        # Calculate weighted health score
        total_weight = 0.0
        weighted_score = 0.0

        for result in results:
            weight = self.component_weights.get(result.component, 0.05)  # Default small weight
            total_weight += weight

            # Convert status to score
            if result.status == HealthStatus.HEALTHY:
                score = 1.0
            elif result.status == HealthStatus.DEGRADED:
                score = 0.5
            elif result.status == HealthStatus.UNHEALTHY:
                score = 0.0
            else:  # UNKNOWN
                score = 0.25

            weighted_score += score * weight

        # Normalize score
        overall_score = weighted_score / max(total_weight, 1.0)

        # Determine overall status
        if overall_score >= 0.8:
            overall_status = HealthStatus.HEALTHY
        elif overall_score >= 0.5:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.UNHEALTHY

        return overall_status, overall_score

    def _determine_traffic_readiness(self, results: List[HealthCheckResult], overall_status: HealthStatus) -> bool:
        """Determine if the service is ready to receive traffic."""
        # Critical components that must be healthy for traffic readiness
        critical_components = {ComponentType.BROWSER_RUNTIME, ComponentType.REDIS_CONNECTION}

        critical_unhealthy = any(
            result.status == HealthStatus.UNHEALTHY
            for result in results
            if result.component in critical_components
        )

        # Service is ready if:
        # 1. No critical components are unhealthy
        # 2. Overall status is not unhealthy
        return not critical_unhealthy and overall_status != HealthStatus.UNHEALTHY