"""Graceful degradation and fallback mechanisms.

Phase 2.6 Implementation:
- Fallback strategies for degraded services
- Service health-based operation modes
- Reduced functionality operations
- Emergency modes and recovery
- Fallback response generation
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union
from enum import Enum
import logging

from .circuit_breaker import CircuitState, CircuitBreakerManager


class ServiceLevel(str, Enum):
    """Service operation levels."""
    FULL = "full"                    # All features available
    DEGRADED = "degraded"           # Reduced functionality
    MINIMAL = "minimal"             # Core features only
    EMERGENCY = "emergency"         # Emergency fallback only


class FallbackStrategy(str, Enum):
    """Types of fallback strategies."""
    CACHED_RESPONSE = "cached_response"       # Use cached data
    REDUCED_QUALITY = "reduced_quality"       # Lower quality output
    ALTERNATIVE_SERVICE = "alternative_service" # Use backup service
    MOCK_RESPONSE = "mock_response"           # Generated response
    FAIL_FAST = "fail_fast"                   # Quick failure with message
    RETRY_WITH_DELAY = "retry_with_delay"     # Delayed retry attempt


@dataclass
class FallbackConfig:
    """Configuration for fallback behavior."""
    strategy: FallbackStrategy
    timeout_seconds: float = 10.0
    cache_ttl_seconds: float = 300.0  # 5 minutes
    retry_delay_seconds: float = 5.0
    max_fallback_attempts: int = 3
    quality_reduction_factor: float = 0.5
    mock_response_template: Optional[Dict[str, Any]] = None


@dataclass
class ServiceHealth:
    """Health status of a service."""
    service_name: str
    is_healthy: bool
    response_time_ms: float
    error_rate_percent: float
    last_check: datetime
    consecutive_failures: int = 0
    availability_percent: float = 100.0


@dataclass
class FallbackExecution:
    """Result of a fallback execution."""
    strategy_used: FallbackStrategy
    success: bool
    response_time_ms: float
    data: Optional[Dict[str, Any]]
    error: Optional[str]
    is_degraded: bool
    metadata: Dict[str, Any] = field(default_factory=dict)


class FallbackManager:
    """Manager for graceful degradation and fallback mechanisms."""

    def __init__(self,
                 circuit_breaker_manager: Optional[CircuitBreakerManager] = None,
                 logger: Optional[logging.Logger] = None):
        self.circuit_breaker_manager = circuit_breaker_manager
        self.logger = logger or logging.getLogger(__name__)

        # Service health tracking
        self.service_health: Dict[str, ServiceHealth] = {}
        self.service_level = ServiceLevel.FULL

        # Fallback configurations
        self.fallback_configs: Dict[str, FallbackConfig] = {}
        self.cache: Dict[str, Dict[str, Any]] = {}

        # Metrics
        self.fallback_metrics: Dict[str, List[FallbackExecution]] = {}
        self.degradation_events: List[Dict[str, Any]] = []

        # Health monitoring
        self.health_check_interval = 30.0  # seconds
        self.health_check_task: Optional[asyncio.Task] = None
        self.monitoring_active = False

    async def start_monitoring(self) -> None:
        """Start health monitoring and degradation management."""
        if self.monitoring_active:
            return

        self.monitoring_active = True
        self.health_check_task = asyncio.create_task(self._health_monitoring_loop())
        self.logger.info("Fallback manager health monitoring started")

    async def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        self.monitoring_active = False
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Fallback manager health monitoring stopped")

    def register_fallback(self, service_name: str, config: FallbackConfig) -> None:
        """Register a fallback configuration for a service."""
        self.fallback_configs[service_name] = config
        self.logger.info(f"Registered fallback strategy '{config.strategy}' for service '{service_name}'")

    async def execute_with_fallback(self,
                                  service_name: str,
                                  primary_func: Callable,
                                  *args,
                                  fallback_data: Optional[Dict[str, Any]] = None,
                                  **kwargs) -> FallbackExecution:
        """Execute a function with fallback mechanisms."""
        start_time = time.time()

        # Check if service should use fallback immediately
        if await self._should_use_fallback(service_name):
            return await self._execute_fallback(service_name, fallback_data, start_time)

        # Try primary function first through circuit breaker
        try:
            # Phase 4.2b: Execute primary function through circuit breaker for proper metrics
            if self.circuit_breaker_manager:
                result = await self.circuit_breaker_manager.call_with_breaker(
                    service_name, primary_func, *args, **kwargs
                )
            else:
                result = await primary_func(*args, **kwargs)

            response_time = (time.time() - start_time) * 1000

            # Update service health on success
            await self._update_service_health(service_name, True, response_time)

            return FallbackExecution(
                strategy_used=FallbackStrategy.CACHED_RESPONSE,  # Not really a fallback
                success=True,
                response_time_ms=response_time,
                data=result,
                error=None,
                is_degraded=False,
                metadata={"primary_execution": True}
            )

        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            # Phase 4.2d: Pass error details for enhanced external service detection
            await self._update_service_health(service_name, False, response_time, str(e))

            self.logger.warning(f"Primary function failed for service '{service_name}': {e}")
            return await self._execute_fallback(service_name, fallback_data, start_time, str(e))

    async def _should_use_fallback(self, service_name: str) -> bool:
        """Determine if a service should use fallback immediately."""
        # Check circuit breaker state
        if self.circuit_breaker_manager:
            try:
                breaker = self.circuit_breaker_manager.breakers.get(service_name)
                if breaker and breaker.state == CircuitState.OPEN:
                    return True
            except Exception:
                pass

        # Check service health
        health = self.service_health.get(service_name)
        if health:
            # Phase 4.2d: Enhanced external service detection triggers
            # Check for recent external service errors
            if hasattr(health, '_external_service_errors') and health._external_service_errors:
                recent_external_errors = [
                    err for err in health._external_service_errors
                    if (datetime.utcnow() - datetime.fromisoformat(err['timestamp'])).total_seconds() < 300  # 5 minutes
                ]
                # Use fallback immediately if multiple external service errors in last 5 minutes
                if len(recent_external_errors) >= 2:
                    self.logger.info(f"Triggering fallback for '{service_name}' due to {len(recent_external_errors)} recent external service errors")
                    return True

            # Use fallback if error rate is too high
            if health.error_rate_percent > 50.0:
                return True
            # Use fallback if too many consecutive failures (lowered threshold for external services)
            if health.consecutive_failures >= 2:  # Reduced from 3 to 2 for faster fallback
                return True
            # Use fallback if response time is too slow
            if health.response_time_ms > 30000:  # 30 seconds
                return True

        # Check current service level
        if self.service_level == ServiceLevel.EMERGENCY:
            return True
        elif self.service_level == ServiceLevel.MINIMAL and service_name not in ['core', 'essential']:
            return True

        return False

    async def _execute_fallback(self,
                              service_name: str,
                              fallback_data: Optional[Dict[str, Any]],
                              start_time: float,
                              primary_error: Optional[str] = None) -> FallbackExecution:
        """Execute the appropriate fallback strategy."""
        config = self.fallback_configs.get(service_name)
        if not config:
            # Default fallback - fail fast
            return FallbackExecution(
                strategy_used=FallbackStrategy.FAIL_FAST,
                success=False,
                response_time_ms=(time.time() - start_time) * 1000,
                data=None,
                error=f"No fallback configured for service '{service_name}'",
                is_degraded=True,
                metadata={"primary_error": primary_error}
            )

        try:
            if config.strategy == FallbackStrategy.CACHED_RESPONSE:
                return await self._fallback_cached_response(service_name, config, start_time, primary_error)

            elif config.strategy == FallbackStrategy.REDUCED_QUALITY:
                return await self._fallback_reduced_quality(service_name, config, fallback_data, start_time, primary_error)

            elif config.strategy == FallbackStrategy.MOCK_RESPONSE:
                return await self._fallback_mock_response(service_name, config, start_time, primary_error)

            elif config.strategy == FallbackStrategy.FAIL_FAST:
                return await self._fallback_fail_fast(service_name, config, start_time, primary_error)

            elif config.strategy == FallbackStrategy.RETRY_WITH_DELAY:
                return await self._fallback_retry_with_delay(service_name, config, fallback_data, start_time, primary_error)

            else:
                return FallbackExecution(
                    strategy_used=config.strategy,
                    success=False,
                    response_time_ms=(time.time() - start_time) * 1000,
                    data=None,
                    error=f"Unknown fallback strategy: {config.strategy}",
                    is_degraded=True,
                    metadata={"primary_error": primary_error}
                )

        except Exception as e:
            self.logger.error(f"Fallback execution failed for service '{service_name}': {e}")
            return FallbackExecution(
                strategy_used=config.strategy,
                success=False,
                response_time_ms=(time.time() - start_time) * 1000,
                data=None,
                error=f"Fallback execution error: {e}",
                is_degraded=True,
                metadata={"primary_error": primary_error, "fallback_error": str(e)}
            )

    async def _fallback_cached_response(self,
                                      service_name: str,
                                      config: FallbackConfig,
                                      start_time: float,
                                      primary_error: Optional[str]) -> FallbackExecution:
        """Return cached response if available."""
        cached_data = self.cache.get(service_name)
        response_time = (time.time() - start_time) * 1000

        if cached_data:
            # Check cache age
            cache_age = time.time() - cached_data.get('timestamp', 0)
            if cache_age <= config.cache_ttl_seconds:
                return FallbackExecution(
                    strategy_used=FallbackStrategy.CACHED_RESPONSE,
                    success=True,
                    response_time_ms=response_time,
                    data=cached_data.get('data'),
                    error=None,
                    is_degraded=True,
                    metadata={
                        "cache_age_seconds": cache_age,
                        "primary_error": primary_error
                    }
                )

        # No valid cache available
        return FallbackExecution(
            strategy_used=FallbackStrategy.CACHED_RESPONSE,
            success=False,
            response_time_ms=response_time,
            data=None,
            error="No valid cached response available",
            is_degraded=True,
            metadata={"primary_error": primary_error}
        )

    async def _fallback_reduced_quality(self,
                                      service_name: str,
                                      config: FallbackConfig,
                                      fallback_data: Optional[Dict[str, Any]],
                                      start_time: float,
                                      primary_error: Optional[str]) -> FallbackExecution:
        """Return reduced quality response."""
        if not fallback_data:
            return FallbackExecution(
                strategy_used=FallbackStrategy.REDUCED_QUALITY,
                success=False,
                response_time_ms=(time.time() - start_time) * 1000,
                data=None,
                error="No fallback data provided for reduced quality response",
                is_degraded=True,
                metadata={"primary_error": primary_error}
            )

        # Simulate reduced quality processing
        await asyncio.sleep(0.1)  # Quick processing

        # Reduce data quality/quantity
        reduced_data = self._reduce_data_quality(fallback_data, config.quality_reduction_factor)

        return FallbackExecution(
            strategy_used=FallbackStrategy.REDUCED_QUALITY,
            success=True,
            response_time_ms=(time.time() - start_time) * 1000,
            data=reduced_data,
            error=None,
            is_degraded=True,
            metadata={
                "quality_factor": config.quality_reduction_factor,
                "primary_error": primary_error
            }
        )

    async def _fallback_mock_response(self,
                                    service_name: str,
                                    config: FallbackConfig,
                                    start_time: float,
                                    primary_error: Optional[str]) -> FallbackExecution:
        """Return mock response."""
        mock_data = config.mock_response_template or {
            "status": "fallback",
            "message": f"Service {service_name} is temporarily unavailable",
            "data": [],
            "degraded": True
        }

        return FallbackExecution(
            strategy_used=FallbackStrategy.MOCK_RESPONSE,
            success=True,
            response_time_ms=(time.time() - start_time) * 1000,
            data=mock_data,
            error=None,
            is_degraded=True,
            metadata={"primary_error": primary_error}
        )

    async def _fallback_fail_fast(self,
                                service_name: str,
                                config: FallbackConfig,
                                start_time: float,
                                primary_error: Optional[str]) -> FallbackExecution:
        """Fail fast with informative error."""
        error_message = f"Service '{service_name}' is currently unavailable"
        if primary_error:
            error_message += f" (Reason: {primary_error})"

        return FallbackExecution(
            strategy_used=FallbackStrategy.FAIL_FAST,
            success=False,
            response_time_ms=(time.time() - start_time) * 1000,
            data=None,
            error=error_message,
            is_degraded=True,
            metadata={"primary_error": primary_error}
        )

    async def _fallback_retry_with_delay(self,
                                       service_name: str,
                                       config: FallbackConfig,
                                       fallback_data: Optional[Dict[str, Any]],
                                       start_time: float,
                                       primary_error: Optional[str]) -> FallbackExecution:
        """Retry with delay as fallback."""
        await asyncio.sleep(config.retry_delay_seconds)

        # For now, return a delayed failure
        # In practice, this might retry the original function
        return FallbackExecution(
            strategy_used=FallbackStrategy.RETRY_WITH_DELAY,
            success=False,
            response_time_ms=(time.time() - start_time) * 1000,
            data=None,
            error=f"Delayed retry not implemented for service '{service_name}'",
            is_degraded=True,
            metadata={
                "delay_seconds": config.retry_delay_seconds,
                "primary_error": primary_error
            }
        )

    def _reduce_data_quality(self, data: Dict[str, Any], factor: float) -> Dict[str, Any]:
        """Reduce data quality/quantity by a factor."""
        if not isinstance(data, dict):
            return data

        reduced = {}
        for key, value in data.items():
            if isinstance(value, list):
                # Reduce list size
                new_size = max(1, int(len(value) * factor))
                reduced[key] = value[:new_size]
            elif isinstance(value, dict):
                # Recursively reduce nested objects
                reduced[key] = self._reduce_data_quality(value, factor)
            else:
                # Keep simple values
                reduced[key] = value

        # Add degradation notice
        reduced['_degraded'] = True
        reduced['_quality_factor'] = factor

        return reduced

    async def _update_service_health(self,
                                   service_name: str,
                                   success: bool,
                                   response_time_ms: float,
                                   error_details: Optional[str] = None) -> None:
        """Update health status for a service with enhanced external service detection."""
        now = datetime.utcnow()

        health = self.service_health.get(service_name)
        if not health:
            health = ServiceHealth(
                service_name=service_name,
                is_healthy=success,
                response_time_ms=response_time_ms,
                error_rate_percent=0.0 if success else 100.0,
                last_check=now,
                consecutive_failures=0 if success else 1
            )
            self.service_health[service_name] = health
        else:
            # Update health metrics
            health.last_check = now
            health.response_time_ms = (health.response_time_ms * 0.7) + (response_time_ms * 0.3)  # EMA

            if success:
                health.consecutive_failures = 0
                health.is_healthy = True
            else:
                health.consecutive_failures += 1
                if health.consecutive_failures >= 3:
                    health.is_healthy = False

            # Update error rate (simple moving average)
            if not hasattr(health, '_recent_requests'):
                health._recent_requests = []

            health._recent_requests.append(success)
            if len(health._recent_requests) > 20:  # Keep last 20 requests
                health._recent_requests.pop(0)

            failures = sum(1 for req in health._recent_requests if not req)
            health.error_rate_percent = (failures / len(health._recent_requests)) * 100
            health.availability_percent = ((len(health._recent_requests) - failures) / len(health._recent_requests)) * 100

        # Phase 4.2d: Enhanced external service detection
        if not success and error_details:
            external_service_indicators = [
                "EXTERNAL_SERVICE",
                "403 Forbidden",
                "CloudFlare",
                "Access denied",
                "Rate limited",
                "IP blocked",
                "Service unavailable",
                "Connection refused",
                "Timeout",
                "Network unreachable"
            ]

            # Check if this is likely an external service issue
            is_external_service_error = any(
                indicator.lower() in error_details.lower()
                for indicator in external_service_indicators
            )

            if is_external_service_error:
                self.logger.warning(
                    f"External service degradation detected for '{service_name}': {error_details}"
                )
                # Mark service as critically degraded for external service issues
                health.is_healthy = False
                # Add external service specific metadata
                if not hasattr(health, '_external_service_errors'):
                    health._external_service_errors = []
                health._external_service_errors.append({
                    'timestamp': now.isoformat(),
                    'error': error_details,
                    'consecutive_failures': health.consecutive_failures
                })
                # Keep only recent external service errors (last 10)
                if len(health._external_service_errors) > 10:
                    health._external_service_errors.pop(0)

    async def _health_monitoring_loop(self) -> None:
        """Continuous health monitoring and service level adjustment."""
        while self.monitoring_active:
            try:
                await self._assess_overall_health()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Health monitoring error: {e}")
                await asyncio.sleep(5)  # Short retry interval

    async def _assess_overall_health(self) -> None:
        """Assess overall system health and adjust service level."""
        if not self.service_health:
            return

        healthy_services = sum(1 for h in self.service_health.values() if h.is_healthy)
        total_services = len(self.service_health)
        avg_availability = sum(h.availability_percent for h in self.service_health.values()) / total_services

        old_level = self.service_level

        # Determine new service level
        if avg_availability >= 90 and healthy_services >= total_services * 0.8:
            new_level = ServiceLevel.FULL
        elif avg_availability >= 70 and healthy_services >= total_services * 0.6:
            new_level = ServiceLevel.DEGRADED
        elif avg_availability >= 50 and healthy_services >= total_services * 0.4:
            new_level = ServiceLevel.MINIMAL
        else:
            new_level = ServiceLevel.EMERGENCY

        if new_level != old_level:
            await self._change_service_level(old_level, new_level, avg_availability)

    async def _change_service_level(self,
                                  old_level: ServiceLevel,
                                  new_level: ServiceLevel,
                                  availability: float) -> None:
        """Change service level and log the event."""
        self.service_level = new_level

        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "from_level": old_level.value,
            "to_level": new_level.value,
            "availability_percent": availability,
            "healthy_services": sum(1 for h in self.service_health.values() if h.is_healthy),
            "total_services": len(self.service_health)
        }

        self.degradation_events.append(event)

        # Keep only recent events
        if len(self.degradation_events) > 100:
            self.degradation_events.pop(0)

        if new_level.value != old_level.value:
            if new_level in [ServiceLevel.MINIMAL, ServiceLevel.EMERGENCY]:
                self.logger.warning(f"Service level degraded: {old_level.value} -> {new_level.value} (availability: {availability:.1f}%)")
            else:
                self.logger.info(f"Service level changed: {old_level.value} -> {new_level.value} (availability: {availability:.1f}%)")

    def update_cache(self, service_name: str, data: Dict[str, Any]) -> None:
        """Update cache for a service."""
        self.cache[service_name] = {
            'data': data,
            'timestamp': time.time()
        }

    def get_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary."""
        if not self.service_health:
            return {
                "service_level": self.service_level.value,
                "total_services": 0,
                "healthy_services": 0,
                "overall_availability": 100.0,
                "services": []
            }

        healthy_count = sum(1 for h in self.service_health.values() if h.is_healthy)
        avg_availability = sum(h.availability_percent for h in self.service_health.values()) / len(self.service_health)

        return {
            "service_level": self.service_level.value,
            "total_services": len(self.service_health),
            "healthy_services": healthy_count,
            "overall_availability": avg_availability,
            "services": [
                {
                    "name": h.service_name,
                    "healthy": h.is_healthy,
                    "availability": h.availability_percent,
                    "error_rate": h.error_rate_percent,
                    "response_time_ms": h.response_time_ms,
                    "consecutive_failures": h.consecutive_failures,
                    # Phase 4.2d: Enhanced external service error reporting
                    "external_service_errors": len(getattr(h, '_external_service_errors', [])),
                    "recent_external_errors": len([
                        err for err in getattr(h, '_external_service_errors', [])
                        if (datetime.utcnow() - datetime.fromisoformat(err['timestamp'])).total_seconds() < 300
                    ]),
                    "last_external_error": getattr(h, '_external_service_errors', [])[-1] if getattr(h, '_external_service_errors', []) else None
                }
                for h in self.service_health.values()
            ],
            "recent_degradation_events": self.degradation_events[-10:]
        }

    def get_fallback_metrics(self) -> Dict[str, Any]:
        """Get fallback execution metrics."""
        total_fallbacks = sum(len(executions) for executions in self.fallback_metrics.values())

        if total_fallbacks == 0:
            return {
                "total_fallback_executions": 0,
                "success_rate": 0.0,
                "strategies_used": {},
                "services": {}
            }

        successful_fallbacks = sum(
            sum(1 for exec in executions if exec.success)
            for executions in self.fallback_metrics.values()
        )

        strategy_counts = {}
        for executions in self.fallback_metrics.values():
            for exec in executions:
                strategy = exec.strategy_used.value
                strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        return {
            "total_fallback_executions": total_fallbacks,
            "success_rate": (successful_fallbacks / total_fallbacks) * 100,
            "strategies_used": strategy_counts,
            "services": {
                service: {
                    "total_fallbacks": len(executions),
                    "successful_fallbacks": sum(1 for exec in executions if exec.success),
                    "avg_response_time_ms": sum(exec.response_time_ms for exec in executions) / len(executions)
                }
                for service, executions in self.fallback_metrics.items()
            }
        }