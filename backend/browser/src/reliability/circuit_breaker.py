"""Circuit breaker pattern implementation for external service resilience.

Phase 2.5 Implementation:
- Circuit breaker for external service calls
- State management (Closed, Open, Half-Open)
- Configurable failure thresholds and timeouts
- Metrics integration and monitoring
- Automatic service recovery detection
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional, List
from enum import Enum
import logging


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"        # Normal operation
    OPEN = "open"           # Failing, rejecting requests
    HALF_OPEN = "half_open" # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5          # Failures before opening
    recovery_timeout: float = 60.0      # Seconds before trying half-open
    success_threshold: int = 2          # Successes before closing from half-open
    timeout: float = 30.0               # Request timeout in seconds
    reset_timeout: float = 300.0        # Reset failure count after this time


@dataclass
class CircuitMetrics:
    """Circuit breaker metrics and statistics."""
    total_requests: int = 0
    total_successes: int = 0
    total_failures: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    state_changes: List[Dict[str, Any]] = field(default_factory=list)


class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open."""

    def __init__(self, message: str, circuit_name: str, state: CircuitState):
        super().__init__(message)
        self.circuit_name = circuit_name
        self.state = state


class CircuitBreaker:
    """Circuit breaker implementation for external service calls."""

    def __init__(self,
                 name: str,
                 config: Optional[CircuitBreakerConfig] = None,
                 logger: Optional[logging.Logger] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.logger = logger or logging.getLogger(__name__)

        self.state = CircuitState.CLOSED
        self.metrics = CircuitMetrics()
        self.last_state_change = datetime.utcnow()
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function through the circuit breaker."""
        async with self._lock:
            # Check if we should allow the request
            if not await self._should_allow_request():
                self.metrics.total_requests += 1
                raise CircuitBreakerError(
                    f"Circuit breaker '{self.name}' is {self.state.value}",
                    self.name,
                    self.state
                )

        # Execute the request
        self.metrics.total_requests += 1
        start_time = time.time()

        try:
            # Add timeout to the call
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout
            )

            # Record success
            execution_time = time.time() - start_time
            await self._record_success(execution_time)
            return result

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            await self._record_failure("timeout", execution_time)
            raise

        except Exception as e:
            execution_time = time.time() - start_time
            await self._record_failure(str(type(e).__name__), execution_time)
            raise

    async def _should_allow_request(self) -> bool:
        """Determine if a request should be allowed based on circuit state."""
        now = datetime.utcnow()

        if self.state == CircuitState.CLOSED:
            return True

        elif self.state == CircuitState.OPEN:
            # Check if we should transition to half-open
            time_since_open = (now - self.last_state_change).total_seconds()
            if time_since_open >= self.config.recovery_timeout:
                await self._transition_to_half_open()
                return True
            return False

        elif self.state == CircuitState.HALF_OPEN:
            # Allow limited requests to test service recovery
            return True

        return False

    async def _record_success(self, execution_time: float) -> None:
        """Record a successful request."""
        async with self._lock:
            self.metrics.total_successes += 1
            self.metrics.consecutive_successes += 1
            self.metrics.consecutive_failures = 0
            self.metrics.last_success_time = datetime.utcnow()

            # State transitions based on success
            if self.state == CircuitState.HALF_OPEN:
                if self.metrics.consecutive_successes >= self.config.success_threshold:
                    await self._transition_to_closed()

            self.logger.debug(
                f"Circuit breaker '{self.name}' recorded success "
                f"(execution_time: {execution_time:.3f}s, consecutive: {self.metrics.consecutive_successes})"
            )

    async def _record_failure(self, error_type: str, execution_time: float) -> None:
        """Record a failed request."""
        async with self._lock:
            self.metrics.total_failures += 1
            self.metrics.consecutive_failures += 1
            self.metrics.consecutive_successes = 0
            self.metrics.last_failure_time = datetime.utcnow()

            # State transitions based on failure
            if self.state == CircuitState.CLOSED:
                if self.metrics.consecutive_failures >= self.config.failure_threshold:
                    await self._transition_to_open()

            elif self.state == CircuitState.HALF_OPEN:
                # Any failure in half-open state goes back to open
                await self._transition_to_open()

            self.logger.warning(
                f"Circuit breaker '{self.name}' recorded failure "
                f"(error: {error_type}, execution_time: {execution_time:.3f}s, "
                f"consecutive: {self.metrics.consecutive_failures})"
            )

    async def _transition_to_open(self) -> None:
        """Transition circuit breaker to open state."""
        old_state = self.state
        self.state = CircuitState.OPEN
        self.last_state_change = datetime.utcnow()

        self._record_state_change(old_state, CircuitState.OPEN)
        self.logger.error(
            f"Circuit breaker '{self.name}' opened "
            f"(consecutive failures: {self.metrics.consecutive_failures})"
        )

    async def _transition_to_half_open(self) -> None:
        """Transition circuit breaker to half-open state."""
        old_state = self.state
        self.state = CircuitState.HALF_OPEN
        self.last_state_change = datetime.utcnow()
        self.metrics.consecutive_successes = 0

        self._record_state_change(old_state, CircuitState.HALF_OPEN)
        self.logger.info(
            f"Circuit breaker '{self.name}' half-opened (testing service recovery)"
        )

    async def _transition_to_closed(self) -> None:
        """Transition circuit breaker to closed state."""
        old_state = self.state
        self.state = CircuitState.CLOSED
        self.last_state_change = datetime.utcnow()
        self.metrics.consecutive_failures = 0

        self._record_state_change(old_state, CircuitState.CLOSED)
        self.logger.info(
            f"Circuit breaker '{self.name}' closed (service recovered)"
        )

    def _record_state_change(self, from_state: CircuitState, to_state: CircuitState) -> None:
        """Record a state change for metrics."""
        self.metrics.state_changes.append({
            "from_state": from_state.value,
            "to_state": to_state.value,
            "timestamp": datetime.utcnow().isoformat(),
            "consecutive_failures": self.metrics.consecutive_failures,
            "consecutive_successes": self.metrics.consecutive_successes
        })

        # Keep only recent state changes (last 50)
        if len(self.metrics.state_changes) > 50:
            self.metrics.state_changes.pop(0)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current circuit breaker metrics."""
        success_rate = 0.0
        if self.metrics.total_requests > 0:
            success_rate = self.metrics.total_successes / self.metrics.total_requests * 100

        time_in_current_state = (datetime.utcnow() - self.last_state_change).total_seconds()

        return {
            "name": self.name,
            "state": self.state.value,
            "time_in_current_state_seconds": time_in_current_state,
            "total_requests": self.metrics.total_requests,
            "total_successes": self.metrics.total_successes,
            "total_failures": self.metrics.total_failures,
            "success_rate_percent": success_rate,
            "consecutive_failures": self.metrics.consecutive_failures,
            "consecutive_successes": self.metrics.consecutive_successes,
            "last_failure": self.metrics.last_failure_time.isoformat() if self.metrics.last_failure_time else None,
            "last_success": self.metrics.last_success_time.isoformat() if self.metrics.last_success_time else None,
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "success_threshold": self.config.success_threshold,
                "timeout": self.config.timeout
            },
            "recent_state_changes": self.metrics.state_changes[-10:]  # Last 10 changes
        }

    async def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        async with self._lock:
            old_state = self.state
            self.state = CircuitState.CLOSED
            self.last_state_change = datetime.utcnow()
            self.metrics.consecutive_failures = 0
            self.metrics.consecutive_successes = 0

            self._record_state_change(old_state, CircuitState.CLOSED)
            self.logger.info(f"Circuit breaker '{self.name}' manually reset")


class CircuitBreakerManager:
    """Manager for multiple circuit breakers."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.default_config = CircuitBreakerConfig()

    def get_or_create_breaker(self,
                             name: str,
                             config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get existing circuit breaker or create a new one."""
        if name not in self.breakers:
            self.breakers[name] = CircuitBreaker(
                name=name,
                config=config or self.default_config,
                logger=self.logger
            )
            self.logger.info(f"Created circuit breaker '{name}'")

        return self.breakers[name]

    async def call_with_breaker(self,
                               breaker_name: str,
                               func: Callable,
                               *args,
                               config: Optional[CircuitBreakerConfig] = None,
                               **kwargs) -> Any:
        """Execute a function through a named circuit breaker."""
        breaker = self.get_or_create_breaker(breaker_name, config)
        return await breaker.call(func, *args, **kwargs)

    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics for all circuit breakers."""
        return {
            name: breaker.get_metrics()
            for name, breaker in self.breakers.items()
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics for all circuit breakers."""
        total_breakers = len(self.breakers)
        open_breakers = sum(1 for b in self.breakers.values() if b.state == CircuitState.OPEN)
        half_open_breakers = sum(1 for b in self.breakers.values() if b.state == CircuitState.HALF_OPEN)

        return {
            "total_circuit_breakers": total_breakers,
            "open_circuit_breakers": open_breakers,
            "half_open_circuit_breakers": half_open_breakers,
            "closed_circuit_breakers": total_breakers - open_breakers - half_open_breakers,
            "breaker_names": list(self.breakers.keys())
        }

    async def reset_breaker(self, name: str) -> bool:
        """Reset a specific circuit breaker."""
        if name in self.breakers:
            await self.breakers[name].reset()
            return True
        return False

    async def reset_all_breakers(self) -> None:
        """Reset all circuit breakers."""
        for breaker in self.breakers.values():
            await breaker.reset()
        self.logger.info("Reset all circuit breakers")