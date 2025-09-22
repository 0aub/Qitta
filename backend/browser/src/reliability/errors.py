"""Enhanced error handling and recovery system for browser automation.

Phase 2.1 Implementation:
- Comprehensive error classification
- Intelligent retry strategies
- Error context preservation
- Recovery mechanisms
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from enum import Enum
from typing import Any, Dict, Optional, Type
from datetime import datetime

from pydantic import BaseModel


class ErrorSeverity(str, Enum):
    """Error severity levels for prioritized handling."""
    CRITICAL = "critical"  # System failure, immediate attention
    HIGH = "high"         # Service degradation, urgent
    MEDIUM = "medium"     # Recoverable errors, retry possible
    LOW = "low"          # Minor issues, can be ignored
    INFO = "info"        # Informational, no action needed


class ErrorCategory(str, Enum):
    """Error categories for targeted recovery strategies."""
    NETWORK = "network"           # Network timeouts, connection errors
    BROWSER = "browser"           # Browser crashes, page errors
    AUTHENTICATION = "authentication"  # Login failures, session expired
    RATE_LIMIT = "rate_limit"    # API rate limits, throttling
    PARSING = "parsing"           # Data extraction, selector failures
    VALIDATION = "validation"     # Input validation, data format
    RESOURCE = "resource"         # Memory, CPU, disk space
    TIMEOUT = "timeout"          # Operation timeouts
    PERMISSION = "permission"     # Access denied, unauthorized
    UNKNOWN = "unknown"          # Unclassified errors


class RecoveryStrategy(str, Enum):
    """Recovery strategies for different error types."""
    RETRY_IMMEDIATE = "retry_immediate"
    RETRY_BACKOFF = "retry_backoff"
    RETRY_EXPONENTIAL = "retry_exponential"
    RESTART_BROWSER = "restart_browser"
    REFRESH_SESSION = "refresh_session"
    SWITCH_PROXY = "switch_proxy"
    REDUCE_LOAD = "reduce_load"
    ALERT_HUMAN = "alert_human"
    SKIP = "skip"
    FAIL = "fail"


class ErrorContext(BaseModel):
    """Detailed error context for debugging and recovery."""
    timestamp: datetime
    job_id: Optional[str] = None
    task_name: Optional[str] = None
    worker_id: Optional[str] = None
    url: Optional[str] = None
    selector: Optional[str] = None
    parameters: Dict[str, Any] = {}
    browser_state: Dict[str, Any] = {}
    system_state: Dict[str, Any] = {}
    traceback: Optional[str] = None
    attempt_number: int = 1
    max_attempts: int = 3


class EnhancedError(Exception):
    """Base enhanced error with context and recovery information."""

    def __init__(
        self,
        message: str,
        *,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        recovery_strategy: RecoveryStrategy = RecoveryStrategy.RETRY_BACKOFF,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.severity = severity
        self.recovery_strategy = recovery_strategy
        self.context = context or ErrorContext(timestamp=datetime.utcnow())
        self.cause = cause

        # Capture traceback if not provided
        if not self.context.traceback and cause:
            self.context.traceback = ''.join(
                traceback.format_exception(type(cause), cause, cause.__traceback__)
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for logging/storage."""
        return {
            "message": self.message,
            "category": self.category.value,
            "severity": self.severity.value,
            "recovery_strategy": self.recovery_strategy.value,
            "context": self.context.dict(),
            "cause": str(self.cause) if self.cause else None
        }


# Specific error types with predefined recovery strategies

class NetworkError(EnhancedError):
    """Network-related errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=RecoveryStrategy.RETRY_EXPONENTIAL,
            **kwargs
        )


class BrowserError(EnhancedError):
    """Browser-related errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.BROWSER,
            severity=ErrorSeverity.HIGH,
            recovery_strategy=RecoveryStrategy.RESTART_BROWSER,
            **kwargs
        )


class AuthenticationError(EnhancedError):
    """Authentication failures."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.HIGH,
            recovery_strategy=RecoveryStrategy.REFRESH_SESSION,
            **kwargs
        )


class RateLimitError(EnhancedError):
    """Rate limiting errors."""
    def __init__(self, message: str, wait_seconds: int = 60, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.RATE_LIMIT,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=RecoveryStrategy.RETRY_EXPONENTIAL,
            **kwargs
        )
        self.wait_seconds = wait_seconds


class ParsingError(EnhancedError):
    """Data parsing/extraction errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.PARSING,
            severity=ErrorSeverity.LOW,
            recovery_strategy=RecoveryStrategy.SKIP,
            **kwargs
        )


class TimeoutError(EnhancedError):
    """Operation timeout errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.TIMEOUT,
            severity=ErrorSeverity.MEDIUM,
            recovery_strategy=RecoveryStrategy.RETRY_IMMEDIATE,
            **kwargs
        )


class ResourceError(EnhancedError):
    """Resource exhaustion errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.RESOURCE,
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=RecoveryStrategy.REDUCE_LOAD,
            **kwargs
        )


class ErrorHandler:
    """Intelligent error handler with recovery mechanisms."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.error_counts: Dict[str, int] = {}
        self.last_errors: Dict[str, datetime] = {}
        self.circuit_breakers: Dict[str, bool] = {}

    async def handle_error(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None
    ) -> RecoveryStrategy:
        """Handle an error and determine recovery strategy."""

        # Enhance regular exceptions
        if not isinstance(error, EnhancedError):
            error = self._classify_error(error, context)

        # Log the error
        self._log_error(error)

        # Update error statistics
        self._update_error_stats(error)

        # Check circuit breaker
        if self._is_circuit_open(error):
            self.logger.warning(f"Circuit breaker open for {error.category}")
            return RecoveryStrategy.FAIL

        # Apply recovery strategy
        return await self._apply_recovery(error)

    def _classify_error(
        self,
        error: Exception,
        context: Optional[ErrorContext] = None
    ) -> EnhancedError:
        """Classify a regular exception into an enhanced error."""

        error_str = str(error).lower()
        error_type = type(error).__name__

        # Network errors
        if any(term in error_str for term in ['network', 'connection', 'refused', 'timeout']):
            return NetworkError(str(error), context=context, cause=error)

        # Browser errors
        if any(term in error_str for term in ['browser', 'crashed', 'closed', 'disconnected']):
            return BrowserError(str(error), context=context, cause=error)

        # Authentication errors
        if any(term in error_str for term in ['login', 'auth', 'unauthorized', 'forbidden']):
            return AuthenticationError(str(error), context=context, cause=error)

        # Rate limit errors
        if any(term in error_str for term in ['rate', 'limit', 'throttl', 'too many']):
            return RateLimitError(str(error), context=context, cause=error)

        # Timeout errors
        if 'timeout' in error_type.lower() or 'timeout' in error_str:
            return TimeoutError(str(error), context=context, cause=error)

        # Resource errors
        if any(term in error_str for term in ['memory', 'resource', 'space', 'quota']):
            return ResourceError(str(error), context=context, cause=error)

        # Default
        return EnhancedError(
            str(error),
            category=ErrorCategory.UNKNOWN,
            context=context,
            cause=error
        )

    def _log_error(self, error: EnhancedError) -> None:
        """Log error with appropriate level."""
        log_message = f"[{error.category.value}] {error.message}"

        if error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(log_message, extra=error.to_dict())
        elif error.severity == ErrorSeverity.HIGH:
            self.logger.error(log_message, extra=error.to_dict())
        elif error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(log_message, extra=error.to_dict())
        else:
            self.logger.info(log_message, extra=error.to_dict())

    def _update_error_stats(self, error: EnhancedError) -> None:
        """Update error statistics for pattern detection."""
        key = f"{error.category.value}:{error.severity.value}"

        self.error_counts[key] = self.error_counts.get(key, 0) + 1
        self.last_errors[key] = datetime.utcnow()

        # Open circuit breaker if too many errors
        if self.error_counts[key] > 10:  # Threshold
            time_since_last = (datetime.utcnow() - self.last_errors[key]).seconds
            if time_since_last < 60:  # Within 1 minute
                self.circuit_breakers[error.category.value] = True
                self.logger.error(f"Circuit breaker opened for {error.category.value}")

    def _is_circuit_open(self, error: EnhancedError) -> bool:
        """Check if circuit breaker is open for this error category."""
        return self.circuit_breakers.get(error.category.value, False)

    async def _apply_recovery(self, error: EnhancedError) -> RecoveryStrategy:
        """Apply recovery strategy based on error type."""
        strategy = error.recovery_strategy

        if strategy == RecoveryStrategy.RETRY_IMMEDIATE:
            self.logger.info(f"Retrying immediately for {error.category.value}")

        elif strategy == RecoveryStrategy.RETRY_BACKOFF:
            wait_time = 5 * (error.context.attempt_number or 1)
            self.logger.info(f"Retrying with backoff ({wait_time}s) for {error.category.value}")
            await asyncio.sleep(wait_time)

        elif strategy == RecoveryStrategy.RETRY_EXPONENTIAL:
            wait_time = min(2 ** (error.context.attempt_number or 1), 300)
            self.logger.info(f"Retrying with exponential backoff ({wait_time}s)")
            await asyncio.sleep(wait_time)

        elif strategy == RecoveryStrategy.RESTART_BROWSER:
            self.logger.warning("Browser restart required")
            # Browser restart will be handled by worker

        elif strategy == RecoveryStrategy.REFRESH_SESSION:
            self.logger.warning("Session refresh required")
            # Session refresh will be handled by task

        elif strategy == RecoveryStrategy.ALERT_HUMAN:
            self.logger.critical("Human intervention required!")
            # Could send alert via webhook/email

        return strategy

    def reset_circuit_breaker(self, category: ErrorCategory) -> None:
        """Reset circuit breaker for a category."""
        if category.value in self.circuit_breakers:
            del self.circuit_breakers[category.value]
            self.logger.info(f"Circuit breaker reset for {category.value}")

    def get_error_stats(self) -> Dict[str, Any]:
        """Get current error statistics."""
        return {
            "error_counts": self.error_counts,
            "circuit_breakers": list(self.circuit_breakers.keys()),
            "recent_errors": {
                k: v.isoformat() for k, v in self.last_errors.items()
                if (datetime.utcnow() - v).seconds < 3600
            }
        }


# Global error handler instance (will be initialized by service)
error_handler: Optional[ErrorHandler] = None


def get_error_handler() -> ErrorHandler:
    """Get the global error handler instance."""
    if not error_handler:
        raise RuntimeError("Error handler not initialized")
    return error_handler