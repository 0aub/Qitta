"""Reliability module for enterprise-grade browser automation.

This module provides comprehensive reliability features including:
- Circuit breaker pattern for external service resilience
- Graceful degradation and fallback mechanisms  
- Advanced error handling and recovery strategies
- Resource monitoring and dynamic scaling
- Health monitoring and alerting systems
"""

from .circuit_breaker import (
    CircuitBreaker, CircuitBreakerManager, CircuitBreakerConfig, 
    CircuitBreakerError, CircuitState
)
from .fallback_manager import (
    FallbackManager, FallbackConfig, FallbackStrategy, 
    ServiceLevel, FallbackExecution, ServiceHealth
)
from .resource_manager import (
    ResourceMonitor, ResourceOptimizer, AdaptiveWorkerPool,
    ConcurrencyThrottler, ResourceState, ScalingAction
)
from .errors import (
    ErrorHandler, ErrorContext, EnhancedError,
    ErrorCategory, ErrorSeverity, RecoveryStrategy,
    NetworkError, BrowserError, TimeoutError
)
from .monitoring import (
    HealthMonitor, MetricsCollector, AlertManager,
    Alert, AlertSeverity, HealthCheck, PerformanceMonitor
)

__all__ = [
    # Circuit Breaker
    'CircuitBreaker', 'CircuitBreakerManager', 'CircuitBreakerConfig',
    'CircuitBreakerError', 'CircuitState',
    
    # Fallback Management
    'FallbackManager', 'FallbackConfig', 'FallbackStrategy',
    'ServiceLevel', 'FallbackExecution', 'ServiceHealth',
    
    # Resource Management
    'ResourceMonitor', 'ResourceOptimizer', 'AdaptiveWorkerPool',
    'ConcurrencyThrottler', 'ResourceState', 'ScalingAction',
    
    # Error Handling
    'ErrorHandler', 'ErrorContext', 'EnhancedError',
    'ErrorCategory', 'ErrorSeverity', 'RecoveryStrategy',
    'NetworkError', 'BrowserError', 'TimeoutError',
    
    # Monitoring
    'HealthMonitor', 'MetricsCollector', 'AlertManager',
    'Alert', 'AlertSeverity', 'HealthCheck', 'PerformanceMonitor'
]
