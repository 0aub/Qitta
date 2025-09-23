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
from .stealth import (
    StealthManager, StealthLevel, UserAgentPool, TimingProfile, BrowserProfile
)
# Conditional import for resource manager (requires psutil)
try:
    from .resource_manager import (
        ResourceMonitor, ResourceOptimizer, AdaptiveWorkerPool,
        ConcurrencyThrottler, ResourceState, ScalingAction
    )
    RESOURCE_MANAGEMENT_AVAILABLE = True
except ImportError:
    # Create placeholder classes when psutil is not available
    class ResourceMonitor:
        def __init__(self, *args, **kwargs):
            pass

    class ResourceOptimizer:
        def __init__(self, *args, **kwargs):
            pass

    class AdaptiveWorkerPool:
        def __init__(self, *args, **kwargs):
            pass

    class ConcurrencyThrottler:
        def __init__(self, *args, **kwargs):
            pass

    class ResourceState:
        OPTIMAL = "optimal"
        HIGH = "high"
        CRITICAL = "critical"
        OVERLOAD = "overload"

    class ScalingAction:
        SCALE_UP = "scale_up"
        SCALE_DOWN = "scale_down"
        MAINTAIN = "maintain"
        THROTTLE = "throttle"

    RESOURCE_MANAGEMENT_AVAILABLE = False
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

    # Stealth and Anti-Detection
    'StealthManager', 'StealthLevel', 'UserAgentPool', 'TimingProfile', 'BrowserProfile',

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
