"""Production configuration management for reliable browser automation service.

Phase 4.4a: Production Configuration Management
- Environment-based configuration loading
- Secrets management and security settings
- Performance and scaling configurations
- Monitoring and observability settings
- Production-specific reliability parameters
"""

from __future__ import annotations

import os
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum
import json


class DeploymentEnvironment(str, Enum):
    """Deployment environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class LogLevel(str, Enum):
    """Logging levels for production."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class SecurityConfig:
    """Security configuration for production deployment."""
    api_key_required: bool = False
    api_key: str = ""  # Set via BROWSER_API_KEY environment variable
    rate_limiting_enabled: bool = True
    rate_limit_per_minute: int = 60
    max_request_size_mb: int = 10
    cors_origins: List[str] = field(default_factory=list)
    trust_proxy_headers: bool = False
    enforce_https: bool = True


@dataclass
class ScalingConfig:
    """Auto-scaling and performance configuration."""
    min_workers: int = 2
    max_workers: int = 10
    worker_auto_restart_threshold: int = 100  # requests before restart
    max_concurrent_jobs: int = 50
    job_timeout_seconds: int = 300
    resource_monitoring_enabled: bool = True
    cpu_scale_up_threshold: float = 0.8
    cpu_scale_down_threshold: float = 0.3
    memory_scale_up_threshold: float = 0.85
    memory_scale_down_threshold: float = 0.4


@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration."""
    prometheus_enabled: bool = True
    metrics_port: int = 9090
    health_check_interval_seconds: int = 30
    alert_webhook_url: Optional[str] = None
    log_structured: bool = True
    log_level: LogLevel = LogLevel.INFO
    performance_tracking_enabled: bool = True
    trace_sampling_rate: float = 0.1  # 10% sampling for distributed tracing


@dataclass
class ReliabilityConfig:
    """Production reliability configuration."""
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout_seconds: int = 60
    fallback_enabled: bool = True
    dead_letter_queue_enabled: bool = True
    retry_max_attempts: int = 3
    retry_exponential_base: float = 2.0
    retry_max_delay_seconds: int = 60


@dataclass
class SystemConfig:
    """System configuration for paths and basic settings."""
    log_root: str = "/tmp/logs"
    data_root: str = "../storage/scraped_data"
    service_port: int = 8004
    log_level: str = "INFO"
    timezone: str = "UTC"


@dataclass
class RedisConfig:
    """Redis configuration for production."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False
    connection_pool_size: int = 10
    socket_timeout_seconds: int = 5
    socket_connect_timeout_seconds: int = 5


@dataclass
class BrowserConfig:
    """Browser automation configuration."""
    headless: bool = True
    stealth_level: str = "moderate"  # basic, moderate, aggressive, paranoid
    user_data_persistence: bool = False
    browser_pool_size: int = 5
    page_timeout_seconds: int = 30
    navigation_timeout_seconds: int = 30


class ProductionConfig:
    """Production configuration manager."""

    def __init__(self, environment: DeploymentEnvironment = DeploymentEnvironment.PRODUCTION):
        self.environment = environment
        self._load_configuration()

    def _load_configuration(self) -> None:
        """Load configuration based on environment and environment variables."""
        # Base configuration
        self.system = SystemConfig()
        self.security = SecurityConfig()
        self.scaling = ScalingConfig()
        self.monitoring = MonitoringConfig()
        self.reliability = ReliabilityConfig()
        self.redis = RedisConfig()
        self.browser = BrowserConfig()

        # Load environment-specific overrides
        self._load_from_environment()
        self._validate_configuration()

    def _load_from_environment(self) -> None:
        """Load configuration from environment variables."""
        # System settings
        self.system.log_root = os.getenv("LOG_ROOT", self.system.log_root)
        self.system.data_root = os.getenv("DATA_ROOT", self.system.data_root)
        self.system.service_port = self._get_int_env("SERVICE_PORT", self.system.service_port)
        self.system.log_level = os.getenv("LOG_LEVEL", self.system.log_level)

        # Security settings
        self.security.api_key_required = self._get_bool_env("API_KEY_REQUIRED", self.security.api_key_required)
        self.security.rate_limiting_enabled = self._get_bool_env("RATE_LIMITING_ENABLED", self.security.rate_limiting_enabled)
        self.security.rate_limit_per_minute = self._get_int_env("RATE_LIMIT_PER_MINUTE", self.security.rate_limit_per_minute)
        self.security.enforce_https = self._get_bool_env("ENFORCE_HTTPS", self.security.enforce_https)

        # CORS origins from environment
        cors_origins_str = os.getenv("CORS_ORIGINS", "")
        if cors_origins_str:
            self.security.cors_origins = [origin.strip() for origin in cors_origins_str.split(",")]

        # Scaling settings
        self.scaling.min_workers = self._get_int_env("MIN_WORKERS", self.scaling.min_workers)
        self.scaling.max_workers = self._get_int_env("MAX_WORKERS", self.scaling.max_workers)
        self.scaling.max_concurrent_jobs = self._get_int_env("MAX_CONCURRENT_JOBS", self.scaling.max_concurrent_jobs)
        self.scaling.job_timeout_seconds = self._get_int_env("JOB_TIMEOUT_SECONDS", self.scaling.job_timeout_seconds)

        # Monitoring settings
        self.monitoring.prometheus_enabled = self._get_bool_env("PROMETHEUS_ENABLED", self.monitoring.prometheus_enabled)
        self.monitoring.metrics_port = self._get_int_env("METRICS_PORT", self.monitoring.metrics_port)
        self.monitoring.alert_webhook_url = os.getenv("ALERT_WEBHOOK_URL")
        self.monitoring.log_level = LogLevel(os.getenv("LOG_LEVEL", self.monitoring.log_level.value))

        # Redis settings
        self.redis.host = os.getenv("REDIS_HOST", self.redis.host)
        self.redis.port = self._get_int_env("REDIS_PORT", self.redis.port)
        self.redis.db = self._get_int_env("REDIS_DB", self.redis.db)
        self.redis.password = os.getenv("REDIS_PASSWORD")
        self.redis.ssl = self._get_bool_env("REDIS_SSL", self.redis.ssl)

        # Browser settings
        self.browser.headless = self._get_bool_env("BROWSER_HEADLESS", self.browser.headless)
        self.browser.stealth_level = os.getenv("STEALTH_LEVEL", self.browser.stealth_level)

    def _get_bool_env(self, key: str, default: bool) -> bool:
        """Get boolean value from environment."""
        value = os.getenv(key, "").lower()
        if value in ("true", "1", "yes", "on"):
            return True
        elif value in ("false", "0", "no", "off"):
            return False
        return default

    def _get_int_env(self, key: str, default: int) -> int:
        """Get integer value from environment."""
        try:
            return int(os.getenv(key, str(default)))
        except ValueError:
            return default

    def _validate_configuration(self) -> None:
        """Validate configuration values."""
        if self.scaling.min_workers > self.scaling.max_workers:
            raise ValueError("min_workers cannot be greater than max_workers")

        if self.scaling.max_concurrent_jobs < self.scaling.min_workers:
            raise ValueError("max_concurrent_jobs should be at least min_workers")

        if self.redis.port < 1 or self.redis.port > 65535:
            raise ValueError("Redis port must be between 1 and 65535")

        if self.browser.stealth_level not in ["basic", "moderate", "aggressive", "paranoid"]:
            raise ValueError("Invalid stealth level. Must be: basic, moderate, aggressive, or paranoid")

    def get_redis_url(self) -> str:
        """Get Redis connection URL."""
        protocol = "rediss" if self.redis.ssl else "redis"
        auth = f":{self.redis.password}@" if self.redis.password else ""
        return f"{protocol}://{auth}{self.redis.host}:{self.redis.port}/{self.redis.db}"

    def get_configuration_summary(self) -> Dict[str, Any]:
        """Get configuration summary for logging/debugging."""
        return {
            "environment": self.environment.value,
            "security": {
                "api_key_required": self.security.api_key_required,
                "rate_limiting_enabled": self.security.rate_limiting_enabled,
                "enforce_https": self.security.enforce_https,
                "cors_origins_count": len(self.security.cors_origins)
            },
            "scaling": {
                "min_workers": self.scaling.min_workers,
                "max_workers": self.scaling.max_workers,
                "max_concurrent_jobs": self.scaling.max_concurrent_jobs,
                "auto_scaling_enabled": self.scaling.resource_monitoring_enabled
            },
            "monitoring": {
                "prometheus_enabled": self.monitoring.prometheus_enabled,
                "log_level": self.monitoring.log_level.value,
                "health_check_interval": self.monitoring.health_check_interval_seconds
            },
            "reliability": {
                "circuit_breaker_enabled": self.reliability.circuit_breaker_enabled,
                "fallback_enabled": self.reliability.fallback_enabled,
                "dead_letter_queue_enabled": self.reliability.dead_letter_queue_enabled
            },
            "browser": {
                "headless": self.browser.headless,
                "stealth_level": self.browser.stealth_level,
                "pool_size": self.browser.browser_pool_size
            }
        }

    def setup_logging(self) -> logging.Logger:
        """Setup production logging configuration."""
        logger = logging.getLogger("browser.production")
        logger.setLevel(getattr(logging, self.monitoring.log_level.value))

        # Clear existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Create formatter
        if self.monitoring.log_structured:
            # Structured JSON logging for production
            formatter = logging.Formatter(
                '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                '"logger": "%(name)s", "message": "%(message)s", '
                '"environment": "' + self.environment.value + '"}'
            )
        else:
            # Standard logging for development
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
            )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger


# Global configuration instance
_config_instance: Optional[ProductionConfig] = None


def get_config(environment: Optional[DeploymentEnvironment] = None) -> ProductionConfig:
    """Get or create global configuration instance."""
    global _config_instance

    if _config_instance is None or (environment and environment != _config_instance.environment):
        if environment is None:
            # Determine environment from env var
            env_str = os.getenv("DEPLOYMENT_ENVIRONMENT", "production").lower()
            try:
                environment = DeploymentEnvironment(env_str)
            except ValueError:
                environment = DeploymentEnvironment.PRODUCTION

        _config_instance = ProductionConfig(environment)

    return _config_instance


def reset_config() -> None:
    """Reset global configuration instance (useful for testing)."""
    global _config_instance
    _config_instance = None