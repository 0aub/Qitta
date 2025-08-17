"""
Base utilities and shared functions for all scraping tasks.
"""
import logging
from typing import Any, Dict


def _log(logger: logging.Logger, level: str, message: str):
    """Centralized logging utility for all tasks."""
    getattr(logger, level.lower())(message)


def validate_required_params(params: Dict[str, Any], required_fields: list) -> Dict[str, str]:
    """Validate that required parameters are present."""
    errors = {}
    for field in required_fields:
        if field not in params or not params[field]:
            errors[field] = f"{field} is required"
    return errors


def extract_domain(url: str) -> str:
    """Extract domain from URL for classification."""
    import urllib.parse
    parsed = urllib.parse.urlparse(url)
    return parsed.netloc.lower()