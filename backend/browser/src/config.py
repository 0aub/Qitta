from __future__ import annotations
import os

# Environment-backed configuration with the same names you already use
SERVICE_PORT: int = int(os.getenv("SERVICE_PORT", "8000"))
LOG_ROOT: str = os.getenv("LOG_ROOT", "/storage/logs")
DATA_ROOT: str = os.getenv("OUTPUT_ROOT", "/storage/scraped_data")
MAX_CONCURRENT_JOBS: int = int(os.getenv("MAX_CONCURRENT_JOBS", "2"))
API_KEY: str = os.getenv("BROWSER_API_KEY", "")
HEADLESS: bool = os.getenv("BROWSER_HEADLESS", "true").lower() != "false"
