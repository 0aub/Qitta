from __future__ import annotations

import logging
from typing import Optional, List
from playwright.async_api import async_playwright, Browser


class BrowserRuntime:
    """Owns the Playwright process and a shared Chromium Browser."""

    def __init__(self, *, headless: bool, args: Optional[List[str]] = None, logger: Optional[logging.Logger] = None) -> None:
        self._headless = headless
        self._args = args or []
        self._logger = logger or logging.getLogger("browser")
        self._playwright = None
        self.browser: Optional[Browser] = None

    async def start(self) -> None:
        self._logger.info("Starting Playwright runtime…")
        self._playwright = await async_playwright().start()
        self.browser = await self._playwright.chromium.launch(headless=self._headless, args=self._args)
        self._logger.info(f"Chromium launched (headless={self._headless})")

    async def stop(self) -> None:
        self._logger.info("Shutting down Playwright runtime…")
        if self.browser:
            try:
                await self.browser.close()
            except Exception:
                pass
            self.browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None
