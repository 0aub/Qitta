from __future__ import annotations

import logging
from typing import Optional, List
from playwright.async_api import async_playwright, Browser
from .reliability.stealth import StealthManager, StealthLevel


class BrowserRuntime:
    """Owns the Playwright process and a shared Chromium Browser."""

    def __init__(self, *, headless: bool, args: Optional[List[str]] = None, logger: Optional[logging.Logger] = None,
                 stealth_level: StealthLevel = StealthLevel.MODERATE) -> None:
        self._headless = headless
        self._args = args or []
        self._logger = logger or logging.getLogger("browser")
        self._playwright = None
        self.browser: Optional[Browser] = None

        # Phase 4.3: Advanced stealth integration
        self.stealth_manager = StealthManager(stealth_level, logger)

    async def start(self) -> None:
        self._logger.info("Starting Playwright runtime…")
        self._playwright = await async_playwright().start()

        # Phase 4.3: Enhanced stealth browser launch arguments
        stealth_args = [
            # Basic anti-detection (Phase 4.1)
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-blink-features=AutomationControlled',
            '--disable-features=VizDisplayCompositor',
            '--disable-web-security',
            '--disable-features=Translate',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu-sandbox',
            '--disable-software-rasterizer',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',

            # Phase 4.3: Advanced stealth arguments
            '--disable-extensions-except=/dev/null',
            '--disable-extensions',
            '--disable-plugins',
            '--disable-plugins-discovery',
            '--disable-preconnect',
            '--disable-sync',
            '--disable-translate',
            '--hide-scrollbars',
            '--mute-audio',
            '--no-pings',
            '--disable-background-networking',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-breakpad',
            '--disable-client-side-phishing-detection',
            '--disable-component-extensions-with-background-pages',
            '--disable-default-apps',
            '--disable-features=TranslateUI',
            '--disable-hang-monitor',
            '--disable-ipc-flooding-protection',
            '--disable-popup-blocking',
            '--disable-prompt-on-repost',
            '--disable-renderer-backgrounding',
            '--disable-sync',
            '--force-color-profile=srgb',
            '--metrics-recording-only',
            '--no-crash-upload',
            '--safebrowsing-disable-auto-update',
            '--password-store=basic',
            '--use-mock-keychain',
            '--disable-component-update'
        ]

        # Combine user args with enhanced stealth args
        combined_args = list(self._args) + stealth_args

        self.browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=combined_args
        )
        self._logger.info(f"Chromium launched with enhanced stealth level {self.stealth_manager.stealth_level.value} (headless={self._headless})")

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
