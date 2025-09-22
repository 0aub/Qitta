"""Task adapter for reliable worker system.

This module provides compatibility between the existing task system
and the new reliable worker system with context management.
"""

from __future__ import annotations

import logging
from typing import Any, Dict
from playwright.async_api import Browser, BrowserContext

from .tasks import TwitterTask, BookingTask, AirbnbTask, WebsiteTask, SaudiTask, GithubTask


class TaskAdapter:
    """Adapter to bridge old task interface with new context-based interface."""

    @staticmethod
    async def twitter(
        *,
        browser: Browser,
        context: BrowserContext,
        params: Dict[str, Any],
        job_output_dir: str,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Twitter task with context support."""
        # For backward compatibility, pass browser to TwitterTask
        # but internally it should use the context
        return await TwitterTask.run(
            browser=browser,
            params=params,
            job_output_dir=job_output_dir,
            logger=logger
        )

    @staticmethod
    async def booking(
        *,
        browser: Browser,
        context: BrowserContext,
        params: Dict[str, Any],
        job_output_dir: str,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Booking task with context support."""
        return await BookingTask.run(
            params=params,
            logger=logger,
            browser=browser,
            job_output_dir=job_output_dir
        )

    @staticmethod
    async def airbnb(
        *,
        browser: Browser,
        context: BrowserContext,
        params: Dict[str, Any],
        job_output_dir: str,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Airbnb task with context support."""
        return await AirbnbTask.run(
            params=params,
            logger=logger,
            browser=browser,
            job_output_dir=job_output_dir
        )

    @staticmethod
    async def website(
        *,
        browser: Browser,
        context: BrowserContext,
        params: Dict[str, Any],
        job_output_dir: str,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Website task with context support."""
        return await WebsiteTask.run(
            browser=browser,
            params=params,
            job_output_dir=job_output_dir,
            logger=logger
        )

    @staticmethod
    async def saudi(
        *,
        browser: Browser,
        context: BrowserContext,
        params: Dict[str, Any],
        job_output_dir: str,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Saudi task with context support."""
        return await SaudiTask.run(
            browser=browser,
            params=params,
            job_output_dir=job_output_dir,
            logger=logger
        )

    @staticmethod
    async def github(
        *,
        browser: Browser,
        context: BrowserContext,
        params: Dict[str, Any],
        job_output_dir: str,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """GitHub task with context support."""
        return await GithubTask.run(
            browser=browser,
            params=params,
            job_output_dir=job_output_dir,
            logger=logger
        )


# Create task registry compatible with reliable workers
reliable_task_registry = {
    "twitter": TaskAdapter.twitter,
    "booking": TaskAdapter.booking,
    "airbnb": TaskAdapter.airbnb,
    "website": TaskAdapter.website,
    "saudi": TaskAdapter.saudi,
    "github": TaskAdapter.github,
}