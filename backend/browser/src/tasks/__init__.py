# Task modules for browser automation
from typing import Dict, Any, Callable, Awaitable
import logging
from playwright.async_api import Browser

from .booking_hotels import BookingHotelsTask
from .scrape_site import ScrapeSiteTask
from .saudi_open_data import SaudiOpenDataTask
from .github_repo import GitHubRepoTask

# Task registry system
class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: Dict[str, Callable[..., Awaitable[Dict[str, Any]]]] = {}
    
    def register(self, name: str):
        def deco(fn):
            self._tasks[name] = fn
            return fn
        return deco
    
    def resolve(self, name: str) -> str:
        variants = [name, name.replace("_", "-"), name.replace("-", "_")]
        for v in variants:
            if v in self._tasks:
                return v
        return name
    
    @property
    def tasks(self) -> Dict[str, Callable[..., Awaitable[Dict[str, Any]]]]:
        return self._tasks

_registry = TaskRegistry()

# Register task wrapper functions
@_registry.register("booking-hotels")
async def booking_hotels(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await BookingHotelsTask.run(params=params, logger=logger, browser=browser, job_output_dir=job_output_dir)

@_registry.register("scrape-site") 
async def scrape_site(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await ScrapeSiteTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)

@_registry.register("saudi-open-data")
async def saudi_open_data(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await SaudiOpenDataTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)

@_registry.register("github-repo")
async def github_repo(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await GitHubRepoTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)

# Exports for main.py
task_registry = _registry.tasks
def normalise_task(name: str) -> str:
    return _registry.resolve(name)

# Export all task classes and registry functions
__all__ = [
    "BookingHotelsTask",
    "ScrapeSiteTask", 
    "SaudiOpenDataTask",
    "GitHubRepoTask",
    "task_registry",
    "normalise_task"
]