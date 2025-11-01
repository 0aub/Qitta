# Task modules for browser automation
from typing import Dict, Any, Callable, Awaitable
import logging
from playwright.async_api import Browser

from .booking import BookingTask
from .airbnb import AirbnbTask
from .website import WebsiteTask
from .saudi import SaudiTask
from .github import GithubTask

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
@_registry.register("booking")
async def booking(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await BookingTask.run(params=params, logger=logger, browser=browser, job_output_dir=job_output_dir)

@_registry.register("airbnb")
async def airbnb(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await AirbnbTask.run(params=params, logger=logger, browser=browser, job_output_dir=job_output_dir)

@_registry.register("website") 
async def website(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await WebsiteTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)

@_registry.register("saudi")
async def saudi(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await SaudiTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)

@_registry.register("github")
async def github(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await GithubTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)

# Exports for main.py
task_registry = _registry.tasks
def normalise_task(name: str) -> str:
    return _registry.resolve(name)

# Export all task classes and registry functions
__all__ = [
    "BookingTask",
    "AirbnbTask",
    "WebsiteTask",
    "SaudiTask",
    "GithubTask",
    "task_registry",
    "normalise_task"
]