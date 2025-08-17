"""
Modular task registry that imports tasks from separate modules.

This serves as a transitional approach - tasks are organized in separate modules
but still accessible through a central registry for backward compatibility.
"""

# Import all task modules
from .tasks.booking_hotels import BookingHotelsTask
from .tasks.scrape_site import ScrapeSiteTask
from .tasks.saudi_open_data import SaudiOpenDataTask
from .tasks.github_repo import GitHubRepoTask

# Task registry for the main application
TASK_REGISTRY = {
    "booking-hotels": BookingHotelsTask,
    "scrape-site": ScrapeSiteTask,
    "saudi-open-data": SaudiOpenDataTask,
    "github-repo": GitHubRepoTask
}

# Export for easy access
__all__ = ["TASK_REGISTRY", "BookingHotelsTask", "ScrapeSiteTask", "SaudiOpenDataTask", "GitHubRepoTask"]