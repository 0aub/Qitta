# Task modules for browser automation
from .booking_hotels import BookingHotelsTask
from .scrape_site import ScrapeSiteTask
from .saudi_open_data import SaudiOpenDataTask
from .github_repo import GitHubRepoTask

# Export all task classes
__all__ = [
    "BookingHotelsTask",
    "ScrapeSiteTask", 
    "SaudiOpenDataTask",
    "GitHubRepoTask"
]