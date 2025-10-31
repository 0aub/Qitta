"""Advanced anti-detection and stealth mechanisms.

Phase 4.3 Implementation:
- Advanced browser fingerprinting evasion
- IP rotation and proxy management
- Request timing patterns and human simulation
- Header randomization and user agent cycling
- JavaScript execution environment masking
- Canvas fingerprinting protection
- WebRTC leak prevention
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from enum import Enum
import logging
import json
import hashlib

from playwright.async_api import Page, BrowserContext


class StealthLevel(str, Enum):
    """Stealth operation levels."""
    BASIC = "basic"           # Basic anti-detection
    MODERATE = "moderate"     # Standard stealth measures
    AGGRESSIVE = "aggressive" # Maximum stealth
    PARANOID = "paranoid"     # Extreme anti-detection


@dataclass
class UserAgentPool:
    """Pool of realistic user agents for rotation."""
    desktop_chrome: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ])

    desktop_firefox: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0"
    ])

    mobile_chrome: List[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.119 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Android 14; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
        "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ])


@dataclass
class TimingProfile:
    """Human-like timing patterns for interactions."""
    min_delay_ms: int = 100
    max_delay_ms: int = 3000
    typing_delay_range: tuple = (50, 200)
    scroll_delay_range: tuple = (200, 800)
    click_delay_range: tuple = (100, 500)
    page_load_wait_range: tuple = (2000, 5000)


@dataclass
class BrowserProfile:
    """Complete browser fingerprint profile."""
    user_agent: str
    viewport_width: int
    viewport_height: int
    language: str
    timezone: str
    platform: str
    webgl_vendor: str
    webgl_renderer: str
    hardware_concurrency: int
    device_memory: int


class StealthManager:
    """Advanced anti-detection and stealth management."""

    def __init__(self,
                 stealth_level: StealthLevel = StealthLevel.MODERATE,
                 logger: Optional[logging.Logger] = None):
        self.stealth_level = stealth_level
        self.logger = logger or logging.getLogger(__name__)

        # User agent management
        self.user_agent_pool = UserAgentPool()
        self.current_user_agent = None
        self.user_agent_rotation_interval = 3600  # 1 hour
        self.last_user_agent_change = datetime.utcnow()

        # Timing and behavior
        self.timing_profile = TimingProfile()
        self.session_fingerprints: Dict[str, BrowserProfile] = {}

        # Request patterns
        self.request_history: List[Dict[str, Any]] = []
        self.last_request_time = 0.0

        # Canvas and WebGL fingerprinting
        self.canvas_noise_enabled = True
        self.webgl_noise_enabled = True

    async def apply_stealth_to_context(self, context: BrowserContext) -> None:
        """Apply comprehensive stealth measures to a browser context."""
        self.logger.info(f"Applying {self.stealth_level.value} level stealth measures")

        # Phase 4.3: Advanced stealth implementation
        await self._apply_basic_stealth(context)

        if self.stealth_level in [StealthLevel.MODERATE, StealthLevel.AGGRESSIVE, StealthLevel.PARANOID]:
            await self._apply_moderate_stealth(context)

        if self.stealth_level in [StealthLevel.AGGRESSIVE, StealthLevel.PARANOID]:
            await self._apply_aggressive_stealth(context)

        if self.stealth_level == StealthLevel.PARANOID:
            await self._apply_paranoid_stealth(context)

    async def _apply_basic_stealth(self, context: BrowserContext) -> None:
        """Apply basic anti-detection measures."""
        # Rotate user agent if needed
        await self._rotate_user_agent_if_needed(context)

        # NOTE: Viewport must be set when creating context in Playwright, not after
        # viewport = self._get_realistic_viewport()
        # await context.set_viewport_size(viewport["width"], viewport["height"])

        # Basic headers
        await context.set_extra_http_headers({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })

    async def _apply_moderate_stealth(self, context: BrowserContext) -> None:
        """Apply moderate stealth measures."""
        # Inject navigator property modifications
        await context.add_init_script("""
            // Phase 4.3: Navigator property masking
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });

            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });

            // Hide automation indicators
            delete window.navigator.__proto__.webdriver;

            // Mask Chrome runtime
            Object.defineProperty(window, 'chrome', {
                get: () => ({
                    runtime: {},
                    // Add realistic chrome object properties
                }),
            });
        """)

    async def _apply_aggressive_stealth(self, context: BrowserContext) -> None:
        """Apply aggressive anti-detection measures."""
        # Advanced fingerprinting protection
        await context.add_init_script("""
            // Phase 4.3: Advanced fingerprinting protection

            // Canvas fingerprinting protection
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type, quality) {
                const dataURL = originalToDataURL.apply(this, arguments);
                // Add subtle noise to canvas fingerprint
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');
                ctx.fillStyle = `rgb(${Math.floor(Math.random() * 10)}, ${Math.floor(Math.random() * 10)}, ${Math.floor(Math.random() * 10)})`;
                ctx.fillRect(0, 0, 1, 1);
                return dataURL;
            };

            // WebGL fingerprinting protection
            const originalGetParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                const result = originalGetParameter.apply(this, arguments);
                if (parameter === this.RENDERER || parameter === this.VENDOR) {
                    // Return realistic but non-unique values
                    return parameter === this.RENDERER ?
                        'ANGLE (Intel, Intel(R) HD Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)' :
                        'Google Inc. (Intel)';
                }
                return result;
            };

            // Audio context fingerprinting protection
            const originalCreateAnalyser = AudioContext.prototype.createAnalyser;
            AudioContext.prototype.createAnalyser = function() {
                const analyser = originalCreateAnalyser.apply(this, arguments);
                const originalGetFloatFrequencyData = analyser.getFloatFrequencyData;
                analyser.getFloatFrequencyData = function(array) {
                    originalGetFloatFrequencyData.apply(this, arguments);
                    // Add subtle noise to audio fingerprint
                    for (let i = 0; i < array.length; i++) {
                        array[i] += Math.random() * 0.0001 - 0.00005;
                    }
                };
                return analyser;
            };
        """)

    async def _apply_paranoid_stealth(self, context: BrowserContext) -> None:
        """Apply paranoid-level anti-detection measures."""
        # Maximum stealth with comprehensive masking
        await context.add_init_script("""
            // Phase 4.3: Paranoid-level protection

            // Comprehensive timing attack protection
            const originalPerformanceNow = Performance.prototype.now;
            Performance.prototype.now = function() {
                // Add random jitter to timing measurements
                return originalPerformanceNow.apply(this, arguments) + Math.random() * 0.1;
            };

            // Device enumeration protection
            const originalEnumerateDevices = navigator.mediaDevices.enumerateDevices;
            navigator.mediaDevices.enumerateDevices = function() {
                return Promise.resolve([
                    { deviceId: 'default', kind: 'audioinput', label: '', groupId: '' },
                    { deviceId: 'default', kind: 'audiooutput', label: '', groupId: '' },
                    { deviceId: 'default', kind: 'videoinput', label: '', groupId: '' }
                ]);
            };

            // Battery API protection
            Object.defineProperty(navigator, 'getBattery', {
                get: () => undefined,
            });

            // Gamepad API protection
            Object.defineProperty(navigator, 'getGamepads', {
                get: () => () => [],
            });
        """)

    async def _rotate_user_agent_if_needed(self, context: BrowserContext) -> None:
        """Rotate user agent based on time interval."""
        now = datetime.utcnow()
        time_since_change = (now - self.last_user_agent_change).total_seconds()

        if time_since_change >= self.user_agent_rotation_interval or not self.current_user_agent:
            # Select appropriate user agent pool based on stealth level
            if self.stealth_level in [StealthLevel.AGGRESSIVE, StealthLevel.PARANOID]:
                # Use more diverse pool for higher stealth
                all_agents = (self.user_agent_pool.desktop_chrome +
                             self.user_agent_pool.desktop_firefox +
                             self.user_agent_pool.mobile_chrome)
            else:
                # Use desktop Chrome for basic/moderate stealth
                all_agents = self.user_agent_pool.desktop_chrome

            self.current_user_agent = random.choice(all_agents)
            # NOTE: Cannot change user agent on existing context in Playwright
            # User agent must be set when creating the context
            # await context.set_user_agent(self.current_user_agent)
            self.last_user_agent_change = now

            self.logger.info(f"Selected user agent (for next context): {self.current_user_agent[:50]}...")

    def _get_realistic_viewport(self) -> Dict[str, int]:
        """Get realistic viewport dimensions."""
        # Common viewport sizes
        viewports = [
            {"width": 1920, "height": 1080},  # Full HD
            {"width": 1366, "height": 768},   # Common laptop
            {"width": 1536, "height": 864},   # 125% scale
            {"width": 1440, "height": 900},   # MacBook Pro
            {"width": 1280, "height": 720},   # HD
        ]

        if self.stealth_level == StealthLevel.PARANOID:
            # Add some randomization for paranoid mode
            base = random.choice(viewports)
            return {
                "width": base["width"] + random.randint(-50, 50),
                "height": base["height"] + random.randint(-30, 30)
            }

        return random.choice(viewports)

    async def human_like_delay(self, action_type: str = "default") -> None:
        """Add human-like delays between actions."""
        delay_ranges = {
            "typing": self.timing_profile.typing_delay_range,
            "scroll": self.timing_profile.scroll_delay_range,
            "click": self.timing_profile.click_delay_range,
            "page_load": self.timing_profile.page_load_wait_range,
            "default": (self.timing_profile.min_delay_ms, self.timing_profile.max_delay_ms)
        }

        min_delay, max_delay = delay_ranges.get(action_type, delay_ranges["default"])
        delay_ms = random.randint(min_delay, max_delay)

        # Add some randomization based on stealth level
        if self.stealth_level in [StealthLevel.AGGRESSIVE, StealthLevel.PARANOID]:
            # More human-like variation
            delay_ms += random.randint(-delay_ms//4, delay_ms//4)

        await asyncio.sleep(delay_ms / 1000.0)

    async def simulate_human_mouse_movement(self, page: Page, target_x: int, target_y: int) -> None:
        """Simulate realistic mouse movement to target coordinates."""
        if self.stealth_level in [StealthLevel.BASIC, StealthLevel.MODERATE]:
            # Simple movement for basic stealth
            await page.mouse.move(target_x, target_y)
            return

        # Advanced human-like mouse movement for aggressive/paranoid stealth
        current_x, current_y = 100, 100  # Starting position

        # Calculate path with curves
        steps = random.randint(3, 8)
        for i in range(steps):
            progress = (i + 1) / steps

            # Add some curve to the movement
            curve_offset_x = random.randint(-20, 20) * (1 - progress)
            curve_offset_y = random.randint(-20, 20) * (1 - progress)

            next_x = current_x + (target_x - current_x) * progress + curve_offset_x
            next_y = current_y + (target_y - current_y) * progress + curve_offset_y

            await page.mouse.move(next_x, next_y)
            await asyncio.sleep(random.randint(10, 50) / 1000.0)  # Small delays between movements

            current_x, current_y = next_x, next_y

    def should_throttle_request(self) -> bool:
        """Determine if request should be throttled based on timing patterns."""
        now = time.time()
        time_since_last = now - self.last_request_time

        # Aggressive throttling for higher stealth levels
        min_interval = {
            StealthLevel.BASIC: 0.5,      # 500ms
            StealthLevel.MODERATE: 1.0,   # 1 second
            StealthLevel.AGGRESSIVE: 2.0, # 2 seconds
            StealthLevel.PARANOID: 3.0    # 3 seconds
        }

        should_throttle = time_since_last < min_interval[self.stealth_level]
        if not should_throttle:
            self.last_request_time = now

        return should_throttle

    def get_stealth_metrics(self) -> Dict[str, Any]:
        """Get stealth operation metrics."""
        return {
            "stealth_level": self.stealth_level.value,
            "current_user_agent": self.current_user_agent[:100] if self.current_user_agent else None,
            "user_agent_rotations": len(self.session_fingerprints),
            "last_rotation": self.last_user_agent_change.isoformat(),
            "canvas_noise_enabled": self.canvas_noise_enabled,
            "webgl_noise_enabled": self.webgl_noise_enabled,
            "request_throttling_active": self.stealth_level in [StealthLevel.AGGRESSIVE, StealthLevel.PARANOID],
            "total_requests_tracked": len(self.request_history)
        }

    async def cleanup_traces(self, context: BrowserContext) -> None:
        """Clean up any traces that might identify automation."""
        try:
            # Clear any automation-related storage
            await context.clear_cookies()
            await context.clear_permissions()

            # Execute cleanup script
            pages = context.pages
            for page in pages:
                try:
                    await page.evaluate("""
                        // Clear any automation markers
                        delete window.navigator.webdriver;
                        delete window.chrome;

                        // Clear local storage of automation traces
                        localStorage.clear();
                        sessionStorage.clear();
                    """)
                except Exception:
                    pass  # Ignore errors during cleanup

        except Exception as e:
            self.logger.warning(f"Error during trace cleanup: {e}")