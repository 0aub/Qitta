from __future__ import annotations

import logging
from typing import Optional, List
from playwright.async_api import async_playwright, Browser

# Conditional import for stealth (may require psutil)
try:
    from .reliability.stealth import StealthManager, StealthLevel
    STEALTH_AVAILABLE = True
except ImportError:
    # Create placeholder classes when stealth is not available
    class StealthManager:
        def __init__(self, *args, **kwargs):
            pass
        async def apply_stealth_to_context(self, *args, **kwargs):
            pass
        async def cleanup_traces(self, *args, **kwargs):
            pass

    class StealthLevel:
        BASIC = "basic"
        MODERATE = "moderate"
        AGGRESSIVE = "aggressive"
        PARANOID = "paranoid"

    STEALTH_AVAILABLE = False


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

        # Phase 6.5: EMERGENCY CONTAINER ENVIRONMENT ISOLATION
        import os
        os.environ['DBUS_SESSION_BUS_ADDRESS'] = ''  # Disable session bus
        os.environ['DBUS_SYSTEM_BUS_ADDRESS'] = ''   # Disable system bus
        os.environ['XDG_RUNTIME_DIR'] = '/tmp'       # Redirect runtime to /tmp
        os.environ['DISPLAY'] = ':99'                # Virtual display

        # EMERGENCY: Complete system isolation for containers
        os.environ['HOME'] = '/tmp'                  # Override home directory
        os.environ['USER'] = 'container'             # Set container user
        os.environ['LOGNAME'] = 'container'          # Set log name
        os.environ['TMPDIR'] = '/tmp'                # Ensure temp directory
        os.environ['TMP'] = '/tmp'                   # Additional temp
        os.environ['TEMP'] = '/tmp'                  # Windows compatibility

        # Disable problematic system services
        os.environ['PULSE_RUNTIME_PATH'] = '/tmp'    # Disable PulseAudio
        os.environ['PULSE_SOCKET'] = 'disabled'      # Disable audio
        os.environ['ALSA_CARD'] = 'null'            # Null audio card
        os.environ['SDL_AUDIODRIVER'] = 'dummy'      # Dummy audio driver

        # Phase 4.3: Enhanced stealth browser launch arguments
        stealth_args = [
            # CRITICAL container compatibility flags (MUST HAVE for Docker)
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-gpu-sandbox',
            '--disable-software-rasterizer',
            '--remote-debugging-port=9222',
            '--disable-features=VizDisplayCompositor',
            '--disable-ipc-flooding-protection',
            '--single-process',  # Critical for container resource constraints
            '--no-zygote',       # Prevents process spawning issues in containers
            '--memory-pressure-off',     # Disable memory pressure detection
            '--disable-dbus',            # Disable D-Bus in container environment

            # EMERGENCY CONTAINER D-BUS FIXES (Phase 6.2)
            '--disable-features=DBus',
            '--disable-features=DesktopPWAs',
            '--disable-features=MediaRouter',
            '--disable-features=NetworkService',
            '--disable-features=AudioServiceSandbox',
            '--disable-features=VizServiceDBus',
            '--disable-features=SystemNotifications',
            '--disable-logging',
            '--disable-logging-redirect',
            '--log-level=3',  # Suppress error logs
            '--quiet',
            '--silent',
            '--disable-in-process-stack-traces',

            # ULTIMATE D-BUS ELIMINATION (Phase 6.3)
            '--no-dbus',                     # Explicitly disable D-Bus
            '--disable-system-font-check',   # No system font access
            '--disable-desktop-notifications',
            '--disable-device-discovery-notifications',
            '--disable-system-notifications',
            '--disable-chrome-tracing',
            '--disable-crash-reporter',
            '--disable-client-side-phishing-detection',
            '--disable-component-extensions-with-background-pages',
            '--disable-default-component-extension',
            '--disable-extensions-file-access-check',
            '--disable-print-preview',
            '--disable-tab-for-desktop-share',
            '--disable-desktop-capture',
            '--disable-desktop-capture-picker-new-ui',
            '--disable-device-discovery-notifications',
            '--disable-gesture-typing',
            '--disable-speech-synthesis-api',
            '--disable-file-system',
            '--disable-permissions-api',
            '--disable-presentation-api',
            '--disable-remote-fonts',
            '--disable-shared-worker',
            '--disable-speech-api',
            '--disable-web-bluetooth',
            '--disable-webgl',
            '--disable-webgl2',
            '--disable-webrtc',
            '--disable-webusb',
            '--disable-wake-on-wifi',
            '--enable-automation',  # Accept automation
            '--force-device-scale-factor=1',
            '--hide-crash-restore-bubble',
            '--no-service-autorun',
            '--disable-hang-monitor',
            '--disable-prompt-on-repost',
            '--disable-web-security',
            '--allow-running-insecure-content',
            '--disable-add-to-shelf',
            '--disable-background-timer-throttling',
            '--disable-renderer-backgrounding',
            '--disable-backgrounding-occluded-windows',
            '--disable-features=TranslateUI',
            '--disable-ipc-flooding-protection',

            # AGGRESSIVE CONTAINER ISOLATION
            '--disable-namespace-sandbox',
            '--disable-seccomp-filter-sandbox',
            '--allow-no-sandbox-job',
            '--disable-gpu-process-crash-limit',
            '--disable-renderer-accessibility',
            '--disable-accessibility-events',
            '--disable-speech-api',
            '--disable-notifications',
            '--disable-device-discovery-notifications',
            '--disable-background-mode',
            '--disable-background-downloads',
            '--disable-permission-action-reporting',

            # ADDITIONAL critical container flags to prevent Chromium timeout
            '--disable-features=VizDisplayCompositor,VizHitTestSurfaceLayer',
            '--disable-threaded-compositing',
            '--disable-threaded-animation',
            '--disable-checker-imaging',
            '--disable-new-content-rendering-timeout',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--disable-field-trial-config',
            '--disable-background-timer-throttling',
            '--disable-ipc-flooding-protection',
            '--max_old_space_size=4096',
            '--font-render-hinting=none',
            '--disable-skia-runtime-opts',
            '--disable-system-font-check',
            '--disable-features=AudioServiceOutOfProcess',
            '--disable-rtc-smoothness-algorithm',
            '--disable-features=WebRtcHideLocalIpsWithMdns',
            '--disable-features=ScriptStreaming',
            '--js-flags=--max-old-space-size=4096',
            '--disable-domain-reliability',
            '--disable-component-extensions-with-background-pages',
            '--disable-background-networking',
            '--disable-breakpad',
            '--disable-features=TranslateUI,BlinkGenPropertyTrees',
            '--run-all-compositor-stages-before-draw',
            '--disable-new-content-rendering-timeout',
            '--disable-threaded-scrolling',
            '--disable-partial-raster',
            '--disable-features=VizHitTestSurfaceLayer',
            '--disable-surfaces-synchronization',
            '--use-gl=disabled',
            '--disable-gl-drawing-for-tests',

            # EMERGENCY PHASE 6.5: FINAL CONTAINER ISOLATION
            '--disable-features=VizServiceDBus,NetworkService,AudioServiceSandbox',
            '--no-service-autorun',
            '--disable-crash-reporter',
            '--disable-breakpad',
            '--disable-device-discovery-notifications',
            '--disable-local-storage',
            '--disable-session-storage',
            '--disable-databases',
            '--disable-file-system',
            '--disable-notifications',
            '--disable-permission-action-reporting',
            '--disable-plugins-discovery',
            '--disable-reading-from-canvas',
            '--disable-remote-fonts',
            '--disable-renderer-accessibility',
            '--disable-speech-api',
            '--disable-speech-synthesis-api',
            '--disable-webgl',
            '--disable-webgl2',
            '--force-webrtc-ip-handling-policy=disable_non_proxied_udp',
            '--no-experiments',
            '--no-referrers',
            '--no-crash-upload',
            '--no-report-upload',
            '--no-first-run',
            '--aggressive',
            '--disable-default-apps',
            '--disable-extensions-http-throttling',
            '--disable-logging-redirect',
            '--disable-bundled-ppapi-flash',
            '--disable-pepper-3d',
            '--disable-pepper-3d-image-chromium',

            # Basic anti-detection (Phase 4.1)
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=Translate',

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
            '--disable-client-side-phishing-detection',
            '--disable-default-apps',
            '--disable-hang-monitor',
            '--disable-popup-blocking',
            '--disable-prompt-on-repost',
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
