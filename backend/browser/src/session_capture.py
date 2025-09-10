"""
Twitter Session Capture Module
==============================

Integrated session capture functionality for the browser container.
Handles user-interactive authentication and session storage.
"""

import json
import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from playwright.async_api import Browser

class TwitterSessionCapture:
    """Handles Twitter session capture with user interaction."""
    
    SESSIONS_DIR = Path("/sessions")
    
    def __init__(self):
        self.SESSIONS_DIR.mkdir(exist_ok=True)
        self.logger = logging.getLogger("browser.session_capture")
    
    async def capture_session(self, browser: Browser) -> Dict[str, Any]:
        """
        Alternative session capture approaches to bypass Twitter's anti-bot protection.
        """
        self.logger.info("🔐 Starting Twitter session capture with alternative methods...")
        
        # Try multiple approaches
        approaches = [
            self._approach_direct_home_access,
            self._approach_oauth_flow,
            self._approach_persistent_browser,
        ]
        
        for i, approach in enumerate(approaches, 1):
            try:
                self.logger.info(f"🔄 Trying approach {i}/{len(approaches)}: {approach.__name__}")
                result = await approach(browser)
                if result:
                    return result
            except Exception as e:
                self.logger.warning(f"❌ Approach {i} failed: {e}")
                continue
        
        # If all approaches fail, fall back to manual instructions
        return await self._provide_manual_instructions()
    
    async def _approach_direct_home_access(self, browser: Browser) -> Dict[str, Any]:
        """Try accessing Twitter home directly with our credentials."""
        self.logger.info("🏠 Attempting direct home access with credentials...")
        
        context = None
        try:
            # Create a normal browser context
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            )
            
            page = await context.new_page()
            
            # Try going to home page first
            self.logger.info("🌐 Accessing Twitter home page...")
            await page.goto("https://twitter.com/home", wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(5)
            
            # If we're redirected to login, try automated login
            if 'login' in page.url or 'session' in page.url:
                self.logger.info("🔑 Detected login redirect, attempting automated login...")
                return await self._attempt_automated_login(page)
            else:
                self.logger.info("✅ Already logged in! Capturing existing session...")
                return await self._capture_existing_session(context)
                
        finally:
            if context:
                await context.close()
    
    async def _approach_oauth_flow(self, browser: Browser) -> Dict[str, Any]:
        """Try using Twitter's OAuth flow which might be less protected."""
        self.logger.info("🔐 Attempting OAuth flow...")
        
        context = None
        try:
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            )
            
            page = await context.new_page()
            
            # Try OAuth login URL
            oauth_url = "https://twitter.com/i/oauth2/authorize?response_type=code&client_id=TwitterAndroid&redirect_uri=twitterauth://&scope=tweet.read%20users.read%20offline.access&state=state&code_challenge=challenge&code_challenge_method=plain"
            
            self.logger.info("🌐 Accessing OAuth endpoint...")
            await page.goto(oauth_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)
            
            # Check if we get a login form
            inputs = await page.locator('input:not([type="hidden"])').count()
            if inputs > 0:
                self.logger.info(f"✅ OAuth login form found with {inputs} inputs")
                return await self._attempt_automated_login(page)
            else:
                raise Exception("OAuth flow also blocked")
                
        finally:
            if context:
                await context.close()
    
    async def _approach_persistent_browser(self, browser: Browser) -> Dict[str, Any]:
        """Keep browser open for extended time for manual login."""
        self.logger.info("🕰️  Opening persistent browser for manual login...")
        
        context = None
        try:
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            )
            
            page = await context.new_page()
            
            self.logger.info("🌐 Opening Twitter for manual login...")
            await page.goto("https://twitter.com", wait_until='domcontentloaded', timeout=30000)
            
            self.logger.info("👤 PLEASE MANUALLY LOGIN TO TWITTER IN THE BROWSER WINDOW")
            self.logger.info("⏳ Browser will stay open for 10 minutes...")
            self.logger.info("🍪 Once logged in, your session will be automatically captured")
            
            # Wait up to 10 minutes for login
            await self._wait_for_login_success(page, timeout_minutes=10)
            
            # Capture the session
            return await self._capture_existing_session(context)
                
        finally:
            if context:
                await context.close()
    
    async def _attempt_automated_login(self, page) -> Dict[str, Any]:
        """Attempt automated login with provided credentials."""
        import os
        
        try:
            email = os.environ.get('X_EMAIL')
            username = os.environ.get('X_USERNAME') 
            password = os.environ.get('X_PASS')
            
            if not all([email, username, password]):
                raise Exception("Missing Twitter credentials in environment")
            
            self.logger.info("🔑 Attempting automated login...")
            
            # Look for email/username field
            await asyncio.sleep(2)
            email_input = page.locator('input[name="text"], input[type="email"], input[autocomplete="username"]').first
            if await email_input.count() > 0:
                await email_input.fill(email)
                await asyncio.sleep(1)
                
                # Click next/continue
                next_btn = page.locator('button:has-text("Next"), button:has-text("Continue"), [role="button"]:has-text("Next")').first
                if await next_btn.count() > 0:
                    await next_btn.click()
                    await asyncio.sleep(3)
                
                # Password field
                password_input = page.locator('input[name="password"], input[type="password"]').first
                if await password_input.count() > 0:
                    await password_input.fill(password)
                    await asyncio.sleep(1)
                    
                    # Login button
                    login_btn = page.locator('button:has-text("Log in"), button:has-text("Sign in"), [role="button"]:has-text("Log in")').first
                    if await login_btn.count() > 0:
                        await login_btn.click()
                        await asyncio.sleep(5)
                        
                        # Check if login was successful
                        if any(indicator in page.url for indicator in ['/home', '/timeline']) or 'login' not in page.url:
                            self.logger.info("✅ Automated login successful!")
                            return await self._capture_existing_session(page.context)
                        else:
                            raise Exception("Login form filled but login unsuccessful")
                    else:
                        raise Exception("Could not find login button")
                else:
                    raise Exception("Could not find password field")
            else:
                raise Exception("Could not find email/username field")
                
        except Exception as e:
            self.logger.warning(f"Automated login failed: {e}")
            raise
    
    async def _capture_existing_session(self, context) -> Dict[str, Any]:
        """Capture session from already logged-in browser context."""
        self.logger.info("🍪 Capturing session cookies...")
        cookies = await context.cookies()
        
        # Filter important Twitter cookies
        important_cookies = {}
        cookie_names = ['auth_token', 'ct0', '_twitter_sess', 'twid', 'att']
        
        for cookie in cookies:
            if cookie['name'] in cookie_names:
                important_cookies[cookie['name']] = cookie['value']
        
        if not important_cookies.get('auth_token'):
            raise Exception("No auth_token found - login may not have completed")
        
        # Save session to file
        session_data = {
            "cookies": important_cookies,
            "captured_at": datetime.now().isoformat(),
            "expires_estimate": (datetime.now().timestamp() + (30 * 24 * 60 * 60))  # ~30 days
        }
        
        session_file = self.SESSIONS_DIR / "twitter_session.json"
        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=2)
        
        self.logger.info(f"✅ Session saved to {session_file}")
        
        return {
            "status": "success",
            "message": "Session captured successfully",
            "cookies_captured": len(important_cookies),
            "session_file": str(session_file),
            "expires_estimate": session_data["expires_estimate"]
        }
    
    async def _provide_manual_instructions(self) -> Dict[str, Any]:
        """Provide manual session export instructions as fallback."""
        instructions = """
        MANUAL SESSION EXPORT INSTRUCTIONS:
        
        1. Open Twitter in your regular browser
        2. Login normally
        3. Press F12 to open Developer Tools
        4. Go to Application > Storage > Cookies > https://twitter.com
        5. Find these cookies and copy their values:
           - auth_token
           - ct0  
           - _twitter_sess
           - twid
           - att
        6. Create file: /sessions/twitter_session.json
        7. Use this format:
        {
          "cookies": {
            "auth_token": "your_auth_token_value",
            "ct0": "your_ct0_value", 
            "_twitter_sess": "your_session_value",
            "twid": "your_twid_value",
            "att": "your_att_value"
          },
          "captured_at": "2025-09-05T23:00:00.000Z",
          "expires_estimate": 1725926400
        }
        """
        
        self.logger.info(instructions)
        
        return {
            "status": "manual_required",
            "message": "Automatic session capture blocked - manual export required",
            "instructions": instructions
        }
    
    async def _wait_for_login_success(self, page, timeout_minutes: int = 5):
        """Wait for user to complete login process."""
        timeout_seconds = timeout_minutes * 60
        start_time = asyncio.get_event_loop().time()
        
        while True:
            current_time = asyncio.get_event_loop().time()
            
            # Check timeout
            if current_time - start_time > timeout_seconds:
                raise Exception(f"Login timeout after {timeout_minutes} minutes")
            
            # Check current URL for successful login indicators
            current_url = page.url
            
            # Twitter login success indicators
            if any(indicator in current_url for indicator in ['/home', '/timeline']):
                # Additional check: look for elements that appear after login
                try:
                    # Look for compose tweet button or user menu
                    compose_button = page.locator('[data-testid="SideNav_NewTweet_Button"], [aria-label*="Tweet"], [href="/compose/tweet"]')
                    user_menu = page.locator('[data-testid="AppTabBar_Profile_Link"], [data-testid="SideNav_AccountSwitcher_Button"]')
                    
                    if await compose_button.count() > 0 or await user_menu.count() > 0:
                        self.logger.info("✅ Login successful - Twitter interface detected")
                        return
                except:
                    pass
            
            # Wait before next check
            await asyncio.sleep(2)
    
    def list_sessions(self) -> Dict[str, Any]:
        """List available captured sessions."""
        sessions = []
        
        for session_file in self.SESSIONS_DIR.glob("*.json"):
            try:
                with open(session_file, 'r') as f:
                    session_data = json.load(f)
                
                sessions.append({
                    "filename": session_file.name,
                    "captured_at": session_data.get("captured_at"),
                    "expires_estimate": session_data.get("expires_estimate"),
                    "cookies_count": len(session_data.get("cookies", {}))
                })
            except Exception as e:
                self.logger.warning(f"Could not read session file {session_file}: {e}")
        
        return {"sessions": sessions}
    
    def delete_session(self, filename: str) -> Dict[str, str]:
        """Delete a captured session."""
        session_file = self.SESSIONS_DIR / filename
        
        if not session_file.exists():
            raise ValueError("Session not found")
        
        try:
            session_file.unlink()
            return {"message": f"Session {filename} deleted successfully"}
        except Exception as e:
            raise Exception(f"Could not delete session: {str(e)}")

# Global instance
session_capture = TwitterSessionCapture()