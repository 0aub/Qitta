#!/usr/bin/env python3
"""
Quick debug script to test Twitter extraction
"""

import asyncio
import json
from playwright.async_api import async_playwright

async def test_twitter_extraction():
    """Test basic Twitter page loading and element detection."""

    print("🔍 TESTING TWITTER EXTRACTION")
    print("=" * 40)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        try:
            # Test navigation to naval's profile
            test_url = "https://x.com/naval"
            print(f"🌐 Navigating to: {test_url}")

            await page.goto(test_url, wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(5000)

            print(f"✅ Page loaded: {page.url}")

            # Test basic element detection
            div_count = await page.locator('div').count()
            print(f"📊 Total divs found: {div_count}")

            # Test for auth requirement
            auth_selectors = [
                'text=Sign in',
                'text=Log in',
                'text=Login',
                'text=Create account',
                '[data-testid="loginButton"]'
            ]

            auth_required = False
            for selector in auth_selectors:
                count = await page.locator(selector).count()
                if count > 0:
                    print(f"🔒 Auth required - found: {selector}")
                    auth_required = True
                    break

            if not auth_required:
                print("✅ No auth wall detected")

            # Test tweet selectors
            tweet_selectors = [
                'article[data-testid="tweet"]',
                'div[data-testid="tweet"]',
                '[data-testid="tweetWrapperOuter"]',
                '[data-testid="cellInnerDiv"]'
            ]

            print("\n📝 TWEET SELECTOR TESTS:")
            for selector in tweet_selectors:
                count = await page.locator(selector).count()
                print(f"  {selector}: {count} elements")

                if count > 0:
                    # Try to extract text from first element
                    try:
                        first_element = page.locator(selector).first
                        text = await first_element.inner_text()
                        print(f"    First element text: {text[:100]}...")
                    except Exception as e:
                        print(f"    Error extracting text: {e}")

            # Test profile selectors
            print("\n👤 PROFILE SELECTOR TESTS:")
            profile_selectors = [
                'span:has-text("Joined")',
                'span:has-text("Following")',
                'span:has-text("Followers")'
            ]

            for selector in profile_selectors:
                count = await page.locator(selector).count()
                print(f"  {selector}: {count} elements")

            # Check for protected account
            protected_selectors = [
                'text="This account\'s posts are protected"',
                'text="These posts are protected"',
                'text="This account is private"'
            ]

            for selector in protected_selectors:
                count = await page.locator(selector).count()
                if count > 0:
                    print(f"🔒 Protected account detected: {selector}")

            print("\n🎯 EXTRACTION TEST COMPLETE")

        except Exception as e:
            print(f"❌ Test failed: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_twitter_extraction())