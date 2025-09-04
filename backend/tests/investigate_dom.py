#!/usr/bin/env python3
"""
DOM Investigation Script - Find current review selectors
"""

import asyncio
from playwright.async_api import async_playwright

async def investigate_review_dom():
    print("🔍 Investigating Booking.com DOM structure...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Navigate to a known hotel
        url = "https://www.booking.com/hotel/ae/al-bateen-resdences-by-happy-season-jbr.html"
        print(f"📍 Navigating to: {url}")
        
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(5000)
        
        print("\n🔍 SEARCHING FOR REVIEW ELEMENTS:")
        
        # Test various review selectors
        selectors_to_test = [
            # Original selectors that worked
            "[data-testid='review-card']",
            "[data-testid='review-positive-text']", 
            "[data-testid='review-negative-text']",
            "#reviewCardsSection",
            
            # Common review patterns
            "[data-testid*='review']",
            "[class*='review']",
            "[id*='review']",
            ".review",
            ".Review",
            
            # Booking.com specific patterns
            "[data-testid*='guest']",
            "[data-testid*='comment']",
            "[data-testid*='feedback']",
            ".bui-review",
            ".c-review",
            
            # Generic content patterns
            "[role='article']",
            "article",
            ".user-comment",
            ".guest-review"
        ]
        
        found_selectors = []
        
        for selector in selectors_to_test:
            try:
                elements = await page.locator(selector).count()
                if elements > 0:
                    print(f"✅ {selector}: {elements} elements")
                    found_selectors.append((selector, elements))
                    
                    # Get sample text from first element
                    if elements > 0:
                        try:
                            first_text = await page.locator(selector).first.inner_text()
                            if first_text and len(first_text.strip()) > 10:
                                print(f"   Sample text: '{first_text[:100]}...'")
                        except:
                            pass
                else:
                    print(f"❌ {selector}: 0 elements")
            except Exception as e:
                print(f"⚠️  {selector}: ERROR - {e}")
        
        print(f"\n📊 FOUND {len(found_selectors)} working selectors")
        
        # Check for review navigation buttons
        print("\n🔍 SEARCHING FOR REVIEW NAVIGATION:")
        button_selectors = [
            "button:has-text('Reviews')",
            "a:has-text('Reviews')", 
            "button:has-text('See all')",
            "button:has-text('Show more')",
            "[data-testid*='reviews']:has-text('all')",
            "[data-testid*='review']:has-text('more')"
        ]
        
        for selector in button_selectors:
            try:
                count = await page.locator(selector).count()
                if count > 0:
                    text = await page.locator(selector).first.inner_text()
                    print(f"✅ Button '{selector}': {count} found - Text: '{text}'")
                else:
                    print(f"❌ Button '{selector}': 0 found")
            except Exception as e:
                print(f"⚠️  Button '{selector}': ERROR - {e}")
        
        await browser.close()
        
        return found_selectors

if __name__ == "__main__":
    found = asyncio.run(investigate_review_dom())
    print(f"\n🎯 INVESTIGATION COMPLETE - {len(found)} working selectors found")