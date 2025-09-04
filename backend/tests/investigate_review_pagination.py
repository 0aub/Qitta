#!/usr/bin/env python3
"""
Focused Review Pagination Investigation
"""

import asyncio
from playwright.async_api import async_playwright

async def find_review_pagination():
    print("🔍 FOCUSED REVIEW PAGINATION INVESTIGATION...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Test Atlantis (known to have many reviews)
        url = "https://www.booking.com/hotel/ae/atlantis-the-palm.html"
        print(f"🏨 Testing: {url}")
        
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(3000)
        
        # Navigate to reviews section
        reviews_btn = page.locator("[data-testid*='reviews']:has-text('all')").first
        await reviews_btn.click()
        await page.wait_for_timeout(5000)
        
        # Count reviews and look at page structure
        initial_reviews = await page.locator("[data-testid*='review']").count()
        print(f"📊 Reviews visible: {initial_reviews}")
        
        # STRATEGY: Look specifically in the reviews section for pagination
        print("\n🔍 ANALYZING REVIEW SECTION STRUCTURE...")
        
        # Find the reviews container
        reviews_container_selectors = [
            "#reviewCardsSection",
            "[data-testid*='review-list']",
            "[class*='review']:has([data-testid*='review'])",
            "div:has([data-testid*='review'])"
        ]
        
        reviews_container = None
        for selector in reviews_container_selectors:
            try:
                container = page.locator(selector).first
                if await container.is_visible():
                    reviews_container = container
                    print(f"✅ Found reviews container: {selector}")
                    break
            except:
                continue
        
        if not reviews_container:
            reviews_container = page  # Use whole page as fallback
            print("⚠️ Using whole page as reviews container")
        
        # Look for pagination WITHIN or NEAR the reviews container
        pagination_patterns = [
            # Review-specific pagination
            "button:has-text('Show more reviews')",
            "a:has-text('Show more reviews')",
            "button:has-text('Load more')",
            "a:has-text('Load more')", 
            
            # Generic but in reviews area
            "button:has-text('Show more')",
            "button:has-text('Next')",
            "a:has-text('Next')",
            
            # Pagination controls near reviews
            "[class*='pagination']",
            "[data-testid*='pagination']",
            "[class*='page-nav']",
            
            # Button/link that might load more
            "[onclick*='more']",
            "[onclick*='load']"
        ]
        
        working_pagination = []
        
        for pattern in pagination_patterns:
            try:
                elements = reviews_container.locator(pattern)
                count = await elements.count()
                
                if count > 0:
                    for i in range(count):
                        element = elements.nth(i)
                        if await element.is_visible() and await element.is_enabled():
                            text = await element.inner_text()
                            # Filter out obviously wrong buttons
                            if 'photo' not in text.lower() and 'image' not in text.lower():
                                print(f"✅ POTENTIAL: {pattern} - '{text}'")
                                working_pagination.append((pattern, text))
            except:
                continue
        
        # ALTERNATIVE APPROACH: Look for ANY button/link that might be pagination
        print("\n🔍 SEARCHING FOR ANY CLICKABLE ELEMENTS THAT MIGHT LOAD MORE...")
        
        # Get all buttons and links in the page
        all_buttons = await page.locator("button, a").count()
        print(f"📊 Total buttons/links found: {all_buttons}")
        
        # Look for buttons with text that might indicate "more content"
        more_keywords = ['more', 'next', 'load', 'show', 'all', 'view']
        
        for i in range(min(all_buttons, 50)):  # Check first 50 elements
            try:
                element = page.locator("button, a").nth(i)
                if await element.is_visible() and await element.is_enabled():
                    text = await element.inner_text()
                    text_lower = text.lower().strip()
                    
                    # Check if text contains pagination keywords
                    if any(keyword in text_lower for keyword in more_keywords):
                        # Skip obvious non-pagination elements
                        if not any(skip in text_lower for skip in ['photo', 'image', 'gallery', 'book', 'reserve']):
                            print(f"🎯 CANDIDATE: '{text}' (element {i})")
            except:
                continue
        
        print(f"\n📊 SUMMARY:")
        print(f"   Reviews visible: {initial_reviews}")
        print(f"   Potential pagination elements: {len(working_pagination)}")
        
        await browser.close()
        return working_pagination

if __name__ == "__main__":
    results = asyncio.run(find_review_pagination())
    print(f"🎯 INVESTIGATION COMPLETE")