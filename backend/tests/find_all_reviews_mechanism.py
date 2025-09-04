#!/usr/bin/env python3
"""
Find the mechanism to access ALL 20,620 reviews
"""

import asyncio
from playwright.async_api import async_playwright

async def find_all_reviews_access():
    print("🔍 FINDING MECHANISM TO ACCESS ALL 20,620 REVIEWS...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.booking.com/hotel/ae/atlantis-the-palm.html"
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(3000)
        
        # Look for any element that mentions the total review count
        print("🔍 Looking for elements that mention 20,620 reviews...")
        
        # Search for text containing "20,620" or similar large numbers
        review_count_patterns = [
            ":has-text('20,620')",
            ":has-text('20620')", 
            ":has-text('Guest reviews')",
            ":has-text('all reviews')",
            ":has-text('View all')",
            ":has-text('See all')",
            ":has-text('Read all')"
        ]
        
        clickable_elements = []
        
        for pattern in review_count_patterns:
            try:
                elements = page.locator(pattern)
                count = await elements.count()
                
                for i in range(count):
                    element = elements.nth(i)
                    if await element.is_visible():
                        text = await element.inner_text()
                        tag_name = await element.evaluate("el => el.tagName")
                        
                        # Check if it's clickable (link or button)
                        is_clickable = tag_name.lower() in ['a', 'button'] or await element.get_attribute('onclick') is not None
                        
                        print(f"📍 Found: '{text[:100]}...' - Tag: {tag_name} - Clickable: {is_clickable}")
                        
                        if is_clickable and ('20,620' in text or 'all reviews' in text.lower() or 'read all' in text.lower()):
                            clickable_elements.append((element, text, pattern))
                            
            except Exception as e:
                print(f"Error with pattern {pattern}: {e}")
                continue
        
        print(f"\n📊 Found {len(clickable_elements)} potentially clickable review elements")
        
        # TEST clicking the most promising element
        if clickable_elements:
            best_element, best_text, best_pattern = clickable_elements[0]
            print(f"\n🧪 TESTING CLICK: '{best_text[:50]}...' ({best_pattern})")
            
            # Count initial reviews
            initial_reviews = await page.locator("[data-testid*='review']").count()
            print(f"📊 Reviews before click: {initial_reviews}")
            
            try:
                await best_element.click()
                await page.wait_for_timeout(5000)
                
                # Count reviews after click
                new_reviews = await page.locator("[data-testid*='review']").count()
                print(f"📊 Reviews after click: {new_reviews}")
                
                if new_reviews > initial_reviews:
                    print(f"✅ SUCCESS! Reviews increased from {initial_reviews} to {new_reviews}")
                    
                    # Check if we can access even more reviews
                    print("🔄 Looking for pagination after navigation...")
                    
                    # Look for pagination elements on the new page
                    pagination_patterns = [
                        "button:has-text('Next')",
                        "a:has-text('Next')",
                        "button:has-text('Show more')",
                        "a:has-text('Load more')",
                        "[class*='pagination']",
                        "[data-testid*='pagination']"
                    ]
                    
                    found_pagination = False
                    for pattern in pagination_patterns:
                        try:
                            elements = page.locator(pattern)
                            count = await elements.count()
                            if count > 0:
                                print(f"✅ Found pagination: {pattern} ({count} elements)")
                                found_pagination = True
                        except:
                            continue
                    
                    if not found_pagination:
                        print("⚠️ No pagination found on reviews page")
                        
                else:
                    print(f"❌ No change in review count")
                    
            except Exception as e:
                print(f"❌ Click failed: {e}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(find_all_reviews_access())