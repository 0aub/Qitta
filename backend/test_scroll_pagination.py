#!/usr/bin/env python3
"""
Test if Booking.com uses scroll-based pagination
"""

import asyncio
from playwright.async_api import async_playwright

async def test_scroll_pagination():
    print("🔍 TESTING SCROLL-BASED PAGINATION...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.booking.com/hotel/ae/atlantis-the-palm.html"
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(3000)
        
        # Navigate to reviews
        reviews_btn = page.locator("[data-testid*='reviews']:has-text('all')").first
        await reviews_btn.click()
        await page.wait_for_timeout(5000)
        
        # Count initial reviews
        initial_count = await page.locator("[data-testid*='review']").count()
        print(f"📊 Initial reviews: {initial_count}")
        
        # TEST: Scroll down to trigger more reviews
        print("🔄 Scrolling to trigger more reviews...")
        
        for scroll_attempt in range(5):
            # Scroll to bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(3000)
            
            # Count reviews again
            new_count = await page.locator("[data-testid*='review']").count()
            print(f"   Scroll {scroll_attempt + 1}: {new_count} reviews")
            
            if new_count > initial_count:
                print(f"✅ SUCCESS! Reviews increased from {initial_count} to {new_count}")
                break
            
            # Try scrolling to specific review area
            try:
                last_review = page.locator("[data-testid*='review']").last
                await last_review.scroll_into_view_if_needed()
                await page.wait_for_timeout(2000)
                
                newer_count = await page.locator("[data-testid*='review']").count()
                if newer_count > new_count:
                    print(f"✅ SCROLL SUCCESS! Reviews: {newer_count}")
                    break
            except:
                pass
        
        final_count = await page.locator("[data-testid*='review']").count()
        
        # Check the review count shown on the page vs what we extracted
        try:
            review_count_element = page.locator(":has-text('Guest reviews')").first
            review_count_text = await review_count_element.inner_text()
            print(f"📊 Page claims: {review_count_text}")
            print(f"📊 We extracted: {final_count} reviews")
        except:
            print("📊 Could not find review count on page")
        
        await browser.close()
        
        return {
            'initial': initial_count,
            'final': final_count,
            'increased': final_count > initial_count
        }

if __name__ == "__main__":
    result = asyncio.run(test_scroll_pagination())
    print(f"🎯 RESULT: {result}")