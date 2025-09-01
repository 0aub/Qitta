#!/usr/bin/env python3
"""
Multi-page Hotel Investigation - Test with popular hotels that have many reviews
"""

import asyncio
from playwright.async_api import async_playwright

async def investigate_multi_page_hotel():
    print("🔍 Investigating multi-page hotel reviews...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Test popular Dubai hotels with many reviews
        hotels = [
            "https://www.booking.com/hotel/ae/atlantis-the-palm.html",  # Atlantis - very popular
            "https://www.booking.com/hotel/ae/burj-al-arab-jumeirah.html",  # Burj Al Arab - very popular  
            "https://www.booking.com/hotel/ae/armani-hotel-dubai.html"  # Armani - popular
        ]
        
        for hotel_url in hotels:
            print(f"\n📍 Testing: {hotel_url}")
            
            try:
                await page.goto(hotel_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Get hotel name
                try:
                    hotel_name = await page.locator("h2[data-testid='header-title']").first.inner_text()
                    print(f"🏨 Hotel: {hotel_name}")
                except:
                    print("🏨 Hotel: Unknown")
                
                # Click "Read all reviews"  
                try:
                    reviews_button = page.locator("[data-testid*='reviews']:has-text('all')").first
                    if await reviews_button.is_visible():
                        print("✅ Clicking 'Read all reviews'...")
                        await reviews_button.click()
                        await page.wait_for_timeout(5000)
                    else:
                        print("⚠️ Reviews button not found, trying alternatives...")
                        # Try alternative review navigation
                        alt_buttons = [
                            "a:has-text('Reviews')",
                            "button:has-text('Reviews')",
                            "[href*='reviews']"
                        ]
                        clicked = False
                        for alt_selector in alt_buttons:
                            try:
                                alt_button = page.locator(alt_selector).first
                                if await alt_button.is_visible():
                                    await alt_button.click()
                                    await page.wait_for_timeout(5000)
                                    clicked = True
                                    print(f"✅ Clicked alternative: {alt_selector}")
                                    break
                            except:
                                continue
                        if not clicked:
                            print("❌ Could not navigate to reviews")
                            continue
                            
                except Exception as e:
                    print(f"⚠️ Navigation error: {e}")
                    continue
                
                # Count reviews
                review_count = await page.locator("[data-testid*='review']").count()
                print(f"📊 Found {review_count} review elements on page")
                
                # Check for pagination
                pagination_selectors = [
                    "[data-testid*='next']",
                    "[aria-label*='Next']",
                    "button:has-text('Next')",
                    "a:has-text('Next')",
                    "button:has-text('Show more')",
                    "[class*='pagination']"
                ]
                
                pagination_found = False
                for selector in pagination_selectors:
                    try:
                        elements = await page.locator(selector).count()
                        if elements > 0:
                            first = page.locator(selector).first
                            is_visible = await first.is_visible()
                            is_enabled = await first.is_enabled()
                            text = await first.inner_text()
                            
                            if is_visible and is_enabled:
                                print(f"✅ VISIBLE pagination: {selector} - Text: '{text}'")
                                pagination_found = True
                            else:
                                print(f"⚠️ Hidden pagination: {selector} - Visible: {is_visible}, Enabled: {is_enabled}")
                    except:
                        continue
                
                if not pagination_found:
                    print("❌ No visible pagination found")
                else:
                    print("✅ Pagination available!")
                
            except Exception as e:
                print(f"❌ Error testing {hotel_url}: {e}")
                continue
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(investigate_multi_page_hotel())