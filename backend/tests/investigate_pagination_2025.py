#!/usr/bin/env python3
"""
Advanced Pagination Investigation - Find working pagination for 2025
"""

import asyncio
from playwright.async_api import async_playwright

async def investigate_pagination_thoroughly():
    print("🔍 ADVANCED PAGINATION INVESTIGATION...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Test with a hotel known to have MANY reviews (should have multiple pages)
        hotels_with_many_reviews = [
            "https://www.booking.com/hotel/ae/atlantis-the-palm.html",  # Atlantis - 20k+ reviews
            "https://www.booking.com/hotel/ae/burj-al-arab-jumeirah.html",  # Burj Al Arab - famous
        ]
        
        for hotel_url in hotels_with_many_reviews:
            print(f"\n🏨 TESTING: {hotel_url}")
            
            try:
                await page.goto(hotel_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Navigate to reviews section
                print("📍 Navigating to reviews section...")
                try:
                    reviews_btn = page.locator("[data-testid*='reviews']:has-text('all')").first
                    if await reviews_btn.is_visible():
                        await reviews_btn.click()
                        await page.wait_for_timeout(5000)
                        print("✅ Navigated to reviews section")
                    else:
                        print("⚠️ Could not find reviews navigation")
                        continue
                except:
                    print("❌ Failed to navigate to reviews")
                    continue
                
                # Count initial reviews
                initial_reviews = await page.locator("[data-testid*='review']").count()
                print(f"📊 Initial reviews visible: {initial_reviews}")
                
                # Test ALL possible pagination selectors
                pagination_selectors = [
                    # Modern Booking.com patterns
                    "button:has-text('Show more')",
                    "a:has-text('Show more')", 
                    "button:has-text('Next')",
                    "a:has-text('Next')",
                    "[data-testid*='pagination']:has-text('Next')",
                    "[data-testid*='next']",
                    "[aria-label*='Next']",
                    "[aria-label*='More']",
                    
                    # Load more patterns
                    "button:has-text('Load more')",
                    "a:has-text('Load more')",
                    "[data-testid*='load']:has-text('more')",
                    "[class*='load'][class*='more']",
                    
                    # Generic pagination
                    ".pagination-next",
                    "[class*='next'][class*='page']",
                    "[class*='pagination'] button",
                    "[class*='pagination'] a",
                    
                    # Show all patterns
                    "button:has-text('Show all')",
                    "a:has-text('View all')",
                    "[onclick*='more']",
                    "[onclick*='page']"
                ]
                
                working_selectors = []
                
                for selector in pagination_selectors:
                    try:
                        elements = page.locator(selector)
                        count = await elements.count()
                        
                        if count > 0:
                            for i in range(count):
                                element = elements.nth(i)
                                is_visible = await element.is_visible()
                                is_enabled = await element.is_enabled()
                                
                                if is_visible and is_enabled:
                                    text = await element.inner_text()
                                    print(f"✅ WORKING: {selector} - Text: '{text}' - Visible: {is_visible} - Enabled: {is_enabled}")
                                    working_selectors.append((selector, text))
                                else:
                                    text = await element.inner_text() if is_visible else "hidden"
                                    print(f"⚠️ FOUND BUT NOT USABLE: {selector} - Text: '{text}' - Visible: {is_visible} - Enabled: {is_enabled}")
                    except:
                        continue
                
                print(f"\n📊 SUMMARY FOR {hotel_url}:")
                print(f"   Initial reviews: {initial_reviews}")
                print(f"   Working pagination selectors: {len(working_selectors)}")
                
                if working_selectors:
                    print("   🎯 BEST SELECTORS TO USE:")
                    for selector, text in working_selectors[:3]:  # Top 3
                        print(f"      {selector} -> '{text}'")
                
                # TEST CLICKING THE BEST SELECTOR
                if working_selectors:
                    best_selector, best_text = working_selectors[0]
                    print(f"\n🧪 TESTING CLICK: {best_selector}")
                    
                    try:
                        await page.locator(best_selector).first.click()
                        await page.wait_for_timeout(3000)
                        
                        # Check if new reviews loaded
                        new_reviews = await page.locator("[data-testid*='review']").count()
                        if new_reviews > initial_reviews:
                            print(f"✅ SUCCESS! Reviews increased from {initial_reviews} to {new_reviews}")
                        else:
                            print(f"❌ No new reviews loaded. Still {new_reviews}")
                    except Exception as e:
                        print(f"❌ Click failed: {e}")
                
                break  # Test only first hotel if we found working selectors
                
            except Exception as e:
                print(f"❌ Error testing {hotel_url}: {e}")
                continue
        
        await browser.close()
        return working_selectors

if __name__ == "__main__":
    working = asyncio.run(investigate_pagination_thoroughly())
    print(f"\n🎯 FINAL RESULT: {len(working)} working pagination selectors found")