#!/usr/bin/env python3
"""
Pagination Investigation Script - Find current pagination elements
"""

import asyncio
from playwright.async_api import async_playwright

async def investigate_pagination():
    print("🔍 Investigating Booking.com pagination structure...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Navigate to a known hotel
        url = "https://www.booking.com/hotel/ae/al-bateen-resdences-by-happy-season-jbr.html"
        print(f"📍 Navigating to: {url}")
        
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(5000)
        
        # Click "Read all reviews" to get to pagination area
        print("\n🔍 NAVIGATING TO REVIEWS SECTION:")
        try:
            reviews_button = page.locator("[data-testid*='reviews']:has-text('all')").first
            if await reviews_button.is_visible():
                print("✅ Clicking 'Read all reviews'...")
                await reviews_button.click()
                await page.wait_for_timeout(5000)
                print("✅ Successfully navigated to reviews section")
            else:
                print("❌ Could not find reviews button")
        except Exception as e:
            print(f"⚠️ Navigation failed: {e}")
        
        print("\n🔍 SEARCHING FOR PAGINATION ELEMENTS:")
        
        # Test pagination selectors
        pagination_selectors = [
            # Booking.com specific
            "[data-testid='pagination-next-button']",
            "[data-testid*='pagination']:has-text('Next')",
            "[data-testid*='next']",
            ".bui-button:has-text('Next')",
            ".bui-pagination__next-arrow",
            
            # Generic pagination
            "button:has-text('Next page')",
            "a:has-text('Next page')",
            "button:has-text('Next')",
            "a:has-text('Next')",
            "[aria-label*='Next']",
            ".pagination-next",
            "a[href*='offset=']",
            "[class*='next'][class*='page']",
            
            # Load more patterns
            "button:has-text('Show more')",
            "a:has-text('Show more')",
            "button:has-text('Load more')",
            "[class*='load'][class*='more']",
            "[class*='show'][class*='more']",
            
            # Numeric pagination
            "[class*='pagination'] button",
            "[class*='pagination'] a",
            ".paginate button",
            ".paginate a",
            "[data-offset]",
            
            # Any clickable pagination elements
            "button[class*='page']",
            "a[class*='page']",
            "[onclick*='page']",
            "[onclick*='more']"
        ]
        
        found_pagination = []
        
        for selector in pagination_selectors:
            try:
                elements = await page.locator(selector).count()
                if elements > 0:
                    print(f"✅ {selector}: {elements} elements")
                    
                    # Get details from first element
                    try:
                        first_element = page.locator(selector).first
                        text = await first_element.inner_text()
                        is_visible = await first_element.is_visible()
                        is_enabled = await first_element.is_enabled()
                        disabled = await first_element.get_attribute('disabled')
                        
                        print(f"   Text: '{text}' | Visible: {is_visible} | Enabled: {is_enabled} | Disabled attr: {disabled}")
                        
                        if is_visible and is_enabled and not disabled:
                            found_pagination.append((selector, text))
                    except Exception as detail_error:
                        print(f"   Could not get element details: {detail_error}")
                else:
                    print(f"❌ {selector}: 0 elements")
            except Exception as e:
                print(f"⚠️  {selector}: ERROR - {e}")
        
        print(f"\n📊 FOUND {len(found_pagination)} working pagination elements")
        
        # Check current page structure
        print("\n🔍 ANALYZING CURRENT PAGE STRUCTURE:")
        
        # Count reviews on current page
        review_count_selectors = [
            "[data-testid*='review']",
            "[class*='review']",
            "[id*='review']"
        ]
        
        for selector in review_count_selectors:
            try:
                count = await page.locator(selector).count()
                print(f"📊 {selector}: {count} review elements")
            except:
                continue
        
        # Look for page indicators
        page_indicators = [
            "[data-testid*='page']",
            "[class*='page'][class*='current']",
            "[aria-current='page']",
            ".current-page",
            "[class*='active'][class*='page']"
        ]
        
        print("\n🔍 SEARCHING FOR PAGE INDICATORS:")
        for selector in page_indicators:
            try:
                count = await page.locator(selector).count()
                if count > 0:
                    text = await page.locator(selector).first.inner_text()
                    print(f"✅ {selector}: {count} elements - Text: '{text}'")
                else:
                    print(f"❌ {selector}: 0 elements")
            except Exception as e:
                print(f"⚠️ {selector}: ERROR - {e}")
        
        await browser.close()
        
        return found_pagination

if __name__ == "__main__":
    found = asyncio.run(investigate_pagination())
    print(f"\n🎯 PAGINATION INVESTIGATION COMPLETE - {len(found)} working pagination elements found")
    
    if found:
        print("\n✅ WORKING PAGINATION SELECTORS:")
        for selector, text in found:
            print(f"   {selector} -> '{text}'")
    else:
        print("\n❌ NO WORKING PAGINATION ELEMENTS FOUND")