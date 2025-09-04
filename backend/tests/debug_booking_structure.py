#!/usr/bin/env python3
"""
Debug Booking.com page structure to understand current DOM
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_booking_structure():
    print("🔍 DEBUGGING BOOKING.COM STRUCTURE...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Try a specific hotel page first to see if prices are visible there
        hotel_url = "https://www.booking.com/hotel/ae/atlantis-the-palm.html"
        print(f"🏨 Testing hotel page: {hotel_url}")
        
        await page.goto(hotel_url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(5000)
        
        print("\n📍 ANALYZING HOTEL PAGE STRUCTURE...")
        
        # Look for any price-related elements on hotel page
        price_patterns = [
            "*:has-text('$')",
            "*:has-text('USD')", 
            "*:has-text('AED')",
            "*:has-text('per night')",
            "*:has-text('Price')",
            "*[class*='price']",
            "*[data-testid*='price']"
        ]
        
        for pattern in price_patterns:
            try:
                count = await page.locator(pattern).count()
                print(f"   Pattern: {pattern} -> {count} elements")
                
                if count > 0:
                    # Sample first few
                    for i in range(min(count, 3)):
                        try:
                            element = page.locator(pattern).nth(i)
                            if await element.is_visible():
                                text = await element.inner_text()
                                if text and len(text.strip()) < 100:  # Avoid huge blocks
                                    print(f"      Sample {i+1}: '{text.strip()}'")
                        except:
                            pass
            except Exception as e:
                print(f"   Error with {pattern}: {e}")
        
        print("\n📍 TESTING SEARCH RESULTS PAGE...")
        
        # Now try search results
        search_url = "https://www.booking.com/searchresults.html?ss=Dubai&dest_id=-782831&dest_type=city&checkin=2025-09-02&checkout=2025-09-05&group_adults=2&no_rooms=1"
        print(f"🔗 Search URL: {search_url}")
        
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(8000)  # Wait for content to load
        
        print("\n📍 CHECKING PAGE LOAD STATUS...")
        
        # Check if page loaded properly
        page_title = await page.title()
        print(f"📄 Page title: {page_title}")
        
        # Look for any hotels/properties
        property_patterns = [
            "[data-testid*='property']",
            "[data-testid*='hotel']", 
            "[data-testid*='card']",
            "*[class*='hotel']",
            "*[class*='property']",
            "*[class*='accommodation']"
        ]
        
        found_properties = False
        for pattern in property_patterns:
            try:
                count = await page.locator(pattern).count()
                if count > 0:
                    print(f"   Properties pattern: {pattern} -> {count} elements")
                    found_properties = True
            except:
                pass
        
        if not found_properties:
            print("❌ No property elements found - page may not have loaded correctly")
            
            # Check for common Booking.com elements
            print("\n📍 CHECKING FOR BASIC PAGE ELEMENTS...")
            basic_elements = [
                "header",
                "footer", 
                "[class*='booking']",
                "*:has-text('Booking.com')",
                "*:has-text('Search')",
                "form",
                "input"
            ]
            
            for element in basic_elements:
                try:
                    count = await page.locator(element).count()
                    print(f"   {element} -> {count} elements")
                except:
                    pass
                    
            # Get page source to check what's actually loaded
            content = await page.content()
            print(f"\n📊 Page content length: {len(content)} characters")
            
            # Check for common error messages
            error_patterns = [
                "*:has-text('error')",
                "*:has-text('Error')", 
                "*:has-text('not available')",
                "*:has-text('blocked')",
                "*:has-text('captcha')",
                "*:has-text('robot')"
            ]
            
            for pattern in error_patterns:
                try:
                    count = await page.locator(pattern).count()
                    if count > 0:
                        element_text = await page.locator(pattern).first.inner_text()
                        print(f"   ⚠️ {pattern} -> {count} elements: '{element_text[:100]}...'")
                except:
                    pass
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_booking_structure())