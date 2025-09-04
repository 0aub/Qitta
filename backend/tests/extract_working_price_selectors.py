#!/usr/bin/env python3
"""
Extract working price selectors from Booking.com 2025
"""

import asyncio
from playwright.async_api import async_playwright

async def extract_working_price_selectors():
    print("🔍 EXTRACTING WORKING PRICE SELECTORS...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        search_url = "https://www.booking.com/searchresults.html?ss=Dubai&dest_id=-782831&dest_type=city&checkin=2025-09-02&checkout=2025-09-05&group_adults=2&no_rooms=1"
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(8000)
        
        hotels = page.locator("[data-testid*='hotel']")
        hotel_count = await hotels.count()
        print(f"📊 Found {hotel_count} hotels")
        
        working_selectors = []
        
        if hotel_count > 0:
            print("\n📍 TESTING SELECTORS ON FIRST HOTEL WITH PRICE...")
            
            # Find first hotel with visible price
            target_hotel = None
            for i in range(min(5, hotel_count)):
                hotel_card = hotels.nth(i)
                
                # Check if this hotel has price text
                price_check = hotel_card.locator("*:has-text('$')")
                if await price_check.count() > 0:
                    target_hotel = hotel_card
                    print(f"   🎯 Using hotel {i+1} for testing (has price)")
                    break
            
            if target_hotel:
                print("\n📍 BUILDING WORKING SELECTORS...")
                
                # Test different ways to select the price elements
                test_selectors = [
                    # Direct text-based selectors (most reliable)
                    "*:has-text('$')",
                    "span:has-text('$')",
                    "div:has-text('$')", 
                    
                    # Per night selectors
                    "*:has-text('per night')",
                    "*:has-text('From $')",
                    
                    # More specific combinations
                    "*:has-text('$'):not(:has-text('Show'))",
                    "*:has-text('night'):has-text('$')",
                    
                    # Element-specific selectors
                    "span",  # Then filter by content
                    "div",   # Then filter by content
                ]
                
                for selector in test_selectors:
                    try:
                        elements = target_hotel.locator(selector)
                        count = await elements.count()
                        
                        if count > 0:
                            # Test each element to see if it contains price
                            for j in range(count):
                                try:
                                    element = elements.nth(j)
                                    if await element.is_visible():
                                        text = await element.inner_text()
                                        if text and text.strip():
                                            # Check if it's a price
                                            import re
                                            if re.search(r'\$\d+|\d+\$', text):
                                                # Extract just the numeric price
                                                numbers = re.findall(r'\d+\.?\d*', text)
                                                if numbers:
                                                    price_value = float(numbers[0])
                                                    if price_value > 10:  # Reasonable hotel price
                                                        working_selectors.append({
                                                            'selector': selector,
                                                            'text': text.strip(),
                                                            'value': price_value,
                                                            'element_index': j
                                                        })
                                                        print(f"   ✅ {selector} (element {j}): '{text.strip()}' -> ${price_value}")
                                except:
                                    continue
                    except Exception as e:
                        print(f"   ❌ Error with {selector}: {e}")
                        continue
        
        print(f"\n📊 FOUND {len(working_selectors)} WORKING SELECTORS")
        
        # Remove duplicates and rank by reliability
        unique_selectors = {}
        for sel in working_selectors:
            key = sel['selector']
            if key not in unique_selectors or sel['element_index'] == 0:  # Prefer first element
                unique_selectors[key] = sel
        
        print("\n🎯 RECOMMENDED SELECTORS (in order of reliability):")
        
        # Sort by reliability (prefer specific text matches, then first elements)
        sorted_selectors = sorted(unique_selectors.values(), 
                                key=lambda x: (
                                    0 if 'has-text' in x['selector'] else 1,  # Text-based selectors first
                                    x['element_index'],  # Prefer first elements
                                    len(x['selector'])   # Prefer shorter selectors
                                ))
        
        final_selectors = []
        for i, sel in enumerate(sorted_selectors[:8]):  # Top 8 selectors
            print(f"   {i+1}. {sel['selector']} -> ${sel['value']} ('{sel['text']}')")
            final_selectors.append(sel['selector'])
        
        # Test the selectors on multiple hotels
        print(f"\n📍 VALIDATING SELECTORS ON MULTIPLE HOTELS...")
        
        validation_results = {}
        for selector in final_selectors[:5]:  # Test top 5
            success_count = 0
            for i in range(min(10, hotel_count)):  # Test on first 10 hotels
                try:
                    hotel_card = hotels.nth(i)
                    elements = hotel_card.locator(selector)
                    count = await elements.count()
                    
                    if count > 0:
                        element = elements.first
                        if await element.is_visible():
                            text = await element.inner_text()
                            import re
                            if text and re.search(r'\$\d+|\d+\$', text):
                                success_count += 1
                except:
                    continue
            
            success_rate = (success_count / 10) * 100
            validation_results[selector] = success_rate
            print(f"   {selector}: {success_count}/10 hotels ({success_rate:.0f}% success)")
        
        await browser.close()
        
        # Return the best selectors
        best_selectors = sorted(validation_results.items(), key=lambda x: x[1], reverse=True)
        
        return {
            'all_working': final_selectors,
            'best_validated': [sel[0] for sel in best_selectors if sel[1] > 0],
            'validation_results': validation_results
        }

if __name__ == "__main__":
    result = asyncio.run(extract_working_price_selectors())
    print(f"\n🎯 FINAL RESULTS:")
    print(f"All working selectors: {len(result['all_working'])}")
    print(f"Best validated selectors: {result['best_validated'][:5]}")
    print(f"Validation results: {result['validation_results']}")