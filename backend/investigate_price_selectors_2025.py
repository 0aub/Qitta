#!/usr/bin/env python3
"""
Investigate current working price selectors for Booking.com 2025
"""

import asyncio
from playwright.async_api import async_playwright

async def investigate_current_price_selectors():
    print("🔍 INVESTIGATING CURRENT BOOKING.COM PRICE SELECTORS...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Search for hotels in Dubai Marina with dates and price range
        search_url = "https://www.booking.com/searchresults.html?ss=Dubai+Marina&checkin=2025-09-02&checkout=2025-09-05&group_adults=2&no_rooms=1&offset=0"
        print(f"🔗 Search URL: {search_url}")
        
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(8000)  # Wait longer for prices to load
        
        print("\n📍 TESTING CURRENT PRICE SELECTORS USED IN CODE...")
        
        # Current selectors from booking_hotels.py
        current_selectors = [
            "*[data-testid*='price'] *",
            "span[data-testid*='price']",
            "[data-testid*='price']", 
            "*[class*='price'] span",
            "*[class*='price']",
            "*[data-price]",
            "*[price]",
            "span[data-testid='price-and-discounted-price']",
            "[data-testid='price-and-discounted-price']",
            "[data-testid*='price'] span",
            ".bui-price-display__value",
            "[data-testid='price-display'] span"
        ]
        
        working_selectors = []
        
        for i, selector in enumerate(current_selectors):
            try:
                count = await page.locator(selector).count()
                print(f"   Selector {i+1}: {selector} -> {count} elements")
                
                if count > 0:
                    # Check first few elements
                    for j in range(min(count, 3)):
                        try:
                            element = page.locator(selector).nth(j)
                            if await element.is_visible():
                                text = await element.inner_text()
                                if text and text.strip() and ('$' in text or 'USD' in text or 'AED' in text):
                                    print(f"      ✅ Element {j+1}: '{text.strip()}'")
                                    if selector not in [s[0] for s in working_selectors]:
                                        working_selectors.append((selector, text.strip()))
                                    break
                        except:
                            continue
                            
            except Exception as e:
                print(f"   Error with {selector}: {e}")
        
        print(f"\n📊 WORKING SELECTORS: {len(working_selectors)}")
        for selector, sample in working_selectors:
            print(f"   ✅ {selector} -> '{sample}'")
        
        print("\n📍 DISCOVERING NEW PRICE SELECTORS...")
        
        # Comprehensive discovery of price elements
        discovery_selectors = [
            # Modern data-testid patterns
            "[data-testid*='price']",
            "[data-testid*='cost']",
            "[data-testid*='rate']",
            "[data-testid*='amount']",
            
            # Class-based patterns
            "[class*='price']",
            "[class*='cost']", 
            "[class*='rate']",
            "[class*='amount']",
            
            # Currency-specific
            "span:has-text('$')",
            "span:has-text('USD')",
            "span:has-text('AED')",
            "div:has-text('$')",
            
            # Aria labels
            "[aria-label*='price']",
            "[aria-label*='cost']",
            
            # Property card specific
            "[data-testid*='property-card'] *:has-text('$')",
            "[data-testid*='hotel'] *:has-text('$')",
            
            # Price display patterns
            "*[data-price-display]",
            "*[data-price-value]",
            "*[price-value]"
        ]
        
        discovered_selectors = []
        
        for i, selector in enumerate(discovery_selectors):
            try:
                count = await page.locator(selector).count()
                if count > 0:
                    # Sample first visible element
                    for j in range(min(count, 3)):
                        try:
                            element = page.locator(selector).nth(j)
                            if await element.is_visible():
                                text = await element.inner_text()
                                if text and text.strip():
                                    # Check if it looks like a price (contains numbers and currency)
                                    import re
                                    if re.search(r'\$\d+|\d+\s*USD|\d+\s*AED|AED\s*\d+', text):
                                        print(f"   🎯 DISCOVERED: {selector} -> '{text.strip()}'")
                                        if selector not in [s[0] for s in discovered_selectors]:
                                            discovered_selectors.append((selector, text.strip()))
                                        break
                        except:
                            continue
                            
            except Exception as e:
                continue
        
        print(f"\n📊 DISCOVERED SELECTORS: {len(discovered_selectors)}")
        for selector, sample in discovered_selectors:
            print(f"   🎯 {selector} -> '{sample}'")
        
        # Test extraction on first hotel card
        print("\n📍 TESTING EXTRACTION ON FIRST HOTEL CARD...")
        
        try:
            # Find property cards
            property_cards = page.locator("[data-testid*='property-card'], [data-testid*='hotel']")
            card_count = await property_cards.count()
            print(f"📊 Found {card_count} property cards")
            
            if card_count > 0:
                first_card = property_cards.first
                
                # Test each working selector on the first card
                print("🧪 Testing selectors on first card:")
                
                all_test_selectors = working_selectors + discovered_selectors
                for selector, _ in all_test_selectors[:10]:  # Test top 10
                    try:
                        elements = first_card.locator(selector)
                        count = await elements.count()
                        
                        if count > 0:
                            element = elements.first
                            if await element.is_visible():
                                text = await element.inner_text()
                                # Extract numbers from text
                                import re
                                numbers = re.findall(r'\d+', text)
                                if numbers:
                                    print(f"   ✅ {selector} -> '{text.strip()}' (numbers: {numbers})")
                                    
                    except Exception as e:
                        continue
        
        except Exception as e:
            print(f"❌ Card testing failed: {e}")
        
        await browser.close()
        
        return {
            'working_current': working_selectors,
            'discovered': discovered_selectors
        }

if __name__ == "__main__":
    result = asyncio.run(investigate_current_price_selectors())
    print(f"\n🎯 INVESTIGATION COMPLETE")
    print(f"Working current selectors: {len(result['working_current'])}")  
    print(f"Newly discovered selectors: {len(result['discovered'])}")