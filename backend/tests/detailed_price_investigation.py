#!/usr/bin/env python3
"""
Detailed investigation of price elements within hotel cards
"""

import asyncio
from playwright.async_api import async_playwright

async def detailed_price_investigation():
    print("🔍 DETAILED BOOKING.COM PRICE INVESTIGATION...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Search for hotels
        search_url = "https://www.booking.com/searchresults.html?ss=Dubai&dest_id=-782831&dest_type=city&checkin=2025-09-02&checkout=2025-09-05&group_adults=2&no_rooms=1"
        print(f"🔗 Search URL: {search_url}")
        
        await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(8000)
        
        # Find hotel elements
        hotels = page.locator("[data-testid*='hotel']")
        hotel_count = await hotels.count()
        print(f"📊 Found {hotel_count} hotels")
        
        if hotel_count > 0:
            print("\n📍 ANALYZING FIRST 3 HOTEL CARDS...")
            
            for i in range(min(3, hotel_count)):
                hotel_card = hotels.nth(i)
                print(f"\n🏨 HOTEL {i+1}:")
                
                # Get hotel name for context
                try:
                    name_element = hotel_card.locator("h3, [data-testid*='title'], *[class*='title']").first
                    hotel_name = await name_element.inner_text() if await name_element.count() > 0 else f"Hotel {i+1}"
                    print(f"   📝 Hotel: {hotel_name[:50]}...")
                except:
                    hotel_name = f"Hotel {i+1}"
                    print(f"   📝 Hotel: {hotel_name}")
                
                # Look for ANY text containing currency or numbers that might be prices
                print("   💰 Looking for price-like text...")
                
                # Search for various currency patterns within the card
                currency_patterns = [
                    "*:has-text('$')",
                    "*:has-text('USD')",
                    "*:has-text('AED')", 
                    "*:has-text('€')",
                    "*:has-text('night')",
                    "*:has-text('per')",
                    "span",  # Check all spans
                    "div"    # Check all divs
                ]
                
                found_prices = []
                
                for pattern in currency_patterns[:6]:  # Check currency patterns first
                    try:
                        elements = hotel_card.locator(pattern)
                        count = await elements.count()
                        
                        if count > 0:
                            for j in range(min(count, 5)):  # Check first 5 matches
                                try:
                                    element = elements.nth(j)
                                    if await element.is_visible():
                                        text = await element.inner_text()
                                        if text and text.strip() and len(text.strip()) < 50:
                                            # Check if it looks like a price
                                            import re
                                            if re.search(r'\$\d+|\d+\$|USD\s*\d+|\d+\s*USD|AED\s*\d+|\d+\s*AED|€\d+|\d+€', text):
                                                print(f"      ✅ {pattern}: '{text.strip()}'")
                                                found_prices.append(text.strip())
                                            elif pattern in ["*:has-text('night')", "*:has-text('per')"] and re.search(r'\d+', text):
                                                print(f"      💡 {pattern}: '{text.strip()}'")
                                except:
                                    continue
                    except:
                        continue
                
                # If no clear prices found, look at all text elements
                if not found_prices:
                    print("   🔍 No clear prices found, checking all text elements...")
                    
                    try:
                        all_elements = hotel_card.locator("span, div")
                        count = await all_elements.count()
                        
                        for j in range(min(count, 20)):  # Check first 20 elements
                            try:
                                element = all_elements.nth(j)
                                if await element.is_visible():
                                    text = await element.inner_text()
                                    if text and text.strip():
                                        # Look for any numbers that might be prices
                                        import re
                                        if re.search(r'\d+', text) and len(text.strip()) < 30:
                                            tag_name = await element.evaluate("el => el.tagName")
                                            class_name = await element.get_attribute("class") or "no-class"
                                            print(f"      📄 {tag_name}.{class_name[:20]}...: '{text.strip()}'")
                            except:
                                continue
                    except Exception as e:
                        print(f"      ❌ Error checking elements: {e}")
                
                # Check the HTML structure of this card
                print("   🔍 Checking card HTML structure...")
                try:
                    card_html = await hotel_card.inner_html()
                    # Look for price-related attributes in HTML
                    import re
                    price_attrs = re.findall(r'(data-[a-zA-Z-]*price[a-zA-Z-]*|class="[^"]*price[^"]*")', card_html)
                    if price_attrs:
                        print(f"      🎯 Found price-related attributes: {price_attrs[:3]}")
                    else:
                        print("      ❌ No price-related attributes found in HTML")
                        
                    # Save a sample of HTML for manual inspection
                    if i == 0:  # Save first card HTML
                        with open('/tmp/sample_hotel_card.html', 'w') as f:
                            f.write(card_html[:2000] + "..." if len(card_html) > 2000 else card_html)
                        print("      💾 Saved sample HTML to /tmp/sample_hotel_card.html")
                        
                except Exception as e:
                    print(f"      ❌ Error getting HTML: {e}")
        
        # Try to look at the overall page structure for price elements
        print("\n📍 CHECKING OVERALL PAGE FOR PRICE ELEMENTS...")
        
        # Look for price-related CSS classes and data attributes across the page
        price_discovery_selectors = [
            "[class*='rate']",
            "[class*='cost']",
            "[class*='amount']", 
            "[data-*='rate']",
            "[data-*='cost']",
            "[data-*='amount']",
            "span[style*='color']",  # Prices often have special colors
            "div[style*='color']"
        ]
        
        for selector in price_discovery_selectors:
            try:
                count = await page.locator(selector).count()
                if count > 0:
                    print(f"   🔍 {selector} -> {count} elements")
                    # Sample first element
                    first_element = page.locator(selector).first
                    if await first_element.is_visible():
                        text = await first_element.inner_text()
                        if text and len(text.strip()) < 100:
                            print(f"      Sample: '{text.strip()}'")
            except:
                continue
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(detailed_price_investigation())