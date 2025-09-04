#!/usr/bin/env python3
"""
Deep Analysis of Review Pagination on Reviews Page
"""

import asyncio
from playwright.async_api import async_playwright

async def deep_review_pagination_analysis():
    print("🔍 DEEP REVIEW PAGINATION ANALYSIS...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.booking.com/hotel/ae/atlantis-the-palm.html"
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(3000)
        
        # Step 1: Click on "Guest reviews (20,620)" to navigate to reviews page
        print("📍 Navigating to full reviews page...")
        
        try:
            # Use the working selector from previous investigation
            reviews_link = page.locator("a:has-text('Guest reviews (20,620)')").first
            await reviews_link.click()
            await page.wait_for_timeout(5000)
            print("✅ Successfully navigated to reviews page")
        except Exception as e:
            print(f"❌ Failed to navigate: {e}")
            await browser.close()
            return
        
        # Step 2: Analyze the reviews page structure
        current_url = page.url
        print(f"📍 Now on: {current_url}")
        
        # Count reviews on this new page
        initial_reviews = await page.locator("[data-testid*='review']").count()
        print(f"📊 Reviews visible on reviews page: {initial_reviews}")
        
        # Step 3: Look for ALL possible pagination/load-more mechanisms
        print("\n🔍 COMPREHENSIVE PAGINATION SEARCH...")
        
        # All possible pagination patterns for Booking.com reviews
        pagination_patterns = [
            # Load more / Show more patterns
            "button:has-text('Load more')",
            "button:has-text('Show more')",
            "button:has-text('More reviews')",
            "button:has-text('View more')",
            "a:has-text('Load more')",
            "a:has-text('Show more')",
            "a:has-text('More reviews')",
            "a:has-text('View more')",
            
            # Next patterns
            "button:has-text('Next')",
            "a:has-text('Next')",
            "[aria-label*='Next']",
            "[data-testid*='next']",
            
            # Page numbers
            "[class*='pagination']",
            "[data-testid*='pagination']",
            "button[class*='page']",
            "a[class*='page']",
            
            # Modern SPA patterns
            "[role='button']:has-text('more')",
            "[role='button']:has-text('load')",
            "[onClick*='more']",
            "[onClick*='load']",
            "[onClick*='page']",
            
            # Generic clickable elements that might trigger loading
            "div[class*='load']",
            "div[class*='more']",
            "span[class*='load']",
            "span[class*='more']",
        ]
        
        found_elements = []
        
        for pattern in pagination_patterns:
            try:
                elements = page.locator(pattern)
                count = await elements.count()
                
                for i in range(count):
                    element = elements.nth(i)
                    if await element.is_visible() and await element.is_enabled():
                        text = await element.inner_text()
                        tag_name = await element.evaluate("el => el.tagName")
                        class_name = await element.get_attribute("class") or ""
                        
                        # Filter out obviously wrong elements
                        text_lower = text.lower()
                        if not any(skip in text_lower for skip in ['photo', 'image', 'gallery', 'book', 'reserve', 'room']):
                            found_elements.append({
                                'pattern': pattern,
                                'text': text,
                                'tag': tag_name,
                                'class': class_name
                            })
                            print(f"✅ FOUND: {pattern} -> '{text}' (Tag: {tag_name}, Class: {class_name[:50]}...)")
            except:
                continue
        
        print(f"\n📊 Found {len(found_elements)} potential pagination elements")
        
        # Step 4: Test each promising element
        if found_elements:
            print(f"\n🧪 TESTING EACH ELEMENT...")
            
            for i, elem_info in enumerate(found_elements[:5]):  # Test top 5
                print(f"\n🎯 TEST {i+1}: {elem_info['pattern']} -> '{elem_info['text'][:50]}...'")
                
                try:
                    # Get fresh element reference
                    test_element = page.locator(elem_info['pattern']).first
                    
                    # Count before click
                    before_count = await page.locator("[data-testid*='review']").count()
                    
                    # Try clicking
                    await test_element.click()
                    await page.wait_for_timeout(3000)
                    
                    # Count after click
                    after_count = await page.locator("[data-testid*='review']").count()
                    
                    if after_count > before_count:
                        print(f"✅ SUCCESS! Reviews: {before_count} -> {after_count}")
                        
                        # If this worked, continue clicking to get more
                        total_loaded = after_count
                        clicks = 1
                        
                        while clicks < 10:  # Safety limit
                            try:
                                # Try clicking again
                                next_element = page.locator(elem_info['pattern']).first
                                if await next_element.is_visible() and await next_element.is_enabled():
                                    await next_element.click()
                                    await page.wait_for_timeout(3000)
                                    
                                    newer_count = await page.locator("[data-testid*='review']").count()
                                    if newer_count > total_loaded:
                                        total_loaded = newer_count
                                        clicks += 1
                                        print(f"🔄 Click {clicks}: {total_loaded} reviews loaded")
                                    else:
                                        print(f"🛑 No more reviews loaded after {clicks} clicks")
                                        break
                                else:
                                    print(f"🛑 Button no longer available after {clicks} clicks")
                                    break
                            except:
                                print(f"🛑 Click failed after {clicks} successful clicks")
                                break
                        
                        print(f"🎯 FINAL RESULT: Loaded {total_loaded} total reviews with {clicks} clicks")
                        
                        # Calculate what percentage of 20,620 we got
                        percentage = (total_loaded / 20620) * 100
                        print(f"📊 Coverage: {total_loaded}/20,620 reviews ({percentage:.1f}%)")
                        
                        if total_loaded >= 1000:  # Significant improvement
                            print(f"✅ EXCELLENT! Found working pagination: {elem_info['pattern']}")
                            await browser.close()
                            return {
                                'success': True,
                                'pattern': elem_info['pattern'], 
                                'reviews_loaded': total_loaded,
                                'clicks_needed': clicks,
                                'percentage': percentage
                            }
                    else:
                        print(f"❌ No change: {before_count} reviews (same)")
                        
                except Exception as e:
                    print(f"❌ Click failed: {e}")
                    continue
        
        # Step 5: Check if we might need to scroll or use different approach
        print(f"\n🔍 ALTERNATIVE APPROACH: Check for infinite scroll...")
        
        # Try scrolling to see if more reviews load
        before_scroll = await page.locator("[data-testid*='review']").count()
        
        # Scroll to bottom multiple times
        for scroll_attempt in range(3):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)
            
            after_scroll = await page.locator("[data-testid*='review']").count()
            if after_scroll > before_scroll:
                print(f"✅ Scroll {scroll_attempt + 1}: {before_scroll} -> {after_scroll} reviews")
                before_scroll = after_scroll
            else:
                print(f"❌ Scroll {scroll_attempt + 1}: No new reviews ({after_scroll})")
        
        final_count = await page.locator("[data-testid*='review']").count()
        await browser.close()
        
        return {
            'success': False,
            'final_reviews': final_count,
            'pagination_elements_found': len(found_elements),
            'url': current_url
        }

if __name__ == "__main__":
    result = asyncio.run(deep_review_pagination_analysis())
    print(f"\n🎯 ANALYSIS COMPLETE: {result}")