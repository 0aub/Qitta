#!/usr/bin/env python3
"""
Investigate what DOM elements are being picked up as reviewer names
"""

import asyncio
from playwright.async_api import async_playwright

async def investigate_reviewer_names():
    print("🔍 Investigating reviewer name extraction issue...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Navigate to a known hotel with reviews
        url = "https://www.booking.com/hotel/ae/al-bateen-resdences-by-happy-season-jbr.html"
        print(f"📍 Navigating to: {url}")
        
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(5000)
        
        # Click "Read all reviews" to get to reviews section
        try:
            reviews_button = page.locator("[data-testid*='reviews']:has-text('all')").first
            if await reviews_button.is_visible():
                print("✅ Clicking 'Read all reviews'...")
                await reviews_button.click()
                await page.wait_for_timeout(5000)
            else:
                print("❌ Could not find reviews button")
        except Exception as e:
            print(f"⚠️ Navigation failed: {e}")
        
        print("\n🔍 ANALYZING REVIEW STRUCTURE:")
        
        # Find review elements using working selectors
        review_selectors = [
            "[data-testid*='review']",
            "[class*='review']"
        ]
        
        for selector in review_selectors:
            try:
                review_elements = page.locator(selector)
                count = await review_elements.count()
                
                if count > 0:
                    print(f"\n✅ Found {count} elements with selector: {selector}")
                    
                    # Check first few elements in detail
                    for i in range(min(3, count)):
                        print(f"\n📋 ELEMENT {i+1}:")
                        element = review_elements.nth(i)
                        
                        try:
                            # Get all text from the element
                            full_text = await element.inner_text()
                            print(f"   Full text: '{full_text[:150]}...'")
                            
                            # Test reviewer name selectors that might be causing corruption
                            name_selectors = [
                                "[data-testid='reviewer-name']",
                                ".bui-avatar-block__title",
                                ".c-review-block__reviewer-name", 
                                ".reviewer-name",
                                "h4",
                                ".bui-f-font-weight--bold"
                            ]
                            
                            print("   🔍 Testing reviewer name selectors:")
                            for name_selector in name_selectors:
                                try:
                                    name_elements = element.locator(name_selector)
                                    name_count = await name_elements.count()
                                    if name_count > 0:
                                        name_text = await name_elements.first.inner_text()
                                        print(f"      ✅ {name_selector}: '{name_text[:50]}...'")
                                except:
                                    pass
                            
                            # Test review text selectors
                            print("   🔍 Testing review text selectors:")
                            text_selectors = [
                                "[data-testid='review-positive-text']",
                                "[data-testid='review-negative-text']", 
                                ".c-review__positive",
                                ".c-review__negative"
                            ]
                            
                            for text_selector in text_selectors:
                                try:
                                    text_elements = element.locator(text_selector)
                                    text_count = await text_elements.count()
                                    if text_count > 0:
                                        review_text = await text_elements.first.inner_text()
                                        print(f"      ✅ {text_selector}: '{review_text[:50]}...'")
                                except:
                                    pass
                                    
                        except Exception as e:
                            print(f"   ❌ Error analyzing element: {e}")
                            
                    break  # Only analyze first working selector
            except:
                continue
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(investigate_reviewer_names())