#!/usr/bin/env python3
"""
Analyze Booking.com Reviews Page Structure to understand pagination
"""

import asyncio
from playwright.async_api import async_playwright

async def analyze_reviews_page_structure():
    print("🔍 BOOKING.COM REVIEWS STRUCTURE ANALYSIS...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.booking.com/hotel/ae/atlantis-the-palm.html"
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(3000)
        
        # Navigate to reviews page
        reviews_link = page.locator("a:has-text('Guest reviews (20,620)')").first
        await reviews_link.click()
        await page.wait_for_timeout(5000)
        
        current_url = page.url
        print(f"📍 Reviews page URL: {current_url}")
        
        # Get page HTML to analyze structure
        page_content = await page.content()
        
        # Save the page for analysis
        with open('/tmp/reviews_page.html', 'w') as f:
            f.write(page_content)
        
        # Count reviews
        reviews = await page.locator("[data-testid*='review']").count()
        print(f"📊 Reviews found: {reviews}")
        
        # Look for any review-related JavaScript or AJAX endpoints
        print("\n🔍 Analyzing JavaScript and network activity...")
        
        # Check for form elements or hidden inputs that might control pagination
        forms = await page.locator("form").count()
        print(f"📊 Forms found: {forms}")
        
        # Look for any input elements with pagination-related names
        pagination_inputs = await page.locator("input[name*='page'], input[name*='offset'], input[name*='limit']").count()
        print(f"📊 Pagination inputs: {pagination_inputs}")
        
        # Look for JavaScript variables or data attributes
        print("\n🔍 Looking for review data in JavaScript...")
        
        # Check for window objects or data attributes
        try:
            review_data = await page.evaluate("""
                () => {
                    // Look for global variables that might contain review data
                    const globals = Object.keys(window).filter(key => 
                        key.toLowerCase().includes('review') || 
                        key.toLowerCase().includes('page') ||
                        key.toLowerCase().includes('booking')
                    );
                    
                    // Look for data attributes on elements
                    const dataElements = Array.from(document.querySelectorAll('[data-total], [data-count], [data-page]'));
                    
                    return {
                        globals: globals,
                        dataElements: dataElements.map(el => ({
                            tag: el.tagName,
                            attributes: Array.from(el.attributes).map(attr => attr.name + '=' + attr.value)
                        }))
                    };
                }
            """)
            
            print(f"📊 Global variables: {review_data['globals']}")
            print(f"📊 Data elements: {len(review_data['dataElements'])}")
            
        except Exception as e:
            print(f"❌ JavaScript analysis failed: {e}")
        
        # Check for any AJAX/API endpoints by monitoring network
        print("\n🔍 Monitoring network for AJAX calls...")
        
        # Clear any existing listeners and set up new ones
        network_calls = []
        
        async def capture_request(request):
            if 'review' in request.url or 'page' in request.url:
                network_calls.append(request.url)
        
        page.on('request', capture_request)
        
        # Try to trigger network activity by interacting with the page
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(3000)
        
        # Try clicking on different review elements
        try:
            review_elements = page.locator("[data-testid*='review']")
            count = min(await review_elements.count(), 3)
            
            for i in range(count):
                review = review_elements.nth(i)
                try:
                    await review.click()
                    await page.wait_for_timeout(1000)
                except:
                    pass
        except:
            pass
        
        print(f"📊 Network calls captured: {len(network_calls)}")
        for call in network_calls:
            print(f"   🌐 {call}")
        
        # Check the actual page structure around reviews
        print("\n🔍 Analyzing review container structure...")
        
        try:
            review_container_info = await page.evaluate("""
                () => {
                    const reviews = document.querySelectorAll('[data-testid*="review"]');
                    if (reviews.length === 0) return 'No reviews found';
                    
                    const firstReview = reviews[0];
                    const container = firstReview.closest('div[class], section[class]');
                    
                    return {
                        reviewCount: reviews.length,
                        containerTag: container ? container.tagName : 'none',
                        containerClass: container ? container.className : 'none',
                        nextSiblings: container ? container.nextElementSibling ? container.nextElementSibling.outerHTML.slice(0, 200) : 'none' : 'none'
                    };
                }
            """)
            
            print(f"📊 Review container info: {review_container_info}")
            
        except Exception as e:
            print(f"❌ Container analysis failed: {e}")
        
        # Final attempt: Look for ANY button or link that's not already tested
        print("\n🔍 Final scan for ANY actionable elements...")
        
        try:
            all_buttons = await page.locator("button, a, [role='button']").count()
            print(f"📊 Total clickable elements: {all_buttons}")
            
            # Check if any contain text about loading/showing more
            potential_elements = []
            
            for i in range(min(all_buttons, 20)):  # Check first 20
                try:
                    element = page.locator("button, a, [role='button']").nth(i)
                    if await element.is_visible():
                        text = await element.inner_text()
                        aria_label = await element.get_attribute('aria-label') or ''
                        
                        if any(keyword in (text + aria_label).lower() for keyword in 
                               ['more', 'load', 'next', 'page', 'show', 'all', 'view']):
                            if not any(skip in (text + aria_label).lower() for skip in 
                                     ['photo', 'image', 'gallery', 'book', 'reserve']):
                                potential_elements.append({
                                    'index': i,
                                    'text': text[:50],
                                    'aria_label': aria_label
                                })
                except:
                    continue
            
            print(f"📊 Potential review-related elements: {len(potential_elements)}")
            for elem in potential_elements:
                print(f"   🎯 {elem['index']}: '{elem['text']}' (aria: {elem['aria_label']})")
            
        except Exception as e:
            print(f"❌ Element scan failed: {e}")
        
        await browser.close()
        
        return {
            'reviews_found': reviews,
            'url': current_url,
            'network_calls': network_calls
        }

if __name__ == "__main__":
    result = asyncio.run(analyze_reviews_page_structure())
    print(f"\n🎯 STRUCTURE ANALYSIS COMPLETE: {result}")