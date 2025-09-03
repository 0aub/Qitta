"""
Booking.com Investigation & Testing Tools
========================================

Test utilities for debugging and investigating Booking.com structure.
Moved from main task file to maintain clean production code.
"""

import logging
from typing import Dict, Any


class BookingInvestigator:
    """Investigation tools for Booking.com structure analysis."""
    
    @staticmethod
    async def investigate_price_selectors(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Dedicated price selector investigation for Booking.com."""
        
        location = params.get("location", "Dubai Marina")
        logger.info("🎯 PRICE SELECTOR INVESTIGATION STARTING")
        
        try:
            context = await browser.new_context()
            page = await context.new_page()
            
            search_url = f"https://www.booking.com/searchresults.html?ss={location.replace(' ', '+')}&checkin=2025-08-30&checkout=2025-09-02&group_adults=2&no_rooms=1&offset=0"
            logger.info(f"🔗 Search URL: {search_url}")
            
            await page.goto(search_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            
            # Test current price selectors
            current_price_selectors = [
                "span[data-testid='price-and-discounted-price']",
                "[data-testid='price-and-discounted-price']",
                "[data-testid*='price'] span",
                ".bui-price-display__value",
                "[data-testid='price-display'] span"
            ]
            
            price_results = {}
            logger.info("📍 TESTING PRICE SELECTORS")
            
            for i, selector in enumerate(current_price_selectors):
                try:
                    count = await page.locator(selector).count()
                    logger.info(f"   Selector {i+1}: {selector} -> {count} elements")
                    price_results[f"selector_{i+1}"] = {"selector": selector, "count": count}
                    
                    if count > 0:
                        first_element = page.locator(selector).first
                        if await first_element.is_visible():
                            sample_text = await first_element.inner_text()
                            logger.info(f"      Sample: '{sample_text}'")
                            price_results[f"selector_{i+1}"]["sample"] = sample_text
                except Exception as e:
                    logger.info(f"   Error with {selector}: {e}")
                    price_results[f"selector_{i+1}"] = {"selector": selector, "error": str(e)}
            
            await context.close()
            
            return {
                "status": "completed",
                "result": {
                    "investigation_type": "price_selector_analysis",
                    "location": location,
                    "search_url": search_url,
                    "findings": {"price_selectors": price_results}
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Price investigation failed: {e}")
            return {"status": "error", "error": str(e)}

    @staticmethod
    async def investigate_review_structure(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Manual investigation of Booking.com structure to understand pricing and review limitations."""
        
        hotel_url = params.get("hotel_url", "https://www.booking.com/hotel/ae/intercontinental-dubai-marina.html")
        focus = params.get("focus", "reviews")  # "reviews" or "price_selectors"
        
        logger.info(f"🔍 DEBUG: focus parameter = '{focus}' (type: {type(focus)})")
        logger.info(f"🔍 DEBUG: focus == 'price_selectors' ? {focus == 'price_selectors'}")
        
        if focus == "price_selectors":
            logger.info("🔍 MANUAL INVESTIGATION: Booking.com Price Selectors")
        else:
            logger.info("🔍 MANUAL INVESTIGATION: Booking.com Review Structure")
        logger.info("=" * 60)
        
        try:
            context = await browser.new_context()
            page = await context.new_page()
            
            investigation_results = {
                "investigation_type": f"booking_{focus}_analysis",
                "findings": {},
                "recommendations": []
            }
            
            if focus == "price_selectors":
                # Price Investigation: Go to search results page 
                logger.info("🎯 EXECUTING PRICE INVESTIGATION")
                location = params.get("location", "Dubai Marina")
                search_url = f"https://www.booking.com/searchresults.html?ss={location.replace(' ', '+')}&checkin=2025-08-30&checkout=2025-09-02&group_adults=2&no_rooms=1&offset=0"
                logger.info(f"🔗 Search URL: {search_url}")
                
                await page.goto(search_url, wait_until="domcontentloaded")
                await page.wait_for_timeout(5000)  # Wait for prices to load
                
                # Test current price selectors
                current_price_selectors = [
                    "[data-testid='price-and-discounted-price'] .bui-price-display__value",
                    "span[data-testid='price-and-discounted-price']", 
                    ".bui-price-display__value",
                    "[data-testid='price-display']",
                    ".prco-valign-middle-helper",
                    ".bui-f-color--destructive", 
                    ".sr-hotel__price--primary span",
                    "[aria-label*='price']",
                    "span:has-text('$'):not(:has-text('Show'))"
                ]
                
                price_results = {}
                logger.info("📍 TESTING CURRENT PRICE SELECTORS")
                
                for i, selector in enumerate(current_price_selectors):
                    try:
                        count = await page.locator(selector).count()
                        if count > 0:
                            logger.info(f"   ✅ Selector {i+1}: Found {count} elements with: {selector}")
                            price_results[f"selector_{i+1}"] = {"selector": selector, "count": count}
                            
                            # Get sample price text
                            first_element = page.locator(selector).first
                            if await first_element.is_visible():
                                sample_text = await first_element.inner_text()
                                logger.info(f"      📝 Sample text: '{sample_text}'")
                                price_results[f"selector_{i+1}"]["sample"] = sample_text
                        else:
                            logger.info(f"   ❌ Selector {i+1}: No elements found with: {selector}")
                            price_results[f"selector_{i+1}"] = {"selector": selector, "count": 0}
                    except Exception as e:
                        logger.info(f"   ⚠️ Selector {i+1}: Error with {selector}: {e}")
                        price_results[f"selector_{i+1}"] = {"selector": selector, "error": str(e)}
                
                # Discover actual price elements on page
                logger.info("📍 DISCOVERING ACTUAL PRICE ELEMENTS")
                discovery_selectors = [
                    "[class*='price']",
                    "[data-testid*='price']", 
                    "span:has-text('$')",
                    "span:has-text('AED')",  
                    "span:has-text('USD')",
                    ".sr-hotel__price",
                    ".bui-price",
                    "[aria-label*='Price']",
                    "span[class*='price']"
                ]
                
                discovery_results = {}
                for i, selector in enumerate(discovery_selectors):
                    try:
                        count = await page.locator(selector).count()
                        if count > 0:
                            logger.info(f"   🔍 Discovery {i+1}: Found {count} elements with: {selector}")
                            discovery_results[f"discovery_{i+1}"] = {"selector": selector, "count": count}
                            
                            # Get sample text from first few elements
                            for j in range(min(3, count)):
                                element = page.locator(selector).nth(j)
                                if await element.is_visible():
                                    text = await element.inner_text()
                                    if text and any(char.isdigit() for char in text):
                                        logger.info(f"      📝 Sample {j+1}: '{text}'")
                                        discovery_results[f"discovery_{i+1}"][f"sample_{j+1}"] = text
                    except:
                        continue
                
                investigation_results["findings"]["current_selectors"] = price_results
                investigation_results["findings"]["discovered_selectors"] = discovery_results
                investigation_results["search_url"] = search_url
                
            else:
                # Original review investigation
                logger.info(f"🔗 Base Hotel URL: {hotel_url}")
                investigation_results["hotel_url"] = hotel_url
                
                # Test 1: Load base hotel page and look for reviews
                logger.info("📍 TEST 1: Base hotel page review analysis")
                await page.goto(hotel_url, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
                
                # Check how many reviews are visible on main page
                review_selectors = [
                    "[data-testid='review-card']",
                    "[data-testid='review']",
                    ".c-review",
                    ".review_item",
                    ".review-item",
                    "[class*='review']"
                ]
            
                base_page_results = {}
                for selector in review_selectors:
                    try:
                        count = await page.locator(selector).count()
                        if count > 0:
                            logger.info(f"   ✅ Found {count} reviews with selector: {selector}")
                            base_page_results[selector] = count
                            
                            # Get sample review text
                            first_review = page.locator(selector).first
                            if await first_review.is_visible():
                                sample_text = await first_review.inner_text()
                                preview = sample_text[:100] + "..." if len(sample_text) > 100 else sample_text
                                logger.info(f"   📝 Sample: '{preview}'")
                                base_page_results[f"{selector}_sample"] = preview
                            break
                    except:
                        continue
            
                investigation_results["findings"]["base_page_reviews"] = base_page_results
                
                # Test 2: Look for review navigation elements
                logger.info("📍 TEST 2: Review navigation elements")
                nav_selectors = [
                "a[href*='reviews']",
                "button:has-text('Reviews')",
                "[data-testid*='review']",
                "a:has-text('reviews')",
                ".reviews-tab",
                "#reviews_tab",
                "[href*='reviewsTab']"
            ]
            
                nav_results = {}
                review_nav_found = False
                for selector in nav_selectors:
                    try:
                        elements = page.locator(selector)
                        count = await elements.count()
                        if count > 0:
                            logger.info(f"   ✅ Found {count} review navigation elements: {selector}")
                            nav_results[selector] = count
                            review_nav_found = True
                    except:
                        continue
                
                investigation_results["findings"]["navigation_elements"] = nav_results
                if not review_nav_found:
                    logger.info("   ❌ No review navigation elements found on main page")
                    
                # Generate review-specific recommendations
                if base_page_results:
                    max_reviews = max([v for k, v in base_page_results.items() if isinstance(v, int)])
                    investigation_results["recommendations"].append(f"Base hotel page shows {max_reviews} reviews maximum")
                else:
                    investigation_results["recommendations"].append("No reviews found on base hotel page")
                
                if nav_results:
                    investigation_results["recommendations"].append("Review navigation elements found - dedicated review pages may be accessible")
                else:
                    investigation_results["recommendations"].append("No review navigation found - reviews limited to base page only")
            
            await context.close()
            
            # Generate price-specific recommendations if focus is price_selectors
            if focus == "price_selectors":
                working_selectors = [k for k, v in investigation_results["findings"]["current_selectors"].items() if isinstance(v, dict) and v.get("count", 0) > 0]
                discovered_selectors = [k for k, v in investigation_results["findings"]["discovered_selectors"].items() if isinstance(v, dict) and v.get("count", 0) > 0]
                
                if working_selectors:
                    investigation_results["recommendations"].append(f"Found {len(working_selectors)} working current selectors")
                else:
                    investigation_results["recommendations"].append("None of the current price selectors are working - need new selectors")
                
                if discovered_selectors:
                    investigation_results["recommendations"].append(f"Discovered {len(discovered_selectors)} potential new price selectors")
                    investigation_results["recommendations"].append("Update price extraction with discovered selectors")
                else:
                    investigation_results["recommendations"].append("No price elements discovered - may require different search or investigation")
            
            logger.info("=" * 60)
            logger.info("🎯 INVESTIGATION COMPLETE")
            logger.info("=" * 60)
            
            for i, rec in enumerate(investigation_results["recommendations"], 1):
                logger.info(f"{i}. {rec}")
            
            return {
                "status": "completed",
                "result": investigation_results
            }
            
        except Exception as e:
            logger.error(f"❌ Investigation failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }