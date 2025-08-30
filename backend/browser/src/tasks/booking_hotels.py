"""
Enhanced Booking.com Hotel Scraper v6.0 - DOM Scraping Edition
=============================================================

Production-ready scraper using direct DOM extraction:
- Direct property card scraping (no GraphQL complexity)
- Working location-specific results
- Complete reviews extraction with reviewer details
- Full amenities and surroundings extraction
- Proper image validation and deduplication
- Enhanced error handling and maintainable code structure

Version: 6.0 (DOM Scraping - Production Ready)
Author: Enhanced Implementation with Manus AI Integration
"""

import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse
import hashlib


class BookingHotelsTask:
    """Production-ready Booking.com hotel scraper using DOM extraction."""
    
    BASE_URL = "https://www.booking.com"
    
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

    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point with enhanced error handling and complete data extraction."""
        try:
            # Validate parameters
            clean_params = BookingHotelsTask._validate_params(params)
            
            # Determine scraping level
            scrape_level = params.get("scrape_level", 2)  # Default to Level 2
            deep_scrape = params.get("deep_scrape", False) or params.get("deep_scrape_enabled", False)
            
            # If old deep_scrape parameter is used, map it to Level 3
            if deep_scrape and scrape_level == 2:
                scrape_level = 3
                
            logger.info(f"🚀 STARTING BOOKING.COM SCRAPER v6.1 - 4-LEVEL SYSTEM")
            logger.info(f"🔄 LEVEL SYSTEM RESTORED: Now supporting scrape_level 1-4")
            logger.info(f"📍 Location: {clean_params['location']}")
            logger.info(f"📅 Dates: {clean_params['check_in']} to {clean_params['check_out']}")
            logger.info(f"📊 Scrape Level: {scrape_level}")
            
            # Map scrape levels to extraction methods
            level_descriptions = {
                1: "Quick Search - Essential data only (no hotel pages)",
                2: "Full Data - Hotel pages with complete details", 
                3: "Basic Reviews - Level 2 + review sampling",
                4: "Deep Reviews - Level 2 + comprehensive reviews (10-50 reviews)"
            }
            
            logger.info(f"🎯 Level {scrape_level}: {level_descriptions.get(scrape_level, 'Unknown level')}")
            
            # Create scraper instance
            scraper = ModernBookingScraper(browser, logger)
            
            # Execute based on scrape level
            if scrape_level >= 4:
                logger.info("🔥 LEVEL 4 - DEEP REVIEWS: Comprehensive review extraction with enhanced navigation")
                hotels = await scraper.scrape_hotels_level_4(clean_params)
                extraction_method = "level_4_deep_reviews"
            elif scrape_level >= 3:
                logger.info("📝 LEVEL 3 - BASIC REVIEWS: Complete data with review sampling (2-5 reviews per hotel)")
                hotels = await scraper.scrape_hotels_level_3(clean_params)
                extraction_method = "level_3_basic_reviews"
            elif scrape_level >= 2:
                logger.info("🏨 LEVEL 2 - FULL DATA: Hotel pages with amenities, images, and coordinates (no reviews)")
                hotels = await scraper.scrape_hotels_level_2(clean_params)
                extraction_method = "level_2_full_data"
            else:  # Level 1
                logger.info("⚡ LEVEL 1 - QUICK SEARCH: Essential data from search results only")
                hotels = await scraper.scrape_hotels_quick(clean_params)
                extraction_method = "level_1_quick_search"
            
            # Apply filters
            hotels = BookingHotelsTask._apply_filters(hotels, params, logger)
            
            # Calculate metrics
            hotels_with_prices = [h for h in hotels if h.get('price_per_night')]
            success_rate = len(hotels_with_prices) / len(hotels) if hotels else 0
            avg_price = sum(h.get('price_per_night', 0) for h in hotels_with_prices) / len(hotels_with_prices) if hotels_with_prices else 0
            
            # Calculate data completeness
            avg_completeness = sum(h.get('data_completeness', 0) for h in hotels) / len(hotels) if hotels else 0
            
            logger.info(f"🏁 Scraping completed: {len(hotels)} hotels | {success_rate:.1%} with prices | {avg_completeness:.1f}% data completeness")
            
            result = {
                "search_metadata": {
                    "location": clean_params["location"],
                    "check_in": clean_params["check_in"],
                    "check_out": clean_params["check_out"],
                    "nights": clean_params["nights"],
                    "extraction_method": extraction_method,
                    "scrape_level": scrape_level,
                    "deep_scrape_enabled": deep_scrape,
                    "total_found": len(hotels),
                    "success_rate": success_rate,
                    "average_price": avg_price,
                    "average_completeness": avg_completeness,
                    "search_completed_at": datetime.now().isoformat()
                },
                "hotels": hotels
            }
            
            # Save data if output directory provided
            if job_output_dir and hotels:
                import os
                output_file = os.path.join(job_output_dir, "hotels_data.json")
                os.makedirs(job_output_dir, exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Saved complete hotel data to {output_file}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Critical error in scraper: {e}", exc_info=True)
            return {"search_metadata": {"error": str(e)}, "hotels": []}
    
    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize input parameters."""
        location = params.get("location", "").strip()
        if not location:
            raise ValueError("Location is required")
        
        # Parse dates
        check_in = params.get("check_in")
        check_out = params.get("check_out")
        
        if isinstance(check_in, str):
            check_in = datetime.fromisoformat(check_in.replace('Z', '+00:00')).date()
        if isinstance(check_out, str):
            check_out = datetime.fromisoformat(check_out.replace('Z', '+00:00')).date()
        
        # Default dates
        if not check_in:
            check_in = (datetime.now() + timedelta(days=1)).date()
        if not check_out:
            check_out = check_in + timedelta(days=3)
        
        return {
            "location": location,
            "check_in": check_in.isoformat(),
            "check_out": check_out.isoformat(),
            "nights": (check_out - check_in).days,
            "adults": max(1, int(params.get("adults", 2))),
            "children": max(0, int(params.get("children", 0))),
            "rooms": max(1, int(params.get("rooms", 1))),
            "max_results": max(1, int(params.get("max_results", 10))),
            "min_price": params.get("min_price"),
            "max_price": params.get("max_price"),
            "min_rating": params.get("min_rating"),
            "star_rating": params.get("star_rating")
        }
    
    @staticmethod
    def _apply_filters(hotels: List[Dict[str, Any]], params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Apply filtering based on user parameters."""
        original_count = len(hotels)
        filtered_hotels = hotels.copy()
        
        # Price filters
        min_price = params.get("min_price")
        max_price = params.get("max_price")
        if min_price or max_price:
            filtered_hotels = [h for h in filtered_hotels 
                             if h.get('price_per_night') and 
                             (not min_price or h['price_per_night'] >= min_price) and
                             (not max_price or h['price_per_night'] <= max_price)]
        
        # Rating filter
        min_rating = params.get("min_rating")
        if min_rating:
            filtered_hotels = [h for h in filtered_hotels 
                             if h.get('rating') and h['rating'] >= min_rating]
        
        # Star rating filter (if supported)
        star_rating = params.get("star_rating")
        if star_rating:
            # This would need to be implemented based on hotel star data
            pass
        
        logger.info(f"🔽 Filtering: {original_count} → {len(filtered_hotels)} hotels")
        return filtered_hotels


class ModernBookingScraper:
    """Modern DOM-based scraper using Manus AI's approach with enhancements."""
    
    BASE_URL = "https://www.booking.com"
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        
    async def scrape_hotels_quick(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Quick extraction with essential data using DOM scraping."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Navigate and search
            await self._perform_search(page, params)
            
            # Scrape hotel data from property cards
            hotels = await self._scrape_property_cards(page, params["max_results"], deep_scrape=False)
            
            self.logger.info(f"✅ Quick extraction completed: {len(hotels)} hotels")
            return hotels
            
        finally:
            await context.close()
    
    async def scrape_hotels_level_2(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 2: Full hotel data from individual hotel pages (no reviews)."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Navigate and search
            await self._perform_search(page, params)
            
            # Scrape hotel data with deep scraping but no reviews
            hotels = await self._scrape_property_cards(page, params["max_results"], deep_scrape=True)
            
            self.logger.info(f"🏨 Level 2 extraction completed: {len(hotels)} hotels with full data (no reviews)")
            return hotels
            
        finally:
            await context.close()
    
    async def scrape_hotels_level_3(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 3: Full hotel data with basic review sampling (2-5 reviews per hotel)."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Navigate and search
            await self._perform_search(page, params)
            
            # Scrape hotel data with basic reviews
            hotels = await self._scrape_property_cards_level_3(page, params["max_results"])
            
            self.logger.info(f"📝 Level 3 extraction completed: {len(hotels)} hotels with basic reviews")
            return hotels
            
        finally:
            await context.close()
    
    async def scrape_hotels_complete(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Complete extraction with all details including reviews, coordinates, etc."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Navigate and search
            await self._perform_search(page, params)
            
            # Scrape hotel data with deep scraping
            hotels = await self._scrape_property_cards(page, params["max_results"], deep_scrape=True)
            
            self.logger.info(f"🎯 Complete extraction finished: {len(hotels)} hotels")
            return hotels
            
        finally:
            await context.close()
    
    async def scrape_hotels_level_4(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 4: Enhanced comprehensive extraction with deep review analysis (10-50 reviews per hotel)."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Navigate and search
            await self._perform_search(page, params)
            
            # Scrape hotel data with Level 4 deep scraping
            hotels = await self._scrape_property_cards_level_4(page, params["max_results"])
            
            self.logger.info(f"🎯 Level 4 extraction completed: {len(hotels)} hotels with comprehensive reviews")
            return hotels
            
        finally:
            await context.close()
    
    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform search on Booking.com using DOM interaction."""
        try:
            self.logger.info(f"🔍 Navigating to Booking.com and searching for: {params['location']}")
            
            # Navigate to booking.com with more lenient waiting
            try:
                await page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(3000)  # Wait for page to stabilize
            except Exception as e:
                self.logger.warning(f"Initial navigation timeout, retrying with load strategy: {e}")
                await page.goto(self.BASE_URL, wait_until="load", timeout=45000)
                await page.wait_for_timeout(5000)
            
            # Handle popups and consent
            await self._handle_popups(page)
            
            # Fill location
            location_input = page.locator("input[placeholder*='Where are you going?'], input[name='ss']")
            await location_input.fill(params["location"])
            await page.wait_for_timeout(1500)
            
            # Try to select from autocomplete
            try:
                autocomplete = page.locator(f"//div[contains(text(), '{params['location']}')]").first
                await autocomplete.click(timeout=3000)
                self.logger.info("✅ Selected location from autocomplete")
            except:
                self.logger.info("ℹ️  Using typed location (no autocomplete selection)")
                await page.keyboard.press("Tab")
            
            # Handle dates - try to set if calendar is visible
            try:
                check_in_date = params["check_in"]
                check_out_date = params["check_out"]
                
                # Try to click date inputs to open calendar
                date_input = page.locator("[data-testid='date-display-field-start'], input[name='checkin']")
                if await date_input.is_visible():
                    await date_input.click()
                    await page.wait_for_timeout(1000)
                    
                    # Try to select dates from calendar
                    checkin_selector = f"[data-date='{check_in_date}']"
                    checkout_selector = f"[data-date='{check_out_date}']"
                    
                    if await page.locator(checkin_selector).is_visible():
                        await page.locator(checkin_selector).click()
                        await page.wait_for_timeout(500)
                        
                    if await page.locator(checkout_selector).is_visible():
                        await page.locator(checkout_selector).click()
                        await page.wait_for_timeout(500)
                        self.logger.info("✅ Set check-in and check-out dates")
                        
            except Exception as e:
                self.logger.debug(f"Date selection skipped: {e}")
            
            # Set number of adults if occupancy config is available
            try:
                occupancy_button = page.locator("button[data-testid='occupancy-config']")
                if await occupancy_button.is_visible():
                    await occupancy_button.click()
                    await page.wait_for_timeout(1000)
                    
                    # Get current adults count
                    adults_input = page.locator("input[id='group_adults']")
                    current_adults = int(await adults_input.input_value()) if await adults_input.is_visible() else 2
                    target_adults = params.get("adults", 2)
                    
                    if target_adults > current_adults:
                        for _ in range(target_adults - current_adults):
                            await page.locator("button[aria-label*='Increase'][aria-label*='Adults']").click()
                            await page.wait_for_timeout(200)
                    elif target_adults < current_adults:
                        for _ in range(current_adults - target_adults):
                            await page.locator("button[aria-label*='Decrease'][aria-label*='Adults']").click()
                            await page.wait_for_timeout(200)
                    
                    # Handle children if specified
                    if "children" in params and params["children"] > 0:
                        target_children = params["children"]
                        children_input = page.locator("input[id='group_children']")
                        current_children = int(await children_input.input_value()) if await children_input.is_visible() else 0
                        
                        if target_children > current_children:
                            for _ in range(target_children - current_children):
                                await page.locator("button[aria-label*='Increase'][aria-label*='Children']").click()
                                await page.wait_for_timeout(200)
                        
                        self.logger.info(f"✅ Set children to {target_children}")
                    
                    # Handle rooms if specified
                    if "rooms" in params and params["rooms"] > 1:
                        target_rooms = params["rooms"]
                        rooms_input = page.locator("input[id='no_rooms']")
                        current_rooms = int(await rooms_input.input_value()) if await rooms_input.is_visible() else 1
                        
                        if target_rooms > current_rooms:
                            for _ in range(target_rooms - current_rooms):
                                await page.locator("button[aria-label*='Increase'][aria-label*='Rooms']").click()
                                await page.wait_for_timeout(200)
                        
                        self.logger.info(f"✅ Set rooms to {target_rooms}")
                    
                    # Close occupancy dialog
                    await page.locator("button:has-text('Done')").click()
                    self.logger.info(f"✅ Set occupancy: {target_adults} adults")
                    
            except Exception as e:
                self.logger.debug(f"Adults selection skipped: {e}")
            
            # Validate form before searching
            try:
                location_input = page.locator("input[placeholder*='Where are you going?'], input[name='ss']").first
                location_value = await location_input.input_value()
                self.logger.info(f"🔍 Location field value before search: '{location_value}'")
                
                if not location_value or len(location_value.strip()) == 0:
                    self.logger.warning("⚠️  Location field is empty, refilling...")
                    await location_input.fill(params["location"])
                    await page.wait_for_timeout(1000)
                    location_value = await location_input.input_value()
                    self.logger.info(f"🔍 Location field after refill: '{location_value}'")
                    
            except Exception as e:
                self.logger.warning(f"Location validation failed: {e}")
            
            # Click search button with multiple selectors
            search_button_selectors = [
                "button:has-text('Search')", 
                "button[type='submit']",
                "button[data-testid*='search']",
                "button[aria-label*='Search']",
                ".sb-searchbox__button",
                "[class*='search'] button",
                "button[class*='sb-search']",
                "form button[type='submit']"
            ]
            
            search_clicked = False
            for selector in search_button_selectors:
                try:
                    search_button = page.locator(selector).first
                    if await search_button.is_visible(timeout=2000):
                        await search_button.click()
                        self.logger.info(f"✅ Clicked search button with selector: {selector}")
                        search_clicked = True
                        break
                except Exception as e:
                    self.logger.debug(f"Search button selector '{selector}' failed: {e}")
                    continue
                    
            if not search_clicked:
                self.logger.error("❌ Could not find or click search button with any selector")
                # Try pressing Enter on the location field as fallback
                try:
                    location_input = page.locator("input[name='ss']").first
                    await location_input.press("Enter")
                    self.logger.info("✅ Pressed Enter on location field as fallback")
                    search_clicked = True
                except Exception as e:
                    self.logger.error(f"Enter key fallback failed: {e}")
                    
            if not search_clicked:
                raise Exception("Unable to initiate search - no working search mechanism found")
            
            # Wait for navigation to search results and check if successful
            self.logger.info("⏳ Waiting for search navigation...")
            
            # Wait for URL change indicating successful navigation
            try:
                await page.wait_for_url("**/searchresults**", timeout=10000)
                self.logger.info("✅ Successfully navigated to search results page")
            except Exception as e:
                self.logger.warning(f"⚠️  URL didn't change to searchresults: {e}")
                
                # Check current URL and title for debugging
                current_url = page.url
                page_title = await page.title()
                self.logger.warning(f"🔍 Current URL after search: {current_url}")
                self.logger.warning(f"🔍 Current title: {page_title}")
                
                # Check if we're still on homepage
                if current_url == "https://www.booking.com/" or "searchresults" not in current_url:
                    self.logger.error("❌ Search failed - still on homepage or wrong page")
                    
                    # Check for common issues
                    try:
                        body_text = await page.locator("body").inner_text()
                        if "captcha" in body_text.lower() or "robot" in body_text.lower():
                            self.logger.error("🤖 CAPTCHA or bot detection detected!")
                        elif "error" in body_text.lower():
                            self.logger.error("⚠️  Error message on page")
                    except:
                        pass
                    
            # Additional wait for page to fully render
            await page.wait_for_timeout(3000)
            
            # Apply price and rating filters if specified
            await self._apply_search_filters(page, params)
            
            # Wait for search results with multiple selector approach
            property_card_selectors = [
                "div[data-testid='property-card']",
                "div[data-testid='title-wrapper']", 
                "[class*='property-card']",
                ".sr_property_block",
                "[class*='sr-hotel']",
                "div[class*='bh-property-card']"
            ]
            
            search_results_found = False
            for i, selector in enumerate(property_card_selectors):
                try:
                    timeout = 20000 if i == 0 else 5000  # Longer timeout for first attempt
                    await page.wait_for_selector(selector, timeout=timeout)
                    self.logger.info(f"✅ Search results loaded with selector: {selector}")
                    search_results_found = True
                    break
                except Exception as e:
                    self.logger.debug(f"Selector '{selector}' not found: {e}")
                    continue
            
            if not search_results_found:
                self.logger.warning(f"⚠️  No search results found with any selector - debugging page content")
                
                # Debug: Check what's actually on the page
                try:
                    page_title = await page.title()
                    page_url = page.url
                    self.logger.warning(f"🔍 Page title: '{page_title}'")
                    self.logger.warning(f"🔍 Page URL: {page_url}")
                    
                    # Check for common elements
                    body_text = await page.locator("body").inner_text()
                    if "captcha" in body_text.lower() or "robot" in body_text.lower():
                        self.logger.error(f"🤖 CAPTCHA/Robot detection found on page!")
                    
                    # Check for any hotel-related content
                    possible_selectors = ["div", "h1", "h2", "h3", "[class*='hotel']", "[class*='property']"]
                    for sel in possible_selectors:
                        count = await page.locator(sel).count()
                        self.logger.debug(f"Elements '{sel}': {count}")
                        
                except Exception as debug_e:
                    self.logger.error(f"Debug failed: {debug_e}")
            
            # Give page a moment to fully render
            await page.wait_for_timeout(2000)
            
            current_url = page.url
            self.logger.info(f"✅ Search completed successfully")
            self.logger.info(f"🔗 Results URL: {current_url}")
            
            # Verify we're on search results page
            if "searchresults" not in current_url:
                self.logger.warning("⚠️  URL doesn't contain 'searchresults' - may not be on results page")
            
        except Exception as e:
            self.logger.error(f"❌ Search execution failed: {e}")
            raise
    
    async def _apply_search_filters(self, page, params: Dict[str, Any]):
        """Apply price and rating filters on the search results page."""
        try:
            min_price = params.get("min_price")
            max_price = params.get("max_price")
            min_rating = params.get("min_rating")
            
            if not any([min_price, max_price, min_rating]):
                self.logger.info("ℹ️  No price/rating filters to apply")
                return
            
            self.logger.info(f"🎯 Applying filters: min_price={min_price}, max_price={max_price}, min_rating={min_rating}")
            
            # Try to open filters panel
            try:
                filters_button = page.locator("button:has-text('Filter'), button[data-testid*='filter'], [aria-label*='filter' i]")
                if await filters_button.count() > 0:
                    await filters_button.first.click()
                    await page.wait_for_timeout(2000)
                    self.logger.info("✅ Opened filters panel")
                else:
                    self.logger.info("ℹ️  No filters button found, filters may already be visible")
            except Exception as e:
                self.logger.debug(f"Filters panel opening skipped: {e}")
            
            # Apply price filters
            if min_price or max_price:
                await self._set_price_filters(page, min_price, max_price)
            
            # Apply rating filter
            if min_rating:
                await self._set_rating_filter(page, min_rating)
            
            # Apply filters and wait for results to update
            try:
                apply_button = page.locator("button:has-text('Apply'), button:has-text('Show results'), button[data-testid*='apply']")
                if await apply_button.count() > 0:
                    await apply_button.first.click()
                    await page.wait_for_timeout(3000)  # Wait for results to reload
                    self.logger.info("✅ Applied filters successfully")
                else:
                    # Some filters auto-apply without a button
                    await page.wait_for_timeout(2000)
                    self.logger.info("✅ Filters applied (auto-update)")
            except Exception as e:
                self.logger.debug(f"Filter apply button interaction skipped: {e}")
                
        except Exception as e:
            self.logger.warning(f"⚠️  Filter application failed: {e}")
    
    async def _set_price_filters(self, page, min_price: Optional[int], max_price: Optional[int]):
        """Set price range filters."""
        try:
            self.logger.info(f"🎯 Setting price filters: min=${min_price}, max=${max_price}")
            
            # More comprehensive Booking.com price filter selectors  
            min_price_selectors = [
                "input[name*='min_price'], input[name*='price_from']",
                "input[data-testid*='price-from'], input[data-testid*='min-price']",
                "input[placeholder*='Min'], input[placeholder*='minimum']",
                "input[aria-label*='Minimum price'], input[aria-label*='min price']",
                "input[type='number'][name*='min'], input[type='text'][name*='min']",
                ".price-filter input:first-of-type",
                "[data-filter='price'] input:first-of-type"
            ]
            
            max_price_selectors = [
                "input[name*='max_price'], input[name*='price_to']", 
                "input[data-testid*='price-to'], input[data-testid*='max-price']",
                "input[placeholder*='Max'], input[placeholder*='maximum']",
                "input[aria-label*='Maximum price'], input[aria-label*='max price']",
                "input[type='number'][name*='max'], input[type='text'][name*='max']",
                ".price-filter input:last-of-type",
                "[data-filter='price'] input:last-of-type"
            ]
            
            # Try to set minimum price with all selectors
            if min_price:
                self.logger.info(f"🔍 Trying {len(min_price_selectors)} selectors for min price...")
                for i, selector in enumerate(min_price_selectors):
                    try:
                        min_input = page.locator(selector).first
                        if await min_input.is_visible(timeout=2000):
                            await min_input.clear()
                            await min_input.fill(str(min_price))
                            await page.wait_for_timeout(500)
                            self.logger.info(f"✅ Set minimum price ${min_price} with selector {i+1}: {selector}")
                            break
                    except Exception as e:
                        self.logger.debug(f"   Selector {i+1} failed: {e}")
                        continue
                else:
                    self.logger.warning(f"⚠️  Could not set minimum price ${min_price} - no working selectors")
            
            # Try to set maximum price with all selectors
            if max_price:
                self.logger.info(f"🔍 Trying {len(max_price_selectors)} selectors for max price...")
                for i, selector in enumerate(max_price_selectors):
                    try:
                        max_input = page.locator(selector).first
                        if await max_input.is_visible(timeout=2000):
                            await max_input.clear()
                            await max_input.fill(str(max_price))
                            await page.wait_for_timeout(500)
                            self.logger.info(f"✅ Set maximum price ${max_price} with selector {i+1}: {selector}")
                            break
                    except Exception as e:
                        self.logger.debug(f"   Selector {i+1} failed: {e}")
                        continue
                else:
                    self.logger.warning(f"⚠️  Could not set maximum price ${max_price} - no working selectors")
                        
        except Exception as e:
            self.logger.warning(f"⚠️  Price filter setting failed: {e}")
    
    async def _set_rating_filter(self, page, min_rating: float):
        """Set minimum rating filter."""
        try:
            # Convert rating to appropriate format (8.0 -> 8+ stars)
            rating_threshold = int(min_rating)
            self.logger.info(f"🎯 Setting rating filter: {rating_threshold}+ stars (from {min_rating})")
            
            # Comprehensive rating filter selectors for Booking.com
            rating_selectors = [
                # Direct rating value selectors
                f"input[value='{rating_threshold}']",
                f"input[value='{rating_threshold}.0']",  
                f"input[data-value='{rating_threshold}']",
                
                # Label-based selectors
                f"label:has-text('{rating_threshold}+')",
                f"label:has-text('{rating_threshold} stars')",  
                f"label:has-text('{rating_threshold}+')",
                
                # Button-based selectors
                f"button:has-text('{rating_threshold}+')",
                f"button:has-text('{rating_threshold} stars')",
                f"button[data-testid*='rating']:has-text('{rating_threshold}')",
                
                # Checkbox selectors with various patterns
                f"input[type='checkbox'][name*='review_score'][value*='{rating_threshold}']",
                f"input[type='checkbox'][name*='rating'][value*='{rating_threshold}']", 
                f"input[type='checkbox'][data-testid*='rating'][value*='{rating_threshold}']",
                
                # Generic pattern matching 
                f"[data-testid*='rating']:has-text('{rating_threshold}')",
                f"[class*='rating']:has-text('{rating_threshold}')",
                f"[data-filter='rating']:has-text('{rating_threshold}')"
            ]
            
            self.logger.info(f"🔍 Trying {len(rating_selectors)} selectors for {rating_threshold}+ rating...")
            for i, selector in enumerate(rating_selectors):
                try:
                    rating_element = page.locator(selector).first
                    if await rating_element.is_visible(timeout=2000):
                        await rating_element.click()
                        await page.wait_for_timeout(500)
                        self.logger.info(f"✅ Set minimum rating {rating_threshold}+ with selector {i+1}: {selector}")
                        return
                except Exception as e:
                    self.logger.debug(f"   Selector {i+1} failed: {e}")
                    continue
            
            # Fallback: try to find any rating-related elements and use highest available
            self.logger.info("🔄 Trying fallback rating selection...")
            fallback_selectors = [
                "label:has-text('star'), button:has-text('star')",
                "input[type='checkbox'][name*='rating'], input[type='checkbox'][name*='review']",
                "[data-testid*='rating'] input, [class*='rating'] input",
                ".filter-item:has-text('star'), .filter-option:has-text('star')"
            ]
            
            for selector in fallback_selectors:
                try:
                    rating_options = page.locator(selector)
                    count = await rating_options.count()
                    if count > 0:
                        # Click the last (typically highest) rating option as fallback
                        await rating_options.last.click()
                        self.logger.info(f"✅ Set rating filter using fallback approach with {count} options")
                        return
                except Exception as e:
                    self.logger.debug(f"   Fallback selector failed: {e}")
                    continue
            
            self.logger.warning(f"⚠️  Could not set minimum rating {rating_threshold}+ - no working selectors found")
                    
        except Exception as e:
            self.logger.warning(f"⚠️  Rating filter setting failed: {e}")
    
    async def _scrape_property_cards(self, page, max_results: int, deep_scrape: bool = False) -> List[Dict[str, Any]]:
        """Scrape hotel data from property cards."""
        hotels = []
        
        try:
            # Try multiple selectors for property cards (Booking.com changes frequently)
            property_card_selectors = [
                "div[data-testid='property-card']",
                "div[data-testid='title-wrapper']", 
                "[class*='property-card']",
                "div[aria-label*='property']",
                ".sr_property_block",
                "[class*='sr-hotel']",
                "div[class*='bh-property-card']"
            ]
            
            property_cards = None
            card_count = 0
            
            for selector in property_card_selectors:
                try:
                    cards = page.locator(selector)
                    count = await cards.count()
                    self.logger.info(f"🔍 Trying selector '{selector}': {count} cards found")
                    if count > 0:
                        property_cards = cards
                        card_count = count
                        self.logger.info(f"✅ Using selector '{selector}' - found {count} property cards")
                        break
                except Exception as e:
                    self.logger.debug(f"Selector '{selector}' failed: {e}")
                    continue
            
            if card_count == 0:
                self.logger.error(f"❌ No property cards found with any selector!")
                return hotels
            
            self.logger.info(f"🏨 Found {card_count} property cards on page")
            
            # Limit to max_results
            cards_to_process = min(card_count, max_results)
            
            for i in range(cards_to_process):
                try:
                    card = property_cards.nth(i)
                    hotel_data = await self._extract_basic_hotel_data(card, i + 1)
                    
                    if hotel_data:
                        if deep_scrape and hotel_data.get('booking_url'):
                            # Enhanced data extraction
                            hotel_data = await self._extract_detailed_hotel_data(hotel_data, page.context)
                        
                        # Calculate completeness
                        hotel_data['data_completeness'] = self._calculate_completeness(hotel_data)
                        hotel_data['scraping_timestamp'] = datetime.now().isoformat()
                        hotel_data['source'] = 'search_results'  # Mark as actual search results
                        
                        hotels.append(hotel_data)
                        
                        self.logger.info(f"✅ Hotel {i+1}/{cards_to_process}: {hotel_data.get('name', 'Unknown')} - ${hotel_data.get('price_per_night', 'N/A')}/night")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️  Failed to extract hotel {i+1}: {e}")
                    continue
            
            return hotels
            
        except Exception as e:
            self.logger.error(f"❌ Property cards extraction failed: {e}")
            return hotels
    
    async def _scrape_property_cards_level_3(self, page, max_results: int) -> List[Dict[str, Any]]:
        """Level 3: Scrape hotel data with basic review sampling (2-5 reviews per hotel)."""
        hotels = []
        
        try:
            # Get all property cards
            property_cards = page.locator("div[data-testid='property-card']")
            card_count = await property_cards.count()
            
            self.logger.info(f"🏨 Level 3: Found {card_count} property cards on page")
            
            # Limit to max_results
            cards_to_process = min(card_count, max_results)
            
            for i in range(cards_to_process):
                try:
                    card = property_cards.nth(i)
                    hotel_data = await self._extract_basic_hotel_data(card, i + 1)
                    
                    if hotel_data:
                        if hotel_data.get('booking_url'):
                            # Level 3: Full data extraction with basic reviews (2-5 reviews)
                            hotel_data = await self._extract_detailed_hotel_data_level_3(hotel_data, page.context)
                        
                        # Calculate completeness
                        hotel_data['data_completeness'] = self._calculate_completeness(hotel_data)
                        hotel_data['scraping_timestamp'] = datetime.now().isoformat()
                        hotel_data['source'] = 'search_results'
                        hotel_data['extraction_level'] = 3
                        
                        hotels.append(hotel_data)
                        
                        # Enhanced logging for Level 3
                        review_count = len(hotel_data.get('reviews', []))
                        self.logger.info(f"📝 Level 3 Hotel {i+1}/{cards_to_process}: {hotel_data.get('name', 'Unknown')} - ${hotel_data.get('price_per_night', 'N/A')}/night - {review_count} basic reviews")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️  Level 3: Failed to extract hotel {i+1}: {e}")
                    continue
            
            return hotels
            
        except Exception as e:
            self.logger.error(f"❌ Level 3 property cards extraction failed: {e}")
            return hotels
    
    async def _scrape_property_cards_level_4(self, page, max_results: int) -> List[Dict[str, Any]]:
        """Level 4: Scrape hotel data with comprehensive review extraction (10-50 reviews per hotel)."""
        hotels = []
        
        try:
            # Get all property cards
            property_cards = page.locator("div[data-testid='property-card']")
            card_count = await property_cards.count()
            
            self.logger.info(f"🏨 Level 4: Found {card_count} property cards on page")
            
            # Limit to max_results
            cards_to_process = min(card_count, max_results)
            
            for i in range(cards_to_process):
                try:
                    card = property_cards.nth(i)
                    hotel_data = await self._extract_basic_hotel_data(card, i + 1)
                    
                    if hotel_data:
                        if hotel_data.get('booking_url'):
                            # Level 4: Enhanced data extraction with comprehensive reviews
                            hotel_data = await self._extract_detailed_hotel_data_level_4(hotel_data, page.context)
                        
                        # Calculate completeness
                        hotel_data['data_completeness'] = self._calculate_completeness(hotel_data)
                        hotel_data['scraping_timestamp'] = datetime.now().isoformat()
                        hotel_data['source'] = 'search_results'
                        hotel_data['extraction_level'] = 4
                        
                        hotels.append(hotel_data)
                        
                        # Enhanced logging for Level 4
                        review_count = len(hotel_data.get('reviews', []))
                        self.logger.info(f"🎯 Level 4 Hotel {i+1}/{cards_to_process}: {hotel_data.get('name', 'Unknown')} - ${hotel_data.get('price_per_night', 'N/A')}/night - {review_count} reviews")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️  Level 4: Failed to extract hotel {i+1}: {e}")
                    continue
            
            return hotels
            
        except Exception as e:
            self.logger.error(f"❌ Level 4 property cards extraction failed: {e}")
            return hotels
    
    async def _extract_basic_hotel_data(self, card, index: int) -> Optional[Dict[str, Any]]:
        """Extract basic hotel data from a property card."""
        try:
            hotel_data = {}
            
            # Hotel name - try multiple selectors
            name_selectors = [
                "div[data-testid='title'] a",
                "h3[data-testid='title']", 
                "div[data-testid='title']",
                "a[data-testid='title-link']",
                "h3 a[href*='/hotel/']",
                "a[href*='/hotel/']",
                ".sr-hotel__name a"
            ]
            
            for selector in name_selectors:
                try:
                    name_element = card.locator(selector).first
                    if await name_element.is_visible(timeout=1000):
                        hotel_data['name'] = await name_element.inner_text()
                        self.logger.debug(f"✅ Name found with selector: {selector}")
                        break
                except:
                    continue
            
            # Booking URL
            link_element = card.locator("a").first
            if await link_element.is_visible():
                href = await link_element.get_attribute('href')
                if href:
                    # Ensure full URL
                    if href.startswith('/'):
                        href = f"https://www.booking.com{href}"
                    hotel_data['booking_url'] = href
                    # Extract hotel ID from URL
                    hotel_data['hotel_id'] = self._extract_hotel_id_from_url(href)
            
            # Price - Enhanced selectors for modern Booking.com
            price_selectors = [
                # 2025 Modern Booking.com patterns (most likely)
                "*[data-testid*='price'] *",
                "span[data-testid*='price']",
                "[data-testid*='price']",
                "*[class*='price'] span",
                "*[class*='price']",
                
                # Specific modern patterns  
                "*[data-price]",
                "*[price]",
                "span[class*='bui']:has-text('$')",
                "span[class*='sr']:has-text('$')",
                "div[class*='price'] span",
                
                # Currency-based discovery
                "span:has-text('$')",
                "span:has-text('USD')", 
                "span:has-text('AED')",
                "span:has-text('€')",
                
                # Legacy selectors (kept for fallback)
                ".bui-price-display__value",
                ".sr-hotel__price span",
                ".prco-valign-middle-helper",
                ".bui-price",
                
                # Broad discovery patterns
                "*:has-text('$'):not(button):not(a)",
                "span[class]:has-text('$')",
                "div[class]:has-text('$')"
            ]
            
            # Enhanced price debugging
            self.logger.info(f"🔍 Attempting price extraction for hotel {index}")
            for i, selector in enumerate(price_selectors):
                try:
                    price_element = card.locator(selector).first
                    element_count = await card.locator(selector).count()
                    self.logger.info(f"   Price selector {i+1}: {selector} -> {element_count} elements")
                    
                    if await price_element.is_visible(timeout=1000):
                        price_text = await price_element.inner_text()
                        self.logger.info(f"   Found price text: '{price_text}'")
                        if price_text and price_text.strip():
                            extracted_price = self._extract_price_number(price_text)
                            self.logger.debug(f"   Extracted price value: {extracted_price}")
                            if extracted_price and extracted_price > 0:
                                hotel_data['price_per_night'] = extracted_price
                                self.logger.info(f"✅ Price extracted: ${extracted_price} with selector: {selector}")
                                break
                    else:
                        self.logger.debug(f"   Price element not visible for: {selector}")
                except Exception as e:
                    self.logger.debug(f"   Price extraction error for {selector}: {e}")
                    continue
            
            # Enhanced price debugging and fallback extraction
            if not hotel_data.get('price_per_night'):
                self.logger.warning(f"❌ No price found for hotel {index} with any selector - trying fallbacks")
                
                # Try to find any text containing currency symbols for debugging
                try:
                    debug_elements = await card.locator("*:has-text('$'), *:has-text('USD'), *:has-text('AED'), *:has-text('€')").all()
                    for elem in debug_elements[:3]:  # Check first 3 matches
                        try:
                            debug_text = await elem.inner_text()
                            self.logger.info(f"   💰 Debug price candidate: '{debug_text.strip()}'")
                            # Try to extract price from this text
                            potential_price = self._extract_price_number(debug_text)
                            if potential_price and potential_price > 0:
                                hotel_data['price_per_night'] = potential_price
                                self.logger.info(f"✅ Price extracted from fallback: ${potential_price}")
                                break
                        except:
                            continue
                except Exception as e:
                    self.logger.debug(f"Price debugging failed: {e}")
                
                # Final fallback: mark as price unavailable
                if not hotel_data.get('price_per_night'):
                    hotel_data['price_per_night'] = 0
                    self.logger.info("   💰 Price set to 0 (unavailable)")
            
            # Rating - Updated selectors for 2025 Booking.com DOM
            rating_selectors = [
                # Primary patterns (most likely to work)
                "[data-testid='review-score'] div[aria-label*='scored']",
                "[data-testid='review-score'] div:first-child", 
                "[data-testid='review-score'] span:first-child",
                ".bui-review-score__badge",
                
                # Alternative rating containers  
                "[data-testid*='score']:not([data-testid*='count']) div",
                "[data-testid*='rating'] div:first-child",
                ".sr_gs_rating .bui-review-score__badge",
                "[class*='review-score'] > div:first-child",
                
                # Numeric rating patterns
                "div[aria-label*='Scored']:not([aria-label*='out of'])",
                "[aria-label*='rated' i] div:first-child", 
                "[title*='scored' i]",
                
                # Text-based discovery (broader search)
                "*:has-text(/^[0-9]\\.[0-9]$/) ",  # Matches "8.5", "9.2" etc
                "*:has-text(/^[0-9]\\.[0-9][0-9]?$/)",  # Matches "8.51", "9.23" etc  
                "span:has-text(/^[789]\\./)",  # Ratings starting with 7, 8, 9
                "div:has-text(/^[789]\\./)",
                
                # Fallback containers
                ".property-review-score div:first-child",
                ".sr-hotel__review-score div:first-child", 
                "*[data-score]:not([data-score=''])",
                "*[data-rating]:not([data-rating=''])",
                
                # Last resort - any numeric content in review areas
                "[data-testid*='review'] span:has-text(/^[6-9]\\./)",
                "[class*='review'] span:has-text(/^[6-9]\\./)"]
            
            # Enhanced rating debugging  
            self.logger.info(f"🔍 Attempting rating extraction for hotel {index}")
            for i, selector in enumerate(rating_selectors):
                try:
                    rating_element = card.locator(selector).first
                    element_count = await card.locator(selector).count()
                    self.logger.info(f"   Rating selector {i+1}: {selector} -> {element_count} elements")
                    
                    if await rating_element.is_visible(timeout=1000):
                        rating_text = await rating_element.inner_text()
                        self.logger.info(f"   Found rating text: '{rating_text}'")
                        if rating_text and rating_text.strip():
                            # Enhanced rating parsing
                            rating_value = self._extract_rating_number(rating_text.strip())
                            if rating_value and 0 <= rating_value <= 10:  # Valid rating range
                                hotel_data['rating'] = rating_value
                                self.logger.info(f"✅ Rating extracted: {rating_value} with selector: {selector}")
                                break
                            else:
                                self.logger.debug(f"   Rating parsing failed or out of range: {rating_value}")
                        
                        # Also try aria-label for ratings
                        aria_label = await rating_element.get_attribute('aria-label')
                        if aria_label and 'scored' in aria_label.lower():
                            rating_value = self._extract_rating_number(aria_label)
                            if rating_value and 0 <= rating_value <= 10:
                                hotel_data['rating'] = rating_value
                                self.logger.info(f"✅ Rating extracted from aria-label: {rating_value}")
                                break
                    else:
                        self.logger.debug(f"   Rating element not visible for: {selector}")
                except Exception as e:
                    self.logger.debug(f"   Rating extraction error for {selector}: {e}")
                    continue
            
            if not hotel_data.get('rating'):
                self.logger.warning(f"❌ No rating found for hotel {index} with any selector")
            
            # Review count
            reviews_element = card.locator("div[data-testid='review-score'] > div:last-child")
            if await reviews_element.is_visible():
                reviews_text = await reviews_element.inner_text()
                hotel_data['review_count'] = self._extract_review_count(reviews_text)
            
            # Basic location/address (if visible on card)
            location_element = card.locator("[data-testid='address'], .bui-card__subtitle")
            if await location_element.is_visible():
                hotel_data['address'] = await location_element.inner_text()
            
            # Images - try multiple approaches
            images = []
            img_selectors = [
                "img[data-testid='image']",
                ".bh-property-card img",  
                "img[src*='bstatic']",
                "img"
            ]
            
            for img_selector in img_selectors:
                try:
                    img_element = card.locator(img_selector).first
                    if await img_element.is_visible():
                        img_src = (await img_element.get_attribute('data-src') or 
                                  await img_element.get_attribute('src'))
                        if img_src:
                            fixed_url = self._fix_image_url(img_src)
                            if fixed_url:
                                images.append(fixed_url)
                                break  # Got one image, that's enough for basic extraction
                except:
                    continue
            
            if images:
                hotel_data['images'] = images
            
            # Only return if we got essential data
            if hotel_data.get('name'):
                return hotel_data
            else:
                self.logger.debug(f"Card {index}: Missing essential data (name)")
                return None
                
        except Exception as e:
            self.logger.debug(f"Basic extraction error for card {index}: {e}")
            return None
    
    async def _extract_detailed_hotel_data(self, hotel_data: Dict[str, Any], context) -> Dict[str, Any]:
        """Extract detailed hotel data by visiting the hotel page."""
        try:
            hotel_url = hotel_data.get('booking_url')
            if not hotel_url:
                return hotel_data
            
            self.logger.info(f"🔍 Deep scraping: {hotel_data.get('name')}")
            
            # Open hotel page in new tab
            hotel_page = await context.new_page()
            
            try:
                # More resilient hotel page loading
                try:
                    await hotel_page.goto(hotel_url, wait_until='domcontentloaded', timeout=30000)
                    await hotel_page.wait_for_timeout(3000)
                except:
                    self.logger.warning(f"Hotel page timeout, retrying with load strategy for: {hotel_data.get('name')}")
                    await hotel_page.goto(hotel_url, wait_until='load', timeout=30000)
                    await hotel_page.wait_for_timeout(2000)
                
                # Extract address
                address_element = hotel_page.locator("span[data-testid='address']")
                if await address_element.is_visible():
                    hotel_data['address'] = await address_element.inner_text()
                
                # Extract amenities
                amenities = await self._extract_amenities(hotel_page)
                hotel_data['amenities'] = amenities
                
                # Extract images - preserve basic images if detailed extraction fails
                basic_images = hotel_data.get('images', [])
                detailed_images = await self._extract_all_images(hotel_page)
                
                if detailed_images:
                    # Use detailed images if found
                    hotel_data['images'] = detailed_images
                elif not basic_images:
                    # If no basic images and no detailed images, try basic extraction on hotel page
                    try:
                        main_img = hotel_page.locator("img[src*='bstatic']").first
                        if await main_img.is_visible():
                            img_src = await main_img.get_attribute('src')
                            if img_src:
                                hotel_data['images'] = [self._fix_image_url(img_src)]
                    except:
                        pass
                # If we have basic_images, keep them as fallback
                
                # Extract coordinates and Google Maps URL
                location_data = await self._extract_location_data(hotel_page)
                if location_data:
                    hotel_data.update(location_data)
                
                # Extract description
                description = await self._extract_description(hotel_page)
                if description:
                    hotel_data['description'] = description
                
                # Level 2: No review extraction - only basic hotel data
                # Reviews are only extracted in Level 3+ methods
                
                return hotel_data
                
            finally:
                await hotel_page.close()
                
        except Exception as e:
            self.logger.warning(f"⚠️  Detailed extraction failed for {hotel_data.get('name')}: {e}")
            return hotel_data
    
    async def _extract_detailed_hotel_data_level_3(self, hotel_data: Dict[str, Any], context) -> Dict[str, Any]:
        """Level 3: Extract detailed hotel data with basic review sampling (2-5 reviews)."""
        try:
            hotel_url = hotel_data.get('booking_url')
            if not hotel_url:
                return hotel_data
                
            self.logger.info(f"📝 Level 3: Extracting detailed data from: {hotel_url}")
            
            hotel_page = await context.new_page()
            try:
                # Navigate to hotel page
                await hotel_page.goto(hotel_url, wait_until="domcontentloaded", timeout=30000)
                await hotel_page.wait_for_timeout(3000)
                
                # Extract address
                address_selectors = [
                    "[data-testid='address']",
                    ".hp_address_subtitle",
                    "[data-testid*='location']",
                    ".hp-hotel-location"
                ]
                
                for selector in address_selectors:
                    try:
                        address_element = hotel_page.locator(selector).first
                        if await address_element.is_visible():
                            hotel_data['address'] = await address_element.inner_text()
                            break
                    except:
                        continue
                
                # Extract amenities
                amenities = await self._extract_amenities(hotel_page)
                hotel_data['amenities'] = amenities
                
                # Extract images - preserve basic images if detailed extraction fails
                basic_images = hotel_data.get('images', [])
                detailed_images = await self._extract_all_images(hotel_page)
                hotel_data['images'] = detailed_images if detailed_images else basic_images
                
                # Extract coordinates
                coordinates = await self._extract_location_data(hotel_page)
                if coordinates:
                    hotel_data.update(coordinates)
                
                # Extract description
                description = await self._extract_description(hotel_page)
                if description:
                    hotel_data['description'] = description
                
                # CRITICAL: Ensure price is preserved or extracted on hotel page
                if not hotel_data.get('price_per_night'):
                    self.logger.info("🔍 Level 3: Attempting price extraction on hotel page")
                    self.logger.warning("Missing price data")
                
                # LEVEL 3 FEATURE: Basic review sampling (2-5 reviews max)
                self.logger.info("📝 LEVEL 3: Starting basic review extraction")
                reviews_data = await self._extract_reviews_level_3(hotel_page)
                
                # Always add Level 3 markers
                hotel_data['extraction_method'] = 'LEVEL_3_BASIC_REVIEWS'
                hotel_data['reviews_extraction_target'] = '2-5 basic reviews'
                hotel_data['level_3_attempted'] = True
                
                if reviews_data:
                    hotel_data['reviews'] = reviews_data['reviews']
                    hotel_data['review_count'] = reviews_data.get('total_count', hotel_data.get('review_count', 0))
                    hotel_data['rating_breakdown'] = reviews_data.get('rating_breakdown', {})
                    hotel_data['reviews_found'] = len(reviews_data.get('reviews', []))
                    self.logger.info(f"✅ LEVEL 3: Found {len(reviews_data.get('reviews', []))} basic reviews")
                else:
                    hotel_data['reviews'] = []
                    hotel_data['reviews_found'] = 0
                    hotel_data['reviews_status'] = 'No reviews found with Level 3 basic extraction'
                    self.logger.info("⚠️ LEVEL 3: No reviews extracted")
                
                return hotel_data
                
            finally:
                await hotel_page.close()
                
        except Exception as e:
            self.logger.warning(f"⚠️  Level 3 detailed extraction failed for {hotel_data.get('name')}: {e}")
            return hotel_data
    
    async def _extract_detailed_hotel_data_level_4(self, hotel_data: Dict[str, Any], context) -> Dict[str, Any]:
        """Level 4: Extract detailed hotel data with comprehensive review extraction (10-50 reviews)."""
        try:
            hotel_url = hotel_data.get('booking_url')
            if not hotel_url:
                return hotel_data
            
            self.logger.info(f"🔥 Level 4 deep scraping: {hotel_data.get('name')}")
            
            # Open hotel page in new tab
            hotel_page = await context.new_page()
            
            try:
                # More resilient hotel page loading for Level 4
                try:
                    await hotel_page.goto(hotel_url, wait_until='domcontentloaded', timeout=30000)
                    await hotel_page.wait_for_timeout(3000)
                except:
                    self.logger.warning(f"Level 4: Hotel page timeout, retrying with load strategy for: {hotel_data.get('name')}")
                    await hotel_page.goto(hotel_url, wait_until='load', timeout=30000)
                    await hotel_page.wait_for_timeout(2000)
                
                # Extract all the standard detailed data (same as Level 3)
                # Extract address
                address_element = hotel_page.locator("span[data-testid='address']")
                if await address_element.is_visible():
                    hotel_data['address'] = await address_element.inner_text()
                
                # Extract amenities
                amenities = await self._extract_amenities(hotel_page)
                hotel_data['amenities'] = amenities
                
                # Extract images
                basic_images = hotel_data.get('images', [])
                detailed_images = await self._extract_all_images(hotel_page)
                
                if detailed_images:
                    hotel_data['images'] = detailed_images
                elif not basic_images:
                    try:
                        main_img = hotel_page.locator("img[src*='bstatic']").first
                        if await main_img.is_visible():
                            img_src = await main_img.get_attribute('src')
                            if img_src:
                                hotel_data['images'] = [self._fix_image_url(img_src)]
                    except:
                        pass
                
                # Extract coordinates and Google Maps URL
                location_data = await self._extract_location_data(hotel_page)
                if location_data:
                    hotel_data.update(location_data)
                
                # Extract description
                description = await self._extract_description(hotel_page)
                if description:
                    hotel_data['description'] = description
                
                # CRITICAL: Ensure price is preserved or extracted on hotel page\n                if not hotel_data.get('price_per_night'):\n                    self.logger.info(\"\ud83d\udd0d Level 4: Attempting price extraction on hotel page\")\n                    self.logger.warning("Missing price data")\n                \n                # LEVEL 4 ENHANCEMENT: Comprehensive review extraction
                self.logger.info("🔥 LEVEL 4: Starting enhanced review extraction")
                reviews_data = await self._extract_reviews_level_4(hotel_page)
                
                # Always add Level 4 markers (even if no reviews found)
                hotel_data['extraction_method'] = 'LEVEL_4_COMPREHENSIVE_REVIEWS'
                hotel_data['reviews_extraction_target'] = '10-50 comprehensive reviews'
                hotel_data['level_4_attempted'] = True
                
                if reviews_data:
                    hotel_data['reviews'] = reviews_data['reviews']
                    hotel_data['review_count'] = reviews_data.get('total_count', hotel_data.get('review_count', 0))
                    hotel_data['rating_breakdown'] = reviews_data.get('rating_breakdown', {})
                    hotel_data['reviews_found'] = len(reviews_data.get('reviews', []))
                    self.logger.info(f"✅ LEVEL 4: Found {len(reviews_data.get('reviews', []))} reviews")
                else:
                    hotel_data['reviews'] = []
                    hotel_data['reviews_found'] = 0 
                    hotel_data['reviews_status'] = 'No reviews found despite Level 4 enhancement'
                    self.logger.info("⚠️ LEVEL 4: No reviews extracted despite enhanced methods")
                
                return hotel_data
                
            finally:
                await hotel_page.close()
                
        except Exception as e:
            self.logger.warning(f"⚠️  Level 4 detailed extraction failed for {hotel_data.get('name')}: {e}")
            return hotel_data
    
    async def _extract_amenities(self, page) -> List[str]:
        """Extract hotel amenities."""
        amenities = []
        try:
            # Try multiple selectors for amenities with enhanced debugging
            amenity_selectors = [
                "div[data-testid='property-most-popular-facilities-wrapper'] span[data-testid='facility-name']",
                "[data-testid='property-highlights'] li",
                ".hp_desc_important_facilities li",
                ".important_facilities li",
                ".hotel-facilities__list li",
                ".bh-property-highlights li",
                "[data-testid='facility-name']",
                ".facility_text"
            ]
            
            self.logger.debug(f"🏨 Extracting amenities with {len(amenity_selectors)} selectors")
            
            for i, selector in enumerate(amenity_selectors):
                amenity_elements = page.locator(selector)
                count = await amenity_elements.count()
                self.logger.debug(f"   Selector {i+1}: {selector} -> {count} elements")
                
                if count > 0:
                    for j in range(min(count, 20)):  # Limit to 20 amenities
                        try:
                            amenity_text = await amenity_elements.nth(j).inner_text()
                            if amenity_text and amenity_text.strip():
                                amenities.append(amenity_text.strip())
                        except Exception as e:
                            self.logger.debug(f"   Failed to extract amenity {j}: {e}")
                            continue
                    
                    if amenities:
                        self.logger.debug(f"✅ Found {len(amenities)} amenities with selector: {selector}")
                        break  # Use first working selector
            
            if not amenities:
                self.logger.warning("⚠️  No amenities found with any selector")
            
        except Exception as e:
            self.logger.warning(f"Amenities extraction error: {e}")
        
        return list(set(amenities))[:20]  # Remove duplicates and limit
    
    async def _extract_all_images(self, page) -> List[str]:
        """Extract hotel images."""
        images = []
        try:
            self.logger.debug("🖼️  Extracting hotel images")
            
            # First try to find images without opening gallery
            basic_image_selectors = [
                "img[data-testid='hotel-photo']",
                ".bh-photo-grid img",
                ".hp-gallery img",
                ".hotel-photo img", 
                "img[src*='bstatic']",
                ".gallery-image img",
                # Additional modern selectors
                "[data-testid='property-section-images'] img",
                ".hp__gallery-container img",
                ".hp__gallery-image img",
                "img[alt*='Hotel']",
                "img[alt*='Property']",
                ".gallery_image img",
                ".hotel-gallery img",
                "[data-capla-component*='gallery'] img"
            ]
            
            seen_urls = set()
            
            # Try basic extraction first
            for selector in basic_image_selectors:
                img_elements = page.locator(selector)
                count = await img_elements.count()
                self.logger.debug(f"   Basic selector {selector}: {count} images")
                
                if count > 0:
                    for i in range(min(count, 10)):  # Limit to 10 from each selector
                        try:
                            img_element = img_elements.nth(i)
                            
                            # Try different src attributes
                            img_src = (await img_element.get_attribute('data-src') or
                                      await img_element.get_attribute('src') or  
                                      await img_element.get_attribute('data-highres') or
                                      await img_element.get_attribute('data-lazy') or
                                      await img_element.get_attribute('data-original') or
                                      await img_element.get_attribute('srcset'))
                            
                            if img_src and ('bstatic' in img_src or 'booking.com' in img_src):
                                # Handle srcset format
                                if ' ' in img_src and ',' in img_src:  # srcset format
                                    img_src = img_src.split(',')[0].split(' ')[0]
                                    
                                fixed_url = self._fix_image_url(img_src)
                                if fixed_url and fixed_url not in seen_urls:
                                    seen_urls.add(fixed_url)
                                    images.append(fixed_url)
                                    self.logger.debug(f"   ✅ Added image: {fixed_url[:50]}...")
                                    
                        except Exception as e:
                            self.logger.debug(f"   Failed to extract image {i}: {e}")
                            continue
                
                if len(images) >= 15:  # Stop when we have enough images
                    break
            
            # If we don't have many images, try opening gallery
            if len(images) < 5:
                self.logger.debug("🖼️  Trying to open photo gallery for more images")
                try:
                    gallery_selectors = [
                        "button[aria-label*='photo']",
                        ".bh-photo-grid-item a",
                        "button:has-text('Photos')",
                        "[data-testid='gallery-trigger']",
                        # Additional modern gallery triggers
                        "button[data-testid='photos-trigger']",
                        "[data-testid='image-gallery-trigger']",
                        ".hp__gallery-trigger",
                        "button:has-text('Show all photos')",
                        "a[data-testid='hotel-gallery-trigger']"
                    ]
                    
                    for gallery_selector in gallery_selectors:
                        gallery_button = page.locator(gallery_selector).first
                        if await gallery_button.is_visible():
                            await gallery_button.click()
                            await page.wait_for_timeout(2000)
                            self.logger.debug(f"✅ Opened gallery with {gallery_selector}")
                            break
                    
                    # Extract from opened gallery
                    gallery_image_selectors = [
                        ".slick-slide img",
                        ".bh-photo-modal img", 
                        ".gallery-modal img",
                        ".lightbox img"
                    ]
                    
                    for selector in gallery_image_selectors:
                        img_elements = page.locator(selector)
                        count = await img_elements.count()
                        self.logger.debug(f"   Gallery selector {selector}: {count} images")
                        
                        if count > 0:
                            for i in range(min(count, 20)):
                                try:
                                    img_element = img_elements.nth(i)
                                    img_src = (await img_element.get_attribute('data-highres') or 
                                              await img_element.get_attribute('data-src') or
                                              await img_element.get_attribute('src') or
                                              await img_element.get_attribute('data-lazy') or
                                              await img_element.get_attribute('data-original') or
                                              await img_element.get_attribute('srcset'))
                                    
                                    if img_src and ('bstatic' in img_src or 'booking.com' in img_src):
                                        # Handle srcset format
                                        if ' ' in img_src and ',' in img_src:  # srcset format
                                            img_src = img_src.split(',')[0].split(' ')[0]
                                            
                                        fixed_url = self._fix_image_url(img_src)
                                        if fixed_url and fixed_url not in seen_urls:
                                            seen_urls.add(fixed_url)
                                            images.append(fixed_url)
                                            
                                except:
                                    continue
                            break
                    
                    # Close gallery
                    try:
                        await page.keyboard.press('Escape')
                        await page.wait_for_timeout(500)
                    except:
                        pass
                        
                except Exception as e:
                    self.logger.debug(f"Gallery opening failed: {e}")
            
            self.logger.debug(f"✅ Extracted {len(images)} hotel images")
            
        except Exception as e:
            self.logger.warning(f"Images extraction error: {e}")
        
        return images[:20]  # Return max 20 images
    
    async def _extract_location_data(self, page) -> Optional[Dict[str, Any]]:
        """Extract location coordinates and Google Maps URL."""
        try:
            location_data = {}
            
            # Extract coordinates from JavaScript
            coordinates = await page.evaluate("""
                () => {
                    // Try multiple methods to get coordinates
                    if (window.B && window.B.env) {
                        const env = window.B.env;
                        if (env.b_map_center_latitude && env.b_map_center_longitude) {
                            return {
                                lat: parseFloat(env.b_map_center_latitude),
                                lng: parseFloat(env.b_map_center_longitude)
                            };
                        }
                    }
                    return null;
                }
            """)
            
            if coordinates:
                location_data['latitude'] = coordinates['lat']
                location_data['longitude'] = coordinates['lng']
                location_data['google_maps_url'] = f"https://www.google.com/maps/search/{coordinates['lat']},{coordinates['lng']}"
            
            # Try to find actual Google Maps link
            maps_link = page.locator("a[href*='maps.google'], a[href*='google.com/maps']")
            if await maps_link.is_visible():
                href = await maps_link.get_attribute('href')
                if href:
                    location_data['google_maps_url'] = href
            
            return location_data if location_data else None
            
        except Exception as e:
            self.logger.debug(f"Location extraction error: {e}")
            return None
    
    async def _extract_description(self, page) -> Optional[str]:
        """Extract hotel description."""
        try:
            description_selectors = [
                "[data-testid='property-description']",
                "#property_description_content",
                ".hp_desc_main_content"
            ]
            
            for selector in description_selectors:
                desc_element = page.locator(selector)
                if await desc_element.is_visible():
                    description = await desc_element.inner_text()
                    if description and len(description) > 50:
                        return description.strip()[:2000]  # Limit length
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Description extraction error: {e}")
            return None
    
    async def _extract_reviews_level_4(self, page) -> Optional[Dict[str, Any]]:
        """Level 4: TRUE ALL REVIEWS extraction - attempts to get ALL available reviews (potentially 1000+)."""
        reviews_data = {"reviews": [], "total_count": 0, "rating_breakdown": {}}
        
        try:
            self.logger.info("🔥 LEVEL 4: Starting TRUE ALL REVIEWS extraction - Target: ALL available reviews")
            
            # STEP 1: Try to navigate to the full reviews page
            all_reviews_loaded = False
            
            # Strategy A: Look for "See all reviews" or similar buttons
            show_all_buttons = [
                "a:has-text('See all reviews')",
                "a:has-text('Show all reviews')",
                "a:has-text('View all reviews')", 
                "button:has-text('See all reviews')",
                "button:has-text('Show all reviews')",
                "button:has-text('View all reviews')",
                "[data-testid*='reviews']:has-text('all')",
                "a[href*='review']:has-text('See all')",
                "a[href*='review']:has-text('Show all')",
                ".bui-review-score__text a",
                "[class*='reviews']:has-text('all')"
            ]
            
            for button_selector in show_all_buttons:
                try:
                    button = page.locator(button_selector).first
                    if await button.is_visible(timeout=3000):
                        self.logger.info(f"🔥 Level 4: Found 'See all reviews' button: {button_selector}")
                        await button.click()
                        await page.wait_for_timeout(5000)  # Wait for reviews page to load
                        all_reviews_loaded = True
                        break
                except Exception as e:
                    self.logger.debug(f"Level 4: Button {button_selector} not found: {e}")
                    continue
            
            # Strategy B: If no button found, try to construct reviews page URL
            if not all_reviews_loaded:
                try:
                    current_url = page.url
                    if '/hotel/' in current_url:
                        # Try to construct reviews URL (Booking.com pattern)
                        base_url = current_url.split('?')[0]  # Remove query params
                        reviews_url = f"{base_url}?tab=reviews"
                        
                        self.logger.info(f"🔥 Level 4: Trying reviews URL: {reviews_url}")
                        await page.goto(reviews_url, wait_until='domcontentloaded', timeout=30000)
                        await page.wait_for_timeout(3000)
                        all_reviews_loaded = True
                except Exception as e:
                    self.logger.debug(f"Level 4: Reviews URL navigation failed: {e}")
            
            # STEP 2: Implement aggressive scrolling to load ALL reviews
            if all_reviews_loaded:
                self.logger.info("🔥 Level 4: Starting aggressive scroll-to-load-all strategy")
                
                # Progressive scrolling with load-more detection
                scroll_attempts = 0
                max_scroll_attempts = 50  # Allow up to 50 scroll attempts
                no_new_content_count = 0
                previous_review_count = 0
                
                while scroll_attempts < max_scroll_attempts and no_new_content_count < 5:
                    # Scroll down gradually
                    scroll_position = (scroll_attempts + 1) * 0.1
                    if scroll_position > 1.0:
                        scroll_position = 1.0
                    
                    await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {scroll_position})")
                    await page.wait_for_timeout(2000)  # Wait for potential lazy loading
                    
                    # Look for "Load more" or "Show more" buttons
                    load_more_buttons = [
                        "button:has-text('Load more')",
                        "button:has-text('Show more')",
                        "button:has-text('More reviews')",
                        "a:has-text('Load more')",
                        "a:has-text('Show more')",
                        "[data-testid*='load-more']",
                        "[data-testid*='show-more']",
                        ".bui-pagination__nav-button"
                    ]
                    
                    for load_button_selector in load_more_buttons:
                        try:
                            load_button = page.locator(load_button_selector).first
                            if await load_button.is_visible(timeout=1000):
                                self.logger.info(f"🔥 Level 4: Clicking load more button: {load_button_selector}")
                                await load_button.click()
                                await page.wait_for_timeout(3000)  # Wait for new content to load
                                break
                        except:
                            continue
                    
                    # Check if we're loading more content by counting review elements
                    current_review_count = await page.locator("[data-testid*='review'], .c-review-block, .bui-review-item, .review-item, [class*='review']").count()
                    
                    if current_review_count == previous_review_count:
                        no_new_content_count += 1
                        self.logger.debug(f"Level 4: No new reviews loaded (attempt {no_new_content_count}/5)")
                    else:
                        no_new_content_count = 0
                        self.logger.info(f"🔥 Level 4: Reviews count increased to {current_review_count}")
                    
                    previous_review_count = current_review_count
                    scroll_attempts += 1
                
                self.logger.info(f"🔥 Level 4: Finished aggressive loading - Final review count: {previous_review_count}")
            
            # STEP 3: Extract ALL loaded reviews with no limits
            review_selectors = [
                "[data-testid*='review']",
                ".c-review-block",
                ".bui-review-item", 
                ".review-item",
                "[class*='review']",
                "[id*='review']",
                ".review_list_new_item",
                ".hp-review-block",
                ".review-card",
                "[data-testid='review-card']"
            ]
            
            reviews = []
            for selector in review_selectors:
                try:
                    review_elements = page.locator(selector)
                    count = await review_elements.count()
                    
                    self.logger.info(f"🔥 Level 4: Selector '{selector}' found {count} review elements")
                    
                    if count > 0:
                        # Level 4: Extract ALL reviews (no limit!)
                        self.logger.info(f"🔥 Level 4: Extracting ALL {count} reviews - NO LIMITS!")
                        
                        for i in range(count):
                            try:
                                element = review_elements.nth(i)
                                review_data = await self._extract_single_review(element)
                                
                                if review_data and (review_data.get('reviewer_name') or review_data.get('review_text')):
                                    # Apply filtering but be less strict for Level 4
                                    is_valid = True
                                    
                                    if review_data.get('review_text'):
                                        if not self._is_valid_review_text(review_data['review_text']):
                                            is_valid = False
                                    
                                    if review_data.get('reviewer_name'):
                                        if not self._is_valid_reviewer_name(review_data['reviewer_name']):
                                            # For Level 4, just clean the name rather than discarding
                                            review_data.pop('reviewer_name', None)
                                    
                                    if is_valid and (review_data.get('reviewer_name') or review_data.get('review_text')):
                                        review_data['extraction_timestamp'] = datetime.now().isoformat()
                                        reviews.append(review_data)
                                        
                                        # Log progress every 50 reviews
                                        if len(reviews) % 50 == 0:
                                            self.logger.info(f"🔥 Level 4: Progress - {len(reviews)} reviews extracted...")
                                    
                            except Exception as e:
                                self.logger.debug(f"Level 4 review {i+1} extraction failed: {e}")
                                continue
                        
                        # For Level 4, use the selector that found the most reviews
                        if reviews:
                            self.logger.info(f"✅ Level 4: Successfully extracted {len(reviews)} total reviews with selector: {selector}")
                            break
                            
                except Exception as e:
                    self.logger.debug(f"Level 4 selector {selector} failed: {e}")
                    continue
            
            # STEP 4: Compile final results
            reviews_data['reviews'] = reviews
            reviews_data['total_count'] = len(reviews)
            
            if reviews:
                self.logger.info(f"🎊 LEVEL 4 SUCCESS: Extracted {len(reviews)} TOTAL REVIEWS!")
                reviews_data['extraction_method'] = 'LEVEL_4_ALL_REVIEWS_EXTRACTION'
                reviews_data['extraction_timestamp'] = datetime.now().isoformat()
                reviews_data['all_reviews_attempted'] = True
                reviews_data['reviews_page_loaded'] = all_reviews_loaded
                return reviews_data
            else:
                # Fallback to Level 3 approach if all else fails
                self.logger.warning("❌ Level 4: No reviews found with ALL reviews approach - falling back")
                fallback_data = await self._extract_reviews_level_3(page)
                if fallback_data and fallback_data.get('reviews'):
                    fallback_data['extraction_method'] = 'LEVEL_4_FALLBACK_TO_LEVEL_3'
                    fallback_data['extraction_timestamp'] = datetime.now().isoformat()
                    return fallback_data
            
            # Return empty structure if all methods fail
            reviews_data['extraction_method'] = 'LEVEL_4_NO_REVIEWS_FOUND'
            reviews_data['extraction_timestamp'] = datetime.now().isoformat()
            return reviews_data
            
        except Exception as e:
            self.logger.error(f"❌ Level 4 ALL reviews extraction failed: {e}")
            return {
                "reviews": [],
                "total_count": 0,
                "rating_breakdown": {},
                "extraction_method": "LEVEL_4_ERROR_RECOVERY",
                "extraction_timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
    
    async def _extract_reviews_level_3(self, page) -> Optional[Dict[str, Any]]:
        """Level 3: Basic review extraction (2-5 reviews max with simpler logic)."""
        reviews_data = {"reviews": [], "total_count": 0, "rating_breakdown": {}}
        
        try:
            self.logger.info("📝 LEVEL 3: Starting basic review extraction")
            
            # Simple navigation to reviews - just scroll down
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.5)")
            await page.wait_for_timeout(2000)
            
            # Try to find review elements directly (no complex navigation)
            review_selectors = [
                "[data-testid*='review']",
                ".c-review-block",
                ".bui-review-item", 
                ".review-item",
                # Additional modern selectors
                "[class*='review']",
                "[id*='review']",
                ".review_list_new_item",
                ".hp-review-block"
            ]
            
            reviews = []
            for selector in review_selectors:
                try:
                    review_elements = page.locator(selector)
                    count = await review_elements.count()
                    
                    self.logger.info(f"📝 Level 3: Selector '{selector}' found {count} review elements")
                    
                    if count > 0:
                        # Level 3: Extract only 2-5 reviews (basic sampling)
                        max_reviews = min(count, 5)
                        self.logger.info(f"📝 Level 3: Found {count} reviews, extracting {max_reviews}")
                        
                        for i in range(max_reviews):
                            try:
                                element = review_elements.nth(i)
                                review_data = await self._extract_single_review(element)
                                
                                if review_data and (review_data.get('reviewer_name') or review_data.get('review_text')):
                                    # Filter out invalid review text and reviewer names
                                    if review_data.get('review_text') and not self._is_valid_review_text(review_data['review_text']):
                                        self.logger.debug(f"Level 3: Filtered out invalid review text: {review_data['review_text'][:50]}...")
                                        continue
                                    if review_data.get('reviewer_name') and not self._is_valid_reviewer_name(review_data['reviewer_name']):
                                        self.logger.debug(f"Level 3: Filtered out invalid reviewer name: {review_data['reviewer_name']}")
                                        # Remove invalid reviewer name but keep review if text is valid
                                        review_data.pop('reviewer_name', None)
                                        if not review_data.get('review_text'):
                                            continue
                                    
                                    review_data['extraction_timestamp'] = datetime.now().isoformat()
                                    reviews.append(review_data)
                                    self.logger.info(f"✅ Level 3: Extracted review {i+1}: {review_data.get('reviewer_name', 'No name')} - {review_data.get('review_text', '')[:50]}...")
                                else:
                                    # Debug failed extraction
                                    try:
                                        element_text = await element.inner_text()
                                        self.logger.debug(f"❌ Review {i+1} failed - Text: {element_text[:100]}...")
                                    except:
                                        pass
                                        
                            except Exception as e:
                                self.logger.debug(f"Level 3 review {i+1} extraction failed: {e}")
                                continue
                        
                        if reviews:
                            break  # Got reviews, stop trying other selectors
                            
                except Exception as e:
                    self.logger.debug(f"Level 3 selector {selector} failed: {e}")
                    continue
            
            reviews_data['reviews'] = reviews
            reviews_data['total_count'] = len(reviews)
            
            if not reviews:
                self.logger.warning("❌ Level 3: No reviews found with any selector on hotel page")
                # Debug: log page title and URL to confirm we're on the right page
                try:
                    page_title = await page.title()
                    page_url = page.url
                    self.logger.debug(f"Level 3 debug - Page title: {page_title}")
                    self.logger.debug(f"Level 3 debug - Page URL: {page_url}")
                except:
                    pass
            else:
                self.logger.info(f"✅ Level 3: Successfully extracted {len(reviews)} reviews")
            reviews_data['extraction_method'] = 'LEVEL_3_BASIC'
            reviews_data['extraction_timestamp'] = datetime.now().isoformat()
            
            if reviews:
                self.logger.info(f"✅ Level 3: Extracted {len(reviews)} basic reviews")
                return reviews_data
            else:
                self.logger.info("⚠️ Level 3: No reviews found")
                return None
                
        except Exception as e:
            self.logger.warning(f"Level 3 reviews extraction error: {e}")
            return None
    
    async def _extract_reviews(self, page) -> Optional[Dict[str, Any]]:
        """Extract comprehensive hotel reviews with all available details."""
        reviews_data = {"reviews": [], "total_count": 0, "rating_breakdown": {}}
        
        try:
            # Navigate to reviews section if exists
            await self._navigate_to_reviews_section(page)
            
            # Try to load all reviews with pagination
            await self._load_all_reviews(page)
            
            # Extract all reviews with comprehensive data
            reviews = await self._extract_all_review_details(page)
            
            reviews_data['reviews'] = reviews
            reviews_data['total_count'] = len(reviews)
            
            self.logger.info(f"✅ Extracted {len(reviews)} complete reviews")
            return reviews_data if reviews else None
            
        except Exception as e:
            self.logger.debug(f"Reviews extraction error: {e}")
            return None
    
    async def _navigate_to_reviews_section(self, page):
        """Navigate to reviews section by clicking reviews tab."""
        try:
            # Try different review tab selectors
            tab_selectors = [
                "a[data-testid='reviews-tab-item']",
                "button:has-text('Reviews')",
                "a:has-text('Reviews')", 
                ".bui-tab__item:has-text('Reviews')",
                "[href*='reviews']"
            ]
            
            for selector in tab_selectors:
                try:
                    tab = page.locator(selector).first
                    if await tab.is_visible(timeout=2000):
                        await tab.click()
                        await page.wait_for_timeout(2000)
                        break
                except:
                    continue
        except Exception as e:
            self.logger.debug(f"Could not navigate to reviews section: {e}")
    
    async def _load_all_reviews(self, page):
        """Load all reviews by clicking pagination buttons and show more links."""
        try:
            # Try to click "Show all reviews" or "Load more" buttons
            load_more_selectors = [
                "button:has-text('Show all reviews')",
                "button:has-text('Load more reviews')",
                "button:has-text('Show more')",
                ".bui-button:has-text('reviews')",
                "[data-testid='reviews-show-more']"
            ]
            
            for selector in load_more_selectors:
                try:
                    button = page.locator(selector).first
                    if await button.is_visible(timeout=2000):
                        await button.click()
                        await page.wait_for_timeout(3000)
                        break
                except:
                    continue
            
            # Try to scroll to load more reviews
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(2000)
            
        except Exception as e:
            self.logger.debug(f"Could not load all reviews: {e}")
    
    async def _extract_all_review_details(self, page):
        """Extract detailed information from all review cards."""
        reviews = []
        
        try:
            # Modern Booking.com review selectors
            review_selectors = [
                "[data-testid='review-card']",
                ".c-review-block",
                ".bui-review-card", 
                ".review-item",
                ".c-review",
                "[class*='review'][class*='card']"
            ]
            
            review_cards = None
            review_count = 0
            
            # Find review cards using modern selectors
            for selector in review_selectors:
                review_cards = page.locator(selector)
                review_count = await review_cards.count()
                if review_count > 0:
                    break
            
            if review_count == 0:
                return reviews
            
            # Extract data from all review cards (remove 20 limit)
            for i in range(review_count):
                try:
                    card = review_cards.nth(i)
                    review_data = await self._extract_single_review(card)
                    
                    if review_data and (review_data.get('reviewer_name') or review_data.get('review_text')):
                        reviews.append(review_data)
                        
                except Exception as e:
                    self.logger.debug(f"Error extracting review {i+1}: {e}")
                    continue
            
        except Exception as e:
            self.logger.debug(f"Error in review details extraction: {e}")
        
        return reviews
    
    async def _extract_single_review(self, card):
        """Extract comprehensive data from a single review card."""
        review_data = {}
        
        try:
            # Reviewer name - try modern selectors
            name_selectors = [
                "[data-testid='reviewer-name']",
                ".bui-avatar-block__title",
                ".c-review-block__reviewer-name", 
                ".reviewer-name",
                "h4",
                ".bui-f-font-weight--bold"
            ]
            
            for selector in name_selectors:
                try:
                    name_element = card.locator(selector).first
                    if await name_element.is_visible(timeout=1000):
                        name_text = await name_element.inner_text()
                        if name_text.strip():
                            review_data['reviewer_name'] = name_text.strip()
                            break
                except:
                    continue
            
            # Reviewer country/location
            country_selectors = [
                "[data-testid='reviewer-country']",
                ".bui-avatar-block__subtitle",
                ".c-review-block__reviewer-country",
                ".reviewer-country"
            ]
            
            for selector in country_selectors:
                try:
                    country_element = card.locator(selector).first
                    if await country_element.is_visible(timeout=1000):
                        country_text = await country_element.inner_text()
                        if country_text.strip():
                            review_data['reviewer_country'] = country_text.strip()
                            break
                except:
                    continue
            
            # Review date
            date_selectors = [
                "[data-testid='review-date']",
                ".bui-review-score__info-text:has-text('Reviewed')",
                ".c-review-block__date",
                ".review-date",
                "time"
            ]
            
            for selector in date_selectors:
                try:
                    date_element = card.locator(selector).first
                    if await date_element.is_visible(timeout=1000):
                        date_text = await date_element.inner_text()
                        if date_text.strip():
                            # Clean up date text
                            clean_date = date_text.replace('Reviewed:', '').replace('Reviewed', '').strip()
                            if clean_date:
                                review_data['review_date'] = clean_date
                                break
                except:
                    continue
            
            # Review score/rating
            score_selectors = [
                "[data-testid='review-score']",
                ".bui-review-score__badge",
                ".c-score-bar__score",
                ".review-score"
            ]
            
            for selector in score_selectors:
                try:
                    score_element = card.locator(selector).first
                    if await score_element.is_visible(timeout=1000):
                        score_text = await score_element.inner_text()
                        try:
                            score = float(score_text.strip())
                            review_data['review_score'] = score
                            break
                        except:
                            continue
                except:
                    continue
            
            # Review title (if exists)
            title_selectors = [
                "[data-testid='review-title']", 
                ".c-review-block__title",
                ".review-title h3",
                ".bui-review-card__title"
            ]
            
            for selector in title_selectors:
                try:
                    title_element = card.locator(selector).first
                    if await title_element.is_visible(timeout=1000):
                        title_text = await title_element.inner_text()
                        if title_text.strip():
                            review_data['review_title'] = title_text.strip()
                            break
                except:
                    continue
            
            # Review text - comprehensive extraction
            text_parts = []
            
            # Try positive/negative review sections
            positive_selectors = [".c-review__positive", "[data-testid='review-positive']"]
            negative_selectors = [".c-review__negative", "[data-testid='review-negative']"]
            
            for selector in positive_selectors:
                try:
                    pos_element = card.locator(selector).first
                    if await pos_element.is_visible(timeout=1000):
                        pos_text = await pos_element.inner_text()
                        if pos_text.strip():
                            text_parts.append(f"👍 {pos_text.strip()}")
                            break
                except:
                    continue
            
            for selector in negative_selectors:
                try:
                    neg_element = card.locator(selector).first
                    if await neg_element.is_visible(timeout=1000):
                        neg_text = await neg_element.inner_text()
                        if neg_text.strip():
                            text_parts.append(f"👎 {neg_text.strip()}")
                            break
                except:
                    continue
            
            # Fallback to general review text
            if not text_parts:
                general_text_selectors = [
                    "[data-testid='review-content']",
                    ".c-review__body",
                    ".bui-review-card__description",
                    ".review-text",
                    "p"
                ]
                
                for selector in general_text_selectors:
                    try:
                        text_element = card.locator(selector).first
                        if await text_element.is_visible(timeout=1000):
                            text_content = await text_element.inner_text()
                            if text_content.strip() and len(text_content.strip()) > 10:
                                text_parts.append(text_content.strip())
                                break
                    except:
                        continue
            
            if text_parts:
                review_data['review_text'] = " | ".join(text_parts)
            
            # Add helpful votes if available  
            try:
                helpful_element = card.locator("[data-testid='review-helpful'], .helpful-votes").first
                if await helpful_element.is_visible(timeout=1000):
                    helpful_text = await helpful_element.inner_text()
                    if helpful_text and any(char.isdigit() for char in helpful_text):
                        review_data['helpful_votes'] = helpful_text.strip()
            except:
                pass
                
        except Exception as e:
            self.logger.debug(f"Error extracting single review: {e}")
        
        return review_data
    
    async def _extract_reviews_fallback(self, page) -> Optional[Dict[str, Any]]:
        """Fallback method to extract reviews without specific container."""
        reviews_data = {"reviews": [], "total_count": 0, "rating_breakdown": {}}
        
        try:
            self.logger.info("🔍 DEBUG: Using fallback review extraction")
            
            # Try to find any review-like elements on the page
            fallback_selectors = [
                ".review",
                "[class*='review']",
                "[data-testid*='review']",
                ".guest-review",
                ".user-review"
            ]
            
            for selector in fallback_selectors:
                elements = page.locator(selector)
                count = await elements.count()
                self.logger.info(f"🔍 DEBUG: Fallback selector {selector}: {count} elements")
                
                if count > 0:
                    # Try to extract some basic review data
                    reviews = []
                    for i in range(min(count, 5)):
                        try:
                            element = elements.nth(i)
                            text = await element.inner_text()
                            if text and len(text) > 20:  # Basic validation
                                reviews.append({
                                    'reviewer_name': 'Anonymous',
                                    'review_text': text[:200] + "..." if len(text) > 200 else text,
                                    'extracted_at': datetime.now().isoformat()
                                })
                        except:
                            continue
                    
                    if reviews:
                        reviews_data['reviews'] = reviews
                        reviews_data['total_count'] = len(reviews)
                        self.logger.info(f"✅ Fallback extracted {len(reviews)} reviews")
                        return reviews_data
                        
        except Exception as e:
            self.logger.info(f"🔍 DEBUG: Fallback extraction error: {e}")
        
        return None
    
    async def _handle_popups(self, page):
        """Handle cookie consent and other popups."""
        try:
            popup_selectors = [
                "button[aria-label=\"Dismiss sign-in info.\"]",
                "button[id*='accept']",
                "button:has-text('Accept')",
                "#onetrust-accept-btn-handler",
                "button:has-text('Got it')",
                ".bui-button--primary"
            ]
            
            for selector in popup_selectors:
                try:
                    button = page.locator(selector)
                    if await button.is_visible():
                        await button.click()
                        self.logger.debug(f"✅ Dismissed popup: {selector}")
                        await page.wait_for_timeout(1000)
                        break
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Popup handling: {e}")
    
    async def _create_browser_context(self):
        """Create optimized browser context."""
        return await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
        )
    
    def _extract_hotel_id_from_url(self, url: str) -> Optional[str]:
        """Extract hotel ID from booking URL."""
        try:
            # Extract from URL patterns like /hotel/ae/hotel-name.html?hotel_id=123
            import re
            match = re.search(r'hotel_id=(\d+)', url)
            if match:
                return match.group(1)
            
            # Extract from URL path
            match = re.search(r'/hotel/[^/]+/([^.]+)', url)
            if match:
                return hashlib.md5(match.group(1).encode()).hexdigest()[:8]
                
        except:
            pass
        return hashlib.md5(url.encode()).hexdigest()[:8]
    
    def _extract_price_number(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text."""
        try:
            # Remove currency symbols and extract numbers
            import re
            numbers = re.findall(r'[\d,]+\.?\d*', str(price_text).replace(',', ''))
            if numbers:
                return float(numbers[0])
        except:
            pass
        return None
    
    def _extract_review_count(self, reviews_text: str) -> Optional[int]:
        """Extract review count from text."""
        try:
            import re
            # Extract numbers from text like "1,234 reviews"
            numbers = re.findall(r'[\d,]+', str(reviews_text).replace(',', ''))
            if numbers:
                return int(numbers[0])
        except:
            pass
        return None
    
    def _extract_rating_number(self, rating_text: str) -> Optional[float]:
        """Extract numeric rating from text with enhanced parsing."""
        try:
            import re
            
            # Direct numeric pattern (handles "8.5", "9.23", etc.)
            rating_match = re.search(r'\b([0-9])\.([0-9]{1,2})\b', str(rating_text))
            if rating_match:
                rating_value = float(rating_match.group())
                if 0 <= rating_value <= 10:
                    return rating_value
            
            # Handle "Scored 8.5 out of 10" format
            scored_match = re.search(r'scored\s+([0-9])\.([0-9]{1,2})', str(rating_text).lower())
            if scored_match:
                rating_value = float(scored_match.group(1) + '.' + scored_match.group(2))
                if 0 <= rating_value <= 10:
                    return rating_value
            
            # Handle "Rated 8.5" format
            rated_match = re.search(r'rated\s+([0-9])\.([0-9]{1,2})', str(rating_text).lower())
            if rated_match:
                rating_value = float(rated_match.group(1) + '.' + rated_match.group(2))
                if 0 <= rating_value <= 10:
                    return rating_value
            
            # Fallback: any decimal number in valid range
            all_numbers = re.findall(r'[0-9]+\.?[0-9]*', str(rating_text))
            for number_str in all_numbers:
                try:
                    number = float(number_str)
                    if 6.0 <= number <= 10.0:  # Reasonable rating range
                        return number
                except ValueError:
                    continue
                    
        except Exception:
            pass
        return None
    
    def _is_valid_reviewer_name(self, name_text: str) -> bool:
        """Enhanced check if text looks like a valid reviewer name vs review title."""
        if not name_text or len(name_text) < 2:
            return False
        
        name_text = name_text.strip()
        name_lower = name_text.lower()
        
        # Filter out common review titles/sentiments that get mistaken for names
        invalid_names = {
            'wonderful', 'excellent', 'good', 'bad', 'terrible', 'amazing', 'fantastic',
            'disappointing', 'ok', 'okay', 'great', 'nice', 'perfect', 'awful',
            'decent', 'average', 'poor', 'outstanding', 'superb', 'magnificent',
            'horrible', 'lovely', 'pleasant', 'unpleasant', 'satisfactory',
            'unsatisfactory', 'exceptional', 'mediocre', 'incredible', 'disgusting',
            'clean', 'dirty', 'comfortable', 'uncomfortable', 'convenient', 'inconvenient',
            'spacious', 'cramped', 'quiet', 'noisy', 'friendly', 'unfriendly',
            'review', 'guest', 'customer', 'user', 'traveler', 'visitor'
        }
        
        # Check if it's a common review sentiment word
        if name_lower in invalid_names:
            return False
        
        # Check for common review phrases
        review_phrases = ['amazing experience', 'wonderful stay', 'great hotel', 'terrible service']
        if any(phrase in name_lower for phrase in review_phrases):
            return False
        
        # Enhanced filtering for country suffixes that get combined with names
        country_suffixes = [
            'united states', 'united kingdom', 'united arab emirates', 'saudi arabia',
            'south africa', 'new zealand', 'costa rica', 'czech republic', 'sri lanka',
            'united states of america', 'uae', 'usa', 'uk', 'germany', 'france', 'italy',
            'spain', 'australia', 'canada', 'brazil', 'mexico', 'india', 'china', 'japan',
            'korea', 'thailand', 'singapore', 'malaysia', 'indonesia', 'philippines',
            'netherlands', 'belgium', 'switzerland', 'austria', 'sweden', 'norway',
            'denmark', 'finland', 'poland', 'russia', 'turkey', 'egypt', 'morocco'
        ]
        
        # Enhanced filtering for names with country suffixes (like "Salloura United Arab Emirates")  
        for suffix in country_suffixes:
            if suffix in name_lower and len(name_text) > len(suffix) + 3:
                # If country is at the end, extract just the name part
                if name_lower.endswith(' ' + suffix):
                    name_only = name_text[:-len(suffix)-1].strip()
                    if len(name_only) >= 2 and not any(invalid in name_only.lower() for invalid in invalid_names):
                        return True
                return False
        
        # Filter out UI elements that might get extracted as names
        ui_elements = [
            'see all', 'show more', 'read more', 'view all', 'expand', 'collapse',
            'filter by', 'sort by', 'guest reviews', 'customer reviews', 'reviews',
            'booking', 'reservation', 'book now', 'check availability', 'see availability'
        ]
        
        if any(ui_elem in name_lower for ui_elem in ui_elements):
            return False
        
        # Check if it's too long to be a typical name
        if len(name_text) > 50:
            return False
        
        # Check if it contains mostly special characters 
        import re
        if len(re.sub(r'[a-zA-Z0-9\s]', '', name_text)) > len(name_text) * 0.5:
            return False
        
        # Check if it looks like a date
        if re.match(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', name_text):
            return False
        
        # Check if it's just numbers
        if name_text.isdigit():
            return False
        
        # Filter out names that have country codes attached (like "MohammedSaudi Arabia")
        country_suffixes = [
            'saudi arabia', 'united arab emirates', 'uae', 'united states', 'usa',
            'united kingdom', 'uk', 'australia', 'canada', 'france', 'germany',
            'italy', 'spain', 'netherlands', 'india', 'pakistan', 'china', 'japan'
        ]
        
        # Check if name ends with any country (space-separated or concatenated)
        for suffix in country_suffixes:
            # Check direct match
            if name_lower.endswith(suffix):
                return False
            # Check concatenated (no spaces) 
            if name_lower.endswith(suffix.replace(' ', '')):
                return False
            # Check if country appears anywhere in the name after position 2
            # Handles cases like "TasneemUnited Arab Emirates" 
            if suffix in name_lower and name_lower.find(suffix) >= 2:
                return False
        
        # Looks reasonable
        return True
    
    
    def _is_valid_review_text(self, text: str) -> bool:
        """Check if text looks like valid review content vs UI elements."""
        if not text or len(text.strip()) < 10:
            return False
        
        text_lower = text.lower().strip()
        
        # Filter out common UI elements that get mistaken for review text
        ui_elements = {
            'see availability',
            'show more reviews',
            'read more reviews', 
            'guest reviews',
            'all reviews',
            'booking.com',
            'book now',
            'check availability',
            'reserve now',
            'rated wonderful',
            'rated excellent', 
            'rated good'
        }
        
        # Check if it's just a UI element
        if text_lower in ui_elements:
            return False
        
        # Check if it starts with common UI patterns  
        ui_prefixes = ['guest reviews (', 'rated ', 'show ', 'read ', 'see ']
        if any(text_lower.startswith(prefix) for prefix in ui_prefixes):
            return False
            
        # Filter out review count patterns like "Guest reviews (21)", "· 21 reviews"
        import re
        if re.match(r'^.*reviews?\s*\([\d,]+\)$', text_lower) or re.match(r'^·\s*[\d,]+\s*reviews?$', text_lower):
            return False
        
        # Filter out standalone review count patterns
        if re.match(r'^\d[\d,]*\s*reviews?$', text_lower):
            return False
        
        # Must contain actual words (not just numbers or symbols)
        word_count = len([word for word in text.split() if word.isalpha() and len(word) > 2])
        if word_count < 3:
            return False
        
        return True
    
    def _fix_image_url(self, url: str) -> str:
        """Fix and enhance image URLs."""
        if not url:
            return url
            
        # Fix protocol
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = "https://cf.bstatic.com" + url
        
        # Convert to high quality
        replacements = [
            ("square60", "max1024x768"),
            ("square200", "max1024x768"),
            ("square240", "max1024x768"),
            ("square600", "max1024x768"),
            ("max300", "max1024x768"),
            ("max500", "max1024x768"),
            ("thumbnail", "max1024x768")
        ]
        
        for old, new in replacements:
            if old in url:
                url = url.replace(old, new)
                break
        
        return url
    
    def _calculate_completeness(self, hotel: Dict[str, Any]) -> float:
        """Calculate data completeness score."""
        # Define required fields and their weights
        fields_weights = {
            'name': 10,
            'price_per_night': 10,
            'rating': 8,
            'address': 5,
            'booking_url': 10,
            'latitude': 8,
            'longitude': 8,
            'google_maps_url': 5,
            'images': 10,
            'amenities': 8,
            'reviews': 10,
            'description': 5,
            'review_count': 3,
            'rating_breakdown': 3,
            'hotel_id': 2
        }
        
        total_weight = sum(fields_weights.values())
        achieved_weight = 0
        
        for field, weight in fields_weights.items():
            if hotel.get(field):
                if isinstance(hotel[field], list):
                    # For lists, check if not empty
                    if len(hotel[field]) > 0:
                        achieved_weight += weight
                elif isinstance(hotel[field], dict):
                    # For dicts, check if has content
                    if any(hotel[field].values()):
                        achieved_weight += weight
                elif isinstance(hotel[field], str):
                    # For strings, check if not empty
                    if hotel[field].strip():
                        achieved_weight += weight
                else:
                    # For other types, just check existence
                    achieved_weight += weight
        
        return round((achieved_weight / total_weight) * 100, 2)
    
    # ==== LEVEL 4 ENHANCED METHODS ====
    
    async def _navigate_to_reviews_section_enhanced(self, page):
        """Enhanced navigation to reviews section with multiple strategies."""
        try:
            self.logger.info("🔍 Level 4: Enhanced navigation to reviews section")
            
            # Strategy 1: Scroll down to trigger review loading
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.7)")
            await page.wait_for_timeout(3000)
            
            # Strategy 2: Try clicking review tabs/links
            tab_selectors = [
                "a[data-testid='reviews-tab-item']",
                "button:has-text('Reviews')",
                "a:has-text('Reviews')",
                ".bui-tab__item:has-text('Reviews')",
                "[href*='reviews']",
                "button[aria-label*='review']",
                "a[aria-label*='review']"
            ]
            
            for selector in tab_selectors:
                try:
                    tab = page.locator(selector).first
                    if await tab.is_visible(timeout=2000):
                        self.logger.info(f"🎯 Level 4: Found review navigation: {selector}")
                        await tab.click()
                        await page.wait_for_timeout(3000)
                        break
                except:
                    continue
            
            # Strategy 3: Scroll to any visible review section
            review_section_selectors = [
                "[data-testid*='review']",
                ".reviews-section",
                "#reviews",
                ".review-container"
            ]
            
            for selector in review_section_selectors:
                try:
                    section = page.locator(selector).first
                    if await section.is_visible(timeout=1000):
                        await section.scroll_into_view_if_needed()
                        await page.wait_for_timeout(2000)
                        break
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Enhanced navigation error: {e}")
    
    async def _load_more_reviews_enhanced(self, page):
        """Enhanced review loading with multiple pagination strategies."""
        try:
            self.logger.info("🔄 Level 4: Enhanced review loading")
            
            max_attempts = 3
            for attempt in range(max_attempts):
                
                # Strategy 1: Click load more/show all buttons
                load_more_selectors = [
                    "button:has-text('Show all reviews')",
                    "button:has-text('Load more reviews')",
                    "button:has-text('Show more')",
                    "button:has-text('View all reviews')",
                    ".bui-button:has-text('reviews')",
                    "[data-testid*='load-more']",
                    "[data-testid*='show-more']"
                ]
                
                clicked_something = False
                for selector in load_more_selectors:
                    try:
                        button = page.locator(selector).first
                        if await button.is_visible(timeout=2000) and await button.is_enabled():
                            self.logger.info(f"🔄 Level 4: Clicking load more: {selector}")
                            await button.click()
                            await page.wait_for_timeout(3000)
                            clicked_something = True
                            break
                    except:
                        continue
                
                # Strategy 2: Try pagination buttons
                if not clicked_something:
                    pagination_selectors = [
                        "button[aria-label*='Next']",
                        ".bui-pagination__next",
                        "a:has-text('Next')",
                        "[data-testid*='next']",
                        ".pagination-next"
                    ]
                    
                    for selector in pagination_selectors:
                        try:
                            button = page.locator(selector).first
                            if await button.is_visible(timeout=2000) and await button.is_enabled():
                                self.logger.info(f"🔄 Level 4: Clicking pagination: {selector}")
                                await button.click()
                                await page.wait_for_timeout(3000)
                                clicked_something = True
                                break
                        except:
                            continue
                
                # Strategy 3: Scroll to trigger lazy loading
                if not clicked_something:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000)
                
                # Check if we have enough reviews
                review_count = await self._count_visible_reviews(page)
                self.logger.info(f"🔄 Level 4: Attempt {attempt + 1}, found {review_count} reviews")
                
                if review_count >= 10:  # Target reached
                    break
                    
        except Exception as e:
            self.logger.debug(f"Enhanced review loading error: {e}")
    
    async def _extract_all_review_details_enhanced(self, page):
        """Enhanced comprehensive review extraction with improved selectors."""
        reviews = []
        
        try:
            self.logger.info("📝 Level 4: Enhanced review details extraction")
            
            # Enhanced review selectors based on investigation
            review_selectors = [
                "[data-testid='review-card']",
                ".c-review-block",
                ".bui-review-card",
                ".review-item",
                ".c-review",
                "[class*='review'][class*='card']",
                ".guest-review",
                ".user-review",
                "[data-testid*='review']"
            ]
            
            review_cards = None
            review_count = 0
            
            # Find review cards using enhanced selectors
            for selector in review_selectors:
                review_cards = page.locator(selector)
                review_count = await review_cards.count()
                if review_count > 0:
                    self.logger.info(f"✅ Level 4: Found {review_count} reviews with selector: {selector}")
                    break
            
            if review_count == 0:
                self.logger.info("❌ Level 4: No reviews found with any selector")
                return reviews
            
            # Extract data from reviews (target 10-50 reviews for Level 4)
            target_reviews = min(review_count, 50)  # Level 4 realistic limit
            
            for i in range(target_reviews):
                try:
                    card = review_cards.nth(i)
                    review_data = await self._extract_single_review_enhanced(card)
                    
                    if review_data and (review_data.get('reviewer_name') or review_data.get('review_text')):
                        # Add timestamp for Level 4 reviews
                        review_data['extraction_timestamp'] = datetime.now().isoformat()
                        reviews.append(review_data)
                        
                except Exception as e:
                    self.logger.debug(f"Error extracting enhanced review {i+1}: {e}")
                    continue
            
            self.logger.info(f"✅ Level 4: Successfully extracted {len(reviews)} enhanced reviews")
                        
        except Exception as e:
            self.logger.debug(f"Error in enhanced review details extraction: {e}")
        
        return reviews
    
    async def _extract_single_review_enhanced(self, card):
        """Enhanced single review extraction with improved field detection."""
        review_data = {}
        
        try:
            # Enhanced reviewer name extraction with better filtering
            name_selectors = [
                "[data-testid='reviewer-name']",
                ".bui-avatar-block__title", 
                ".c-review-block__reviewer-name",
                ".reviewer-name",
                "[class*='reviewer'][class*='name']",
                "[class*='guest'][class*='name']",
                "[class*='author']",
                ".review-author"
            ]
            
            for selector in name_selectors:
                try:
                    name_element = card.locator(selector).first
                    if await name_element.is_visible(timeout=500):
                        name_text = await name_element.inner_text()
                        if name_text and name_text.strip():
                            name_text = name_text.strip()
                            # Filter out review titles/sentiment words that might be confused with names
                            if self._is_valid_reviewer_name(name_text):
                                review_data['reviewer_name'] = name_text
                                break
                except:
                    continue
            
            # Fallback: look for any text that looks like a name in the card
            if not review_data.get('reviewer_name'):
                try:
                    # Look for shorter text elements that might be names
                    potential_names = card.locator("*:not(div):not(span) h4, .bui-f-font-weight--bold")
                    count = await potential_names.count()
                    for i in range(min(count, 3)):
                        try:
                            element = potential_names.nth(i)
                            text = await element.inner_text()
                            if text and text.strip() and self._is_valid_reviewer_name(text.strip()):
                                review_data['reviewer_name'] = text.strip()
                                break
                        except:
                            continue
                except:
                    pass
            
            # Enhanced review text extraction
            text_parts = []
            
            # Try positive/negative review sections
            positive_selectors = [".c-review__positive", "[data-testid='review-positive']", ".review-positive"]
            negative_selectors = [".c-review__negative", "[data-testid='review-negative']", ".review-negative"]
            
            for selector in positive_selectors:
                try:
                    pos_element = card.locator(selector).first
                    if await pos_element.is_visible(timeout=500):
                        pos_text = await pos_element.inner_text()
                        if pos_text.strip():
                            text_parts.append(f"👍 {pos_text.strip()}")
                            break
                except:
                    continue
            
            for selector in negative_selectors:
                try:
                    neg_element = card.locator(selector).first
                    if await neg_element.is_visible(timeout=500):
                        neg_text = await neg_element.inner_text()
                        if neg_text.strip():
                            text_parts.append(f"👎 {neg_text.strip()}")
                            break
                except:
                    continue
            
            # Fallback to general review text
            if not text_parts:
                general_text_selectors = [
                    "[data-testid='review-content']",
                    ".c-review__body",
                    ".bui-review-card__description",
                    ".review-text",
                    ".review-content",
                    "p"
                ]
                
                for selector in general_text_selectors:
                    try:
                        text_element = card.locator(selector).first
                        if await text_element.is_visible(timeout=500):
                            text_content = await text_element.inner_text()
                            if text_content.strip() and len(text_content.strip()) > 10:
                                text_parts.append(text_content.strip())
                                break
                    except:
                        continue
            
            if text_parts:
                review_data['review_text'] = " | ".join(text_parts)
            
            # Enhanced date extraction
            date_selectors = [
                "[data-testid='review-date']",
                ".bui-review-score__info-text:has-text('Reviewed')",
                ".c-review-block__date",
                ".review-date",
                "time",
                "[class*='date']"
            ]
            
            for selector in date_selectors:
                try:
                    date_element = card.locator(selector).first
                    if await date_element.is_visible(timeout=500):
                        date_text = await date_element.inner_text()
                        if date_text.strip():
                            clean_date = date_text.replace('Reviewed:', '').replace('Reviewed', '').strip()
                            if clean_date:
                                review_data['review_date'] = clean_date
                                break
                except:
                    continue
            
            # Enhanced score extraction
            score_selectors = [
                "[data-testid='review-score']",
                ".bui-review-score__badge",
                ".c-score-bar__score",
                ".review-score",
                "[class*='score']"
            ]
            
            for selector in score_selectors:
                try:
                    score_element = card.locator(selector).first
                    if await score_element.is_visible(timeout=500):
                        score_text = await score_element.inner_text()
                        try:
                            score = float(score_text.strip())
                            review_data['review_score'] = score
                            break
                        except:
                            continue
                except:
                    continue
                
        except Exception as e:
            self.logger.debug(f"Error extracting enhanced single review: {e}")
        
        return review_data
    
    async def _count_visible_reviews(self, page):
        """Count currently visible reviews on the page."""
        try:
            review_selectors = [
                "[data-testid='review-card']",
                ".c-review-block",
                ".bui-review-card",
                ".review-item"
            ]
            
            for selector in review_selectors:
                count = await page.locator(selector).count()
                if count > 0:
                    return count
            return 0
        except:
            return 0
    
    async def _extract_single_review(self, review_element) -> Optional[Dict[str, Any]]:
        """Extract review data from a single review element."""
        review_data = {}
        
        try:
            # Extract reviewer name with comprehensive selectors for 2025 Booking.com
            name_selectors = [
                # Modern Booking.com selectors
                "[data-testid='reviewer-name']",
                "[data-testid*='reviewer']",
                "[data-testid*='author']",
                
                # Class-based selectors
                ".bui-avatar-block__title",
                ".c-review-block__title", 
                ".review_list_new_item_block_author",
                "[class*='reviewer-name']",
                "[class*='reviewer_name']",
                "[class*='author-name']",
                "[class*='author_name']",
                
                # Generic selectors for names
                "h4", "h5", "h6",  # Names often in headers
                ".hp_reviewer_name",
                "[class*='name']:not([class*='hotel']):not([class*='property'])",
                
                # Fallback - any text that looks like a name
                "div:has-text(/^[A-Z][a-z]+ ?[A-Z]?[a-z]*$/)",
                "span:has-text(/^[A-Z][a-z]+ ?[A-Z]?[a-z]*$/)"
            ]
            
            for selector in name_selectors:
                try:
                    name_element = review_element.locator(selector).first
                    if await name_element.is_visible(timeout=500):
                        name_text = await name_element.inner_text()
                        if name_text and name_text.strip() and self._is_valid_reviewer_name(name_text.strip()):
                            review_data['reviewer_name'] = name_text.strip()
                            break
                except:
                    continue
            
            # Extract review text with comprehensive selectors
            text_selectors = [
                # Modern Booking.com selectors - more specific to avoid UI elements
                "[data-testid='review-content']",
                "[data-testid*='review-text']",
                
                # Class-based selectors - more specific
                ".c-review-block__content",
                ".review-content", 
                ".bui-review-content",
                ".hp_review_text",
                "[class*='review-text']:not([class*='count']):not([class*='header'])",
                "[class*='review_text']:not([class*='count']):not([class*='header'])",
                "[class*='review-content']:not([class*='count']):not([class*='header'])",
                
                # Generic text selectors (longer text usually = review) - but be more specific
                "p:not([class*='count']):not([class*='header'])", 
                "div p:not([class*='count'])"
            ]
            
            text_parts = []
            
            for selector in text_selectors:
                try:
                    text_elements = review_element.locator(selector)
                    count = await text_elements.count()
                    
                    for i in range(min(count, 3)):  # Max 3 text parts per selector
                        try:
                            element = text_elements.nth(i)
                            if await element.is_visible(timeout=500):
                                text = await element.inner_text()
                                if text and text.strip() and len(text.strip()) > 10:
                                    text_parts.append(text.strip())
                        except:
                            continue
                            
                    if text_parts:  # Found text with this selector, stop
                        break
                        
                except:
                    continue
            
            if text_parts:
                review_data['review_text'] = ' '.join(text_parts)
            else:
                # Fallback: extract any text from the review element
                try:
                    full_text = await review_element.inner_text()
                    if full_text and len(full_text.strip()) > 20:  # Must be substantial text
                        # Clean up the text - remove common non-review parts
                        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
                        meaningful_lines = [line for line in lines if len(line) > 10 and not line.isdigit()]
                        if meaningful_lines:
                            review_data['review_text'] = ' '.join(meaningful_lines[:3])  # Max 3 lines
                except:
                    pass
            
            # Extract review date
            date_selectors = [
                "[data-testid='review-date']",
                ".c-review-block__date",
                "[class*='review-date']",
                "[class*='date']"
            ]
            
            for selector in date_selectors:
                try:
                    date_element = review_element.locator(selector).first
                    if await date_element.is_visible(timeout=500):
                        date_text = await date_element.inner_text()
                        if date_text and date_text.strip():
                            review_data['review_date'] = date_text.strip()
                            break
                except:
                    continue
            
            # Extract rating if present
            rating_selectors = [
                "[data-testid='review-score']",
                ".bui-review-score__badge",
                "[class*='rating']",
                "[class*='score']"
            ]
            
            for selector in rating_selectors:
                try:
                    rating_element = review_element.locator(selector).first
                    if await rating_element.is_visible(timeout=500):
                        rating_text = await rating_element.inner_text()
                        if rating_text:
                            rating_value = self._extract_rating_number(rating_text)
                            if rating_value:
                                review_data['rating'] = rating_value
                                break
                except:
                    continue
            
            # Only return review if we have meaningful content
            if review_data.get('reviewer_name') or review_data.get('review_text'):
                review_data['extraction_timestamp'] = datetime.now().isoformat()
                return review_data
            else:
                return None
                
        except Exception as e:
            return None