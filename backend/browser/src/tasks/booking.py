"""
Booking.com Hotel Scraper - Production Ready
==========================================

Clean, optimized version with 4-level extraction system:
- Level 1: Quick search (search results only)  
- Level 2: Full data (hotel pages with complete details)
- Level 3: Basic reviews (Level 2 + 2-5 reviews per hotel)
- Level 4: Deep reviews (Level 2 + comprehensive review extraction)

Version: 7.0 (Production Clean)
Author: Refactored for best practices
"""

import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse
import hashlib


class BookingTask:
    """Production-ready Booking.com hotel scraper with 4-level extraction."""
    
    BASE_URL = "https://www.booking.com"

    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point for hotel scraping."""
        try:
            # Validate and normalize parameters
            clean_params = BookingTask._validate_params(params)
            
            # Determine scraping level
            scrape_level = clean_params.get("level") or clean_params.get("scrape_level") or params.get("level", params.get("scrape_level", 2))
            deep_scrape = params.get("deep_scrape", False) or params.get("deep_scrape_enabled", False)
            
            # Legacy deep_scrape mapping
            if deep_scrape and scrape_level == 2:
                scrape_level = 3
                
            logger.info(f"🚀 BOOKING.COM SCRAPER v7.0 - PRODUCTION CLEAN")
            logger.info(f"📍 Location: {clean_params['location']}")
            logger.info(f"📅 Dates: {clean_params['check_in']} to {clean_params['check_out']}")
            logger.info(f"📊 Scrape Level: {scrape_level}")
            
            # Level descriptions
            level_descriptions = {
                1: "Quick Search - Essential data only",
                2: "Full Data - Complete hotel details", 
                3: "Basic Reviews - Level 2 + review sampling",
                4: "Deep Reviews - Level 2 + comprehensive reviews"
            }
            
            logger.info(f"🎯 {level_descriptions.get(scrape_level, 'Unknown level')}")
            
            # Create scraper instance
            scraper = BookingScraper(browser, logger)
            
            # Execute based on level
            if scrape_level >= 4:
                hotels = await scraper.scrape_level_4(clean_params)
                extraction_method = "level_4_deep_reviews"
            elif scrape_level >= 3:
                hotels = await scraper.scrape_level_3(clean_params)
                extraction_method = "level_3_basic_reviews"
            elif scrape_level >= 2:
                hotels = await scraper.scrape_level_2(clean_params)
                extraction_method = "level_2_full_data"
            else:
                hotels = await scraper.scrape_level_1(clean_params)
                extraction_method = "level_1_quick_search"
            
            # Apply filters
            hotels = BookingTask._apply_filters(hotels, params, logger)
            
            # Calculate metrics
            hotels_with_prices = [h for h in hotels if h.get('price_per_night', 0) > 0]
            success_rate = len(hotels_with_prices) / len(hotels) if hotels else 0
            avg_price = sum(h.get('price_per_night', 0) for h in hotels_with_prices) / len(hotels_with_prices) if hotels_with_prices else 0
            
            logger.info(f"🏁 Completed: {len(hotels)} hotels | {success_rate:.1%} with prices")
            
            result = {
                "search_metadata": {
                    "location": clean_params["location"],
                    "check_in": clean_params["check_in"],
                    "check_out": clean_params["check_out"],
                    "nights": clean_params["nights"],
                    "extraction_method": extraction_method,
                    "scrape_level": scrape_level,
                    "total_found": len(hotels),
                    "success_rate": success_rate,
                    "average_price": avg_price,
                    "search_completed_at": datetime.now().isoformat()
                },
                "hotels": hotels
            }
            
            # Save output if requested
            if job_output_dir and hotels:
                BookingTask._save_output(result, job_output_dir, logger)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Critical error: {e}", exc_info=True)
            return {"search_metadata": {"error": str(e)}, "hotels": []}

    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize input parameters."""
        location = params.get("location", "Dubai")
        max_results = min(params.get("max_results", 10), 50)
        
        # Date handling
        check_in = params.get("check_in")
        check_out = params.get("check_out")
        
        if not check_in:
            check_in = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
        if not check_out:
            check_out = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
        
        # Calculate nights
        try:
            check_in_dt = datetime.strptime(check_in, "%Y-%m-%d")
            check_out_dt = datetime.strptime(check_out, "%Y-%m-%d")
            nights = (check_out_dt - check_in_dt).days
        except:
            nights = 3
        
        return {
            "location": location,
            "check_in": check_in,
            "check_out": check_out,
            "nights": nights,
            "max_results": max_results,
            "min_price": params.get("min_price"),
            "max_price": params.get("max_price"),
            "min_rating": params.get("min_rating"),
            "adults": params.get("adults", 2),
            "rooms": params.get("rooms", 1),
            "level": params.get("level") or params.get("scrape_level", 2)
        }

    @staticmethod
    def _apply_filters(hotels: List[Dict[str, Any]], params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Apply post-processing filters to hotels."""
        original_count = len(hotels)
        
        # Price filters
        min_price = params.get("min_price")
        max_price = params.get("max_price")
        min_rating = params.get("min_rating")
        
        if min_price:
            hotels = [h for h in hotels if h.get('price_per_night', 0) >= min_price]
        if max_price:
            hotels = [h for h in hotels if h.get('price_per_night', 0) <= max_price]
        if min_rating:
            hotels = [h for h in hotels if h.get('rating', 0) >= min_rating]
        
        if len(hotels) != original_count:
            logger.info(f"🔍 Applied filters: {original_count} → {len(hotels)} hotels")
        
        return hotels

    @staticmethod
    def _save_output(result: Dict[str, Any], job_output_dir: str, logger: logging.Logger):
        """Save scraping results to file."""
        import os
        try:
            output_file = os.path.join(job_output_dir, "hotels_data.json")
            os.makedirs(job_output_dir, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved data to {output_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save output: {e}")


class BookingScraper:
    """Main scraper class with level-based extraction methods."""
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger

    async def scrape_level_1(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 1: Quick search - essential data only."""
        self.logger.info("⚡ Level 1: Quick search extraction")
        
        context = await self.browser.new_context()
        try:
            page = await context.new_page()
            
            # Perform search
            await self._perform_search(page, params)
            
            # Extract basic hotel data from search results
            hotels = await self._extract_search_results(page, params["max_results"])
            
            self.logger.info(f"✅ Level 1: Extracted {len(hotels)} hotels from search results")
            return hotels
            
        finally:
            await context.close()

    async def scrape_level_2(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 2: Full data - complete hotel details."""
        self.logger.info("🏨 Level 2: Full data extraction")
        
        # Start with Level 1 data
        hotels = await self.scrape_level_1(params)
        
        # Enhance with detailed data from individual hotel pages
        context = await self.browser.new_context()
        try:
            for i, hotel in enumerate(hotels):
                self.logger.info(f"📍 Processing hotel {i+1}/{len(hotels)}: {hotel.get('name', 'Unknown')}")
                
                try:
                    page = await context.new_page()
                    await page.goto(hotel['booking_url'], wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    
                    # Extract detailed data
                    detailed_data = await self._extract_hotel_details(page)
                    hotel.update(detailed_data)
                    hotel['extraction_level'] = 2
                    
                    await page.close()
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to get details for hotel {i+1}: {e}")
                    hotel['extraction_level'] = 1
                    
            self.logger.info(f"✅ Level 2: Enhanced {len(hotels)} hotels with detailed data")
            return hotels
            
        finally:
            await context.close()

    async def scrape_level_3(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 3: Basic reviews - Level 2 + review sampling."""
        self.logger.info("📝 Level 3: Basic reviews extraction")
        
        # Start with Level 2 data
        hotels = await self.scrape_level_2(params)
        
        # Add basic reviews
        context = await self.browser.new_context()
        try:
            for i, hotel in enumerate(hotels):
                self.logger.info(f"📝 Extracting reviews for hotel {i+1}/{len(hotels)}")
                
                try:
                    page = await context.new_page()
                    await page.goto(hotel['booking_url'], wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    
                    # Extract basic reviews (2-5 reviews)
                    reviews_data = await self._extract_basic_reviews(page)
                    if reviews_data:
                        hotel.update(reviews_data)
                    
                    hotel['extraction_level'] = 3
                    
                    await page.close()
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to get reviews for hotel {i+1}: {e}")
                    hotel['extraction_level'] = 2
                    
            self.logger.info(f"✅ Level 3: Added reviews to {len(hotels)} hotels")
            return hotels
            
        finally:
            await context.close()

    async def scrape_level_4(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 4: Deep reviews - comprehensive review extraction."""
        self.logger.info("🔥 Level 4: Deep reviews extraction")
        
        # Start with Level 2 data (skip Level 3 to avoid duplicate processing)
        hotels = await self.scrape_level_2(params)
        
        # Add comprehensive reviews
        context = await self.browser.new_context()
        try:
            for i, hotel in enumerate(hotels):
                self.logger.info(f"🔥 Deep review extraction for hotel {i+1}/{len(hotels)}")
                
                try:
                    page = await context.new_page()
                    await page.goto(hotel['booking_url'], wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    
                    # Extract comprehensive reviews with pagination
                    reviews_data = await self._extract_comprehensive_reviews(page)
                    if reviews_data:
                        hotel.update(reviews_data)
                    
                    hotel['extraction_level'] = 4
                    
                    await page.close()
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed deep review extraction for hotel {i+1}: {e}")
                    hotel['extraction_level'] = 2
                    
            self.logger.info(f"✅ Level 4: Added comprehensive reviews to {len(hotels)} hotels")
            return hotels
            
        finally:
            await context.close()

    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform hotel search on Booking.com."""
        location = params["location"]
        check_in = params["check_in"]
        check_out = params["check_out"]
        adults = params["adults"]
        rooms = params["rooms"]
        
        # Build search URL
        search_url = (
            f"{BookingTask.BASE_URL}/searchresults.html"
            f"?ss={quote(location)}"
            f"&checkin={check_in}"
            f"&checkout={check_out}"
            f"&group_adults={adults}"
            f"&no_rooms={rooms}"
            f"&offset=0"
        )
        
        self.logger.info(f"🔍 Search URL: {search_url}")
        
        await page.goto(search_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)  # Increased timeout for stability
        
        # Handle popups and overlays
        await self._handle_popups(page)
        
        # Apply search filters if specified
        await self._apply_search_filters(page, params)

    async def _extract_search_results(self, page, max_results: int) -> List[Dict[str, Any]]:
        """Extract hotel data from search results page."""
        hotels = []
        
        # Extract page-level prices first (working price extraction method)
        page_prices = await self._extract_page_level_prices(page, max_results)
        
        # Property card selectors
        card_selectors = [
            "[data-testid='property-card']",
            "[data-testid='hotel-card']", 
            ".sr-hotel__wrapper",
            "[class*='property-card']"
        ]
        
        cards = None
        for selector in card_selectors:
            try:
                self.logger.info(f"  🔍 Testing selector: {selector}")
                test_cards = page.locator(selector)
                count = await test_cards.count()
                self.logger.info(f"  📊 Selector {selector} found {count} elements")
                if count > 0:
                    cards = test_cards
                    self.logger.info(f"✅ Found {count} hotels with selector: {selector}")
                    break
            except Exception as e:
                self.logger.debug(f"  ❌ Selector {selector} failed: {e}")
                continue
        
        if not cards:
            self.logger.warning("❌ No hotel cards found")
            return []
        
        # Extract data from each card
        card_count = min(await cards.count(), max_results)
        
        for i in range(card_count):
            try:
                card = cards.nth(i)
                hotel_data = await self._extract_basic_hotel_data(card, i)
                
                if hotel_data:
                    # Inject page-level price
                    if i < len(page_prices) and page_prices[i] > 0:
                        hotel_data['price_per_night'] = page_prices[i]
                        self.logger.info(f"✅ Added price {page_prices[i]} for hotel {i+1}")
                    
                    hotel_data['extraction_level'] = 1
                    hotels.append(hotel_data)
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to extract hotel {i+1}: {e}")
                continue
        
        return hotels

    async def _extract_page_level_prices(self, page, expected_count: int) -> List[float]:
        """Extract prices from page level (working solution from previous fixes)."""
        prices = []
        
        working_price_selectors = [
            "[data-testid='price-and-discounted-price']",
            "[data-testid*='price']"
        ]
        
        for selector in working_price_selectors:
            try:
                price_elements = page.locator(selector)
                count = await price_elements.count()
                
                if count >= expected_count:
                    self.logger.info(f"✅ Using price selector: {selector} ({count} elements)")
                    
                    for i in range(min(count, expected_count)):
                        try:
                            element = price_elements.nth(i)
                            price_text = await element.inner_text()
                            price_value = self._extract_price_number(price_text)
                            
                            if price_value and price_value > 0:
                                prices.append(price_value)
                            else:
                                prices.append(0)
                                
                        except:
                            prices.append(0)
                    
                    return prices[:expected_count]
                    
            except:
                continue
        
        # Fallback: return zeros
        return [0] * expected_count

    async def _extract_basic_hotel_data(self, card, index: int) -> Optional[Dict[str, Any]]:
        """Extract basic hotel data from property card."""
        try:
            hotel_data = {}
            
            # Hotel name
            name_selectors = [
                "[data-testid='title']",
                "h3",
                ".sr-hotel__name",
                "[class*='title']"
            ]
            
            for selector in name_selectors:
                try:
                    name_element = card.locator(selector).first
                    if await name_element.is_visible():
                        hotel_data['name'] = (await name_element.inner_text()).strip()
                        break
                except:
                    continue
            
            # Booking URL
            try:
                link_element = card.locator("a").first
                if await link_element.is_visible():
                    relative_url = await link_element.get_attribute("href")
                    if relative_url:
                        if relative_url.startswith('/'):
                            hotel_data['booking_url'] = f"{BookingTask.BASE_URL}{relative_url}"
                        else:
                            hotel_data['booking_url'] = relative_url
            except:
                pass
            
            # Rating
            rating_selectors = [
                "[data-testid='review-score']",
                ".bui-review-score__badge",
                "[aria-label*='Scored']"
            ]
            
            for selector in rating_selectors:
                try:
                    rating_element = card.locator(selector).first
                    if await rating_element.is_visible():
                        rating_text = await rating_element.inner_text()
                        rating_value = self._extract_rating_number(rating_text)
                        if rating_value:
                            hotel_data['rating'] = rating_value
                            break
                except:
                    continue
            
            # Review count
            review_selectors = [
                "[data-testid='review-score'] + div",
                ".bui-review-score__text",
                "*:has-text('review')"
            ]
            
            for selector in review_selectors:
                try:
                    review_element = card.locator(selector).first
                    if await review_element.is_visible():
                        review_text = await review_element.inner_text()
                        review_count = self._extract_review_count(review_text)
                        if review_count:
                            hotel_data['review_count'] = review_count
                            break
                except:
                    continue
            
            # Generate hotel ID
            if hotel_data.get('booking_url'):
                hotel_data['hotel_id'] = self._extract_hotel_id_from_url(hotel_data['booking_url'])
            
            # Set defaults for consistent structure across all levels
            hotel_data.setdefault('price_per_night', 0)
            hotel_data.setdefault('rating', 0)
            hotel_data.setdefault('review_count', 0)
            hotel_data.setdefault('address', "")
            hotel_data.setdefault('amenities', [])
            hotel_data.setdefault('images', [])
            hotel_data.setdefault('latitude', None)
            hotel_data.setdefault('longitude', None)
            hotel_data.setdefault('google_maps_url', "")
            hotel_data.setdefault('description', "")
            hotel_data.setdefault('reviews', [])
            
            hotel_data['scraping_timestamp'] = datetime.now().isoformat()
            hotel_data['source'] = "search_results"
            
            return hotel_data if hotel_data.get('name') else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to extract basic data for card {index}: {e}")
            return None

    async def _extract_hotel_details(self, page) -> Dict[str, Any]:
        """Extract detailed hotel information from hotel page."""
        details = {}
        
        try:
            # Address - try multiple strategies
            address_found = False
            self.logger.info("🏠 Starting address extraction")
            
            # Strategy 1: Enhanced address extraction with multiple approaches
            try:
                self.logger.info("🔍 Strategy 1: Comprehensive address extraction")
                
                # Enhanced location patterns with priority order
                location_patterns = [
                    # Priority 1: Specific address selectors
                    {"selector": "[data-testid*='address']", "extract": "direct", "priority": 1},
                    {"selector": "[data-testid*='location']", "extract": "location_info", "priority": 1},
                    {"selector": "*[class*='address']", "extract": "direct", "priority": 1},
                    
                    # Priority 2: Property info sections
                    {"selector": "[data-testid*='property-location']", "extract": "location_info", "priority": 2},
                    {"selector": "*[class*='location']", "extract": "location_info", "priority": 2},
                    {"selector": "*[class*='map'] + *", "extract": "nearby_text", "priority": 2},
                    
                    # Priority 3: General text search with keywords
                    {"selector": "p, span, div", "extract": "keyword_search", "priority": 3, 
                     "keywords": ["Dubai", "UAE", "United Arab Emirates", "Business Bay", "Downtown", "JBR", "Dubai Marina", "DIFC", "Burj Khalifa", "Address:", "Located"]}
                ]
                
                # Sort by priority
                location_patterns.sort(key=lambda x: x['priority'])
                
                for pattern_idx, pattern in enumerate(location_patterns):
                    if address_found:
                        break
                        
                    try:
                        self.logger.info(f"  🔍 Testing pattern {pattern_idx+1} (Priority {pattern['priority']}): {pattern['selector']}")
                        elements = page.locator(pattern["selector"])
                        count = await elements.count()
                        self.logger.info(f"  📊 Found {count} elements for pattern {pattern_idx+1}")
                        
                        if count == 0:
                            continue
                            
                        extract_type = pattern.get("extract", "direct")
                        
                        if extract_type == "direct":
                            # Direct address extraction
                            for i in range(min(count, 5)):
                                try:
                                    text = await elements.nth(i).inner_text()
                                    if text and len(text.strip()) > 10:
                                        text = text.strip()
                                        # Look for complete addresses
                                        if any(keyword in text.lower() for keyword in ['dubai', 'uae', 'united arab emirates', 'street', 'road', 'avenue']):
                                            if 15 < len(text) < 200:  # Reasonable address length
                                                details['address'] = text
                                                address_found = True
                                                self.logger.info(f"✅ Found direct address: {text}")
                                                break
                                except Exception as e:
                                    self.logger.debug(f"Error extracting direct address from element {i}: {e}")
                        
                        elif extract_type == "location_info":
                            # Extract from location information sections
                            for i in range(min(count, 5)):
                                try:
                                    text = await elements.nth(i).inner_text()
                                    if text and len(text) > 20:
                                        self.logger.info(f"  📍 Location info found: {text[:150]}...")
                                        
                                        # Look for specific address patterns
                                        lines = text.split('\n')
                                        for line in lines:
                                            line = line.strip()
                                            
                                            # Pattern 1: Area names with Dubai
                                            area_matches = ['Business Bay', 'Downtown', 'Dubai Marina', 'JBR', 'DIFC', 'Burj Khalifa', 'Dubai Mall', 'Sheikh Zayed Road']
                                            for area in area_matches:
                                                if area in line and 5 < len(line) < 100:
                                                    details['address'] = f"{line}, Dubai, UAE" if 'Dubai' not in line else line
                                                    address_found = True
                                                    self.logger.info(f"✅ Found area-based address: {details['address']}")
                                                    break
                                            
                                            if address_found:
                                                break
                                            
                                            # Pattern 2: Distance-based location descriptions
                                            if any(phrase in line.lower() for phrase in ['located', 'distance', 'mi from', 'km from']) and 'dubai' in line.lower():
                                                if 20 < len(line) < 150:
                                                    details['address'] = line
                                                    address_found = True
                                                    self.logger.info(f"✅ Found distance-based address: {line}")
                                                    break
                                        
                                        if address_found:
                                            break
                                except Exception as e:
                                    self.logger.debug(f"Error extracting location info from element {i}: {e}")
                        
                        elif extract_type == "keyword_search":
                            # Keyword-based search in text elements
                            keywords = pattern.get("keywords", [])
                            for i in range(min(count, 10)):  # More elements for general search
                                try:
                                    text = await elements.nth(i).inner_text()
                                    if text and len(text.strip()) > 15:
                                        text = text.strip()
                                        
                                        # Check if text contains location keywords
                                        keyword_found = any(keyword.lower() in text.lower() for keyword in keywords)
                                        if keyword_found:
                                            # Additional validation for address-like text
                                            if any(indicator in text.lower() for indicator in ['located', 'address', 'street', 'road', 'avenue', 'area', 'district']):
                                                if 20 < len(text) < 200:
                                                    details['address'] = text
                                                    address_found = True
                                                    self.logger.info(f"✅ Found keyword-based address: {text}")
                                                    break
                                except Exception as e:
                                    self.logger.debug(f"Error in keyword search element {i}: {e}")
                        
                        elif extract_type == "nearby_text":
                            # Look for text near map elements
                            for i in range(min(count, 3)):
                                try:
                                    text = await elements.nth(i).inner_text()
                                    if text and len(text.strip()) > 10:
                                        text = text.strip()
                                        if 'dubai' in text.lower() and any(phrase in text.lower() for phrase in ['location', 'address', 'area']):
                                            if 15 < len(text) < 150:
                                                details['address'] = text
                                                address_found = True
                                                self.logger.info(f"✅ Found nearby-text address: {text}")
                                                break
                                except Exception as e:
                                    self.logger.debug(f"Error extracting nearby text from element {i}: {e}")
                                    
                    except Exception as e:
                        self.logger.debug(f"Pattern {pattern_idx+1} failed: {e}")
                        continue
            except Exception as e:
                self.logger.debug(f"Address extraction strategy 1 failed: {e}")
            
            # Strategy 2: Legacy selectors as fallback
            if not address_found:
                self.logger.info("🔍 Strategy 2: Legacy address selectors")
                address_selectors = [
                    "[data-testid='address']",
                    ".hp_address_subtitle",
                    "[class*='address']"
                ]
                
                for idx, selector in enumerate(address_selectors):
                    try:
                        self.logger.info(f"  🔍 Testing legacy selector {idx+1}: {selector}")
                        element = page.locator(selector).first
                        if await element.is_visible():
                            address_text = (await element.inner_text()).strip()
                            details['address'] = address_text
                            self.logger.info(f"✅ Found address via legacy selector: {address_text}")
                            address_found = True
                            break
                    except Exception as e:
                        self.logger.debug(f"  Legacy selector {idx+1} failed: {e}")
                        continue
                        
            # Strategy 3: Extract from description as fallback
            if not address_found:
                self.logger.info("🔍 Strategy 3: Description-based location extraction")
                try:
                    description = await self._extract_description(page)
                    if description and len(description) > 20:
                        self.logger.info(f"📝 Description found: {description[:150]}...")
                        desc_lines = description.split('\n')
                        for line_idx, line in enumerate(desc_lines):
                            line = line.strip()
                            if len(line) < 15:  # Skip short lines
                                continue
                            
                            # Debug each line
                            self.logger.debug(f"  Checking line {line_idx}: {line[:100]}...")
                            
                            # Look for location statements in descriptions
                            has_location_keyword = 'located' in line.lower() or 'location' in line.lower()
                            has_dubai = 'Dubai' in line
                            area_keywords = ['Dubai Mall', 'city center', 'City Walk', 'Downtown', 'Marina', 'JBR', 'Business Bay', 'DIFC']
                            has_area = any(area in line for area in area_keywords)
                            
                            if (has_location_keyword and has_dubai and has_area and 15 < len(line) < 150):
                                # Clean up the line
                                clean_line = line.replace('Prime City Center Location: ', '').replace('Prime Location: ', '').replace('Elegant Accommodations: ', '')
                                details['address'] = clean_line.strip()
                                address_found = True
                                self.logger.info(f"✅ Found address from description: {details['address']}")
                                break
                            elif has_dubai and has_area and len(line) > 15:
                                # Looser match for any Dubai area mention
                                clean_line = line.replace('Prime City Center Location: ', '').replace('Prime Location: ', '')
                                details['address'] = clean_line.strip()
                                address_found = True
                                self.logger.info(f"✅ Found address from area mention: {details['address']}")
                                break
                    else:
                        self.logger.info("📝 No description found or too short")
                except Exception as e:
                    self.logger.warning(f"Description address extraction failed: {e}")
                    
            if not address_found:
                self.logger.warning("⚠️ No address found with any strategy")
            
            # Amenities
            details['amenities'] = await self._extract_amenities(page)
            
            # Images  
            details['images'] = await self._extract_images(page)
            
            # Location data (coordinates)
            location_data = await self._extract_location_data(page)
            if location_data:
                details.update(location_data)
            
            # Description
            description = await self._extract_description(page)
            if description:
                details['description'] = description
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting hotel details: {e}")
        
        return details

    async def _extract_basic_reviews(self, page) -> Optional[Dict[str, Any]]:
        """Extract 2-5 basic reviews from hotel page."""
        
        try:
            self.logger.info("📝 LEVEL 3: Starting basic review extraction (2-5 reviews)")
            
            # Navigate to reviews section
            current_url = page.url
            if '/hotel/' in current_url:
                base_url = current_url.split('?')[0].split('#')[0]
                reviews_url = f"{base_url}#tab-reviews"
                
                self.logger.info(f"📝 Level 3: Navigating to reviews section: {reviews_url}")
                await page.goto(reviews_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(3000)
            
            # Extract reviews using working selectors
            review_card_selectors = [
                "#reviewCardsSection [data-testid='review-card']",
                "[data-testid='review-card']",
                "#reviewCardsSection > div > div > div"
            ]
            
            reviews = []
            for selector in review_card_selectors:
                try:
                    review_cards = page.locator(selector)
                    count = await review_cards.count()
                    
                    if count > 0:
                        self.logger.info(f"📝 Level 3: Found {count} review cards with selector: {selector}")
                        # Extract up to 5 reviews for Level 3
                        for i in range(min(count, 5)):
                            try:
                                card = review_cards.nth(i)
                                review_data = await self._extract_single_review(card)
                                if review_data:
                                    review_data['page_number'] = 1
                                    reviews.append(review_data)
                            except Exception as e:
                                self.logger.warning(f"⚠️ Failed to extract review {i+1}: {e}")
                                continue
                        break
                        
                except:
                    continue
            
            if reviews:
                self.logger.info(f"✅ Level 3: Extracted {len(reviews)} basic reviews")
                return {'reviews': reviews}
            else:
                self.logger.warning("⚠️ Level 3: No reviews found")
                return None
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting basic reviews: {e}")
        
        return None

    async def _extract_comprehensive_reviews(self, page) -> Optional[Dict[str, Any]]:
        """Extract comprehensive reviews with pagination (Level 4)."""
        
        try:
            self.logger.info("🔥 LEVEL 4: Starting comprehensive review extraction with pagination")
            
            # Navigate to reviews section
            current_url = page.url
            if '/hotel/' in current_url:
                base_url = current_url.split('?')[0].split('#')[0]
                reviews_url = f"{base_url}#tab-reviews"
                
                self.logger.info(f"🔥 Level 4: Navigating to reviews section: {reviews_url}")
                await page.goto(reviews_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_timeout(5000)
            
            all_reviews = []
            page_number = 1
            max_pages = 50  # Safety limit
            
            while page_number <= max_pages:
                self.logger.info(f"🔥 LEVEL 4 FIXED: Processing review page {page_number}")
                
                # Wait for page to stabilize
                await page.wait_for_timeout(3000)
                
                # Extract reviews from current page
                page_reviews = []
                
                # Working selectors from backup
                review_card_selectors = [
                    "#reviewCardsSection [data-testid='review-card']",
                    "#reviewCardsSection > div > div > div",
                    "[data-testid='review-card']"
                ]
                
                for selector in review_card_selectors:
                    try:
                        review_cards = page.locator(selector)
                        count = await review_cards.count()
                        
                        if count > 0:
                            self.logger.info(f"🔥 Level 4: Found {count} review cards with selector: {selector}")
                            
                            for i in range(count):
                                try:
                                    card = review_cards.nth(i)
                                    review_data = await self._extract_single_review(card)
                                    if review_data:
                                        review_data['page_number'] = page_number
                                        page_reviews.append(review_data)
                                except Exception as e:
                                    self.logger.warning(f"⚠️ Failed to extract review {i+1}: {e}")
                                    continue
                            
                            break
                    except:
                        continue
                
                if not page_reviews:
                    self.logger.info("🔚 No more reviews found")
                    break
                
                # Remove duplicates based on review text
                unique_reviews = []
                existing_texts = set()
                for review in page_reviews:
                    review_text = review.get('review_text', '')
                    if review_text and review_text not in existing_texts:
                        unique_reviews.append(review)
                        existing_texts.add(review_text)
                
                all_reviews.extend(unique_reviews)
                self.logger.info(f"🔥 LEVEL 4 FIXED: Page {page_number} extracted {len(unique_reviews)} unique reviews (Total: {len(all_reviews)})")
                
                # Try to navigate to next page
                if not await self._click_next_page(page):
                    self.logger.info("🔚 No more pages available")
                    break
                
                page_number += 1
                await page.wait_for_timeout(2000)
            
            if all_reviews:
                self.logger.info(f"✅ Level 4: Extracted {len(all_reviews)} comprehensive reviews across {page_number - 1} pages")
                return {'reviews': all_reviews}
            else:
                self.logger.warning("⚠️ Level 4: No reviews found")
                return None
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error in comprehensive review extraction: {e}")
        
        return None

    async def _extract_reviews_from_page(self, page) -> List[Dict[str, Any]]:
        """Extract all reviews from current page."""
        reviews = []
        
        review_selectors = [
            "[data-testid='review-card']",
            "[class*='review-item']",
            ".c-review",
            "[class*='review'][class*='card']"
        ]
        
        for selector in review_selectors:
            try:
                review_cards = page.locator(selector)
                count = await review_cards.count()
                
                if count > 0:
                    self.logger.info(f"📝 Found {count} review cards with selector: {selector}")
                    
                    for i in range(count):
                        try:
                            card = review_cards.nth(i)
                            review_data = await self._extract_single_review(card)
                            if review_data:
                                review_data['page_number'] = 1  # Will be updated by caller
                                reviews.append(review_data)
                        except Exception as e:
                            self.logger.warning(f"⚠️ Failed to extract review {i+1}: {e}")
                            continue
                    
                    break  # Use first working selector
                    
            except:
                continue
        
        return reviews

    async def _extract_single_review(self, card) -> Optional[Dict[str, Any]]:
        """Extract data from a single review card."""
        try:
            review_data = {}
            
            # Extract review text using working selectors
            text_selectors = [
                "[data-testid='review-positive-text']",
                "[data-testid='review-negative-text']",
                ".c-review__body",
                "[class*='review-text']",
                ".bui-review-content__text"
            ]
            
            review_texts = []
            for selector in text_selectors:
                try:
                    elements = card.locator(selector)
                    count = await elements.count()
                    for i in range(count):
                        element = elements.nth(i)
                        if await element.is_visible():
                            text = await element.inner_text()
                            text = text.strip()
                            
                            # Validate review text
                            if text and self._is_valid_review_text(text):
                                review_texts.append(text)
                                self.logger.info(f"🔍 DEBUG: Review text validation - '{text[:50]}...' -> PASSED (debug mode)")
                except:
                    continue
            
            if review_texts:
                # Combine positive and negative text with separator
                review_data['review_text'] = " | ".join(review_texts)
                review_data['extraction_timestamp'] = datetime.now().isoformat()
                
                # Try to extract reviewer name (optional)
                try:
                    reviewer_selectors = [
                        "[data-testid='reviewer-name']",
                        ".bui-reviewer-name",
                        "[class*='reviewer']"
                    ]
                    
                    for selector in reviewer_selectors:
                        try:
                            name_element = card.locator(selector).first
                            if await name_element.is_visible():
                                reviewer_name = await name_element.inner_text()
                                if reviewer_name and self._is_valid_reviewer_name(reviewer_name.strip()):
                                    review_data['reviewer_name'] = reviewer_name.strip()
                                    break
                        except:
                            continue
                except:
                    pass
                
                return review_data
            else:
                self.logger.warning("⚠️ No valid review text found in card")
                return None
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting single review: {e}")
        
        return None

    async def _navigate_to_reviews_section(self, page) -> bool:
        """Navigate to reviews section if available."""
        try:
            # Look for reviews tab or section
            review_nav_selectors = [
                "a[href*='reviews']",
                "button:has-text('Reviews')",
                "[data-testid*='review']",
                "#reviews_tab"
            ]
            
            for selector in review_nav_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        await element.click()
                        await page.wait_for_timeout(2000)
                        self.logger.info(f"✅ Navigated to reviews using: {selector}")
                        return True
                except:
                    continue
            
            # Check if reviews are already visible on page
            review_check_selectors = [
                "[data-testid='review-card']",
                "[class*='review-item']"
            ]
            
            for selector in review_check_selectors:
                try:
                    count = await page.locator(selector).count()
                    if count > 0:
                        self.logger.info(f"✅ Reviews already visible ({count} found)")
                        return True
                except:
                    continue
            
            self.logger.warning("❌ Could not find or navigate to reviews section")
            return False
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error navigating to reviews: {e}")
            return False

    async def _click_next_page(self, page) -> bool:
        """Click next page button for review pagination."""
        try:
            next_selectors = [
                "button[aria-label='Next page']",
                "[data-testid='pagination-next']",
                "button:has-text('Next')",
                ".pagination-next"
            ]
            
            for selector in next_selectors:
                try:
                    button = page.locator(selector).first
                    if await button.is_visible() and await button.is_enabled():
                        await button.click()
                        await page.wait_for_timeout(2000)
                        self.logger.info(f"✅ Clicked next page: {selector}")
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error clicking next page: {e}")
            return False

    async def _extract_amenities(self, page) -> List[str]:
        """Extract hotel amenities."""
        amenities = []
        try:
            # Prioritize working selectors based on testing
            amenity_selectors = [
                "[data-testid='property-highlights'] li",  # Working selector - moved to top priority
                "div[data-testid='property-most-popular-facilities-wrapper'] span[data-testid='facility-name']",
                ".hp_desc_important_facilities li",
                ".important_facilities li",
                ".hotel-facilities__list li",
                ".bh-property-highlights li",
                "[data-testid='facility-name']",
                ".facility_text"
            ]
            
            self.logger.info(f"🏨 Extracting amenities with {len(amenity_selectors)} selectors")
            
            for i, selector in enumerate(amenity_selectors):
                try:
                    amenity_elements = page.locator(selector)
                    count = await amenity_elements.count()
                    self.logger.info(f"   Selector {i+1}: {selector} -> {count} elements")
                    
                    if count > 0:
                        for j in range(min(count, 20)):  # Limit to 20 amenities
                            try:
                                amenity_text = await amenity_elements.nth(j).inner_text()
                                if amenity_text and amenity_text.strip() and len(amenity_text.strip()) < 100:
                                    amenities.append(amenity_text.strip())
                                    self.logger.debug(f"     Found amenity: {amenity_text.strip()}")
                            except Exception as e:
                                self.logger.debug(f"     Failed to extract amenity {j}: {e}")
                                continue
                        
                        if amenities:
                            self.logger.info(f"✅ Using selector {i+1}: {selector}")
                            break
                except Exception as e:
                    self.logger.debug(f"   Selector {i+1} failed: {e}")
                    continue
            
            # Remove duplicates and limit
            unique_amenities = list(set(amenities))[:15]
            self.logger.info(f"✅ Extracted {len(unique_amenities)} amenities: {unique_amenities}")
            return unique_amenities
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting amenities: {e}")
            return []

    async def _extract_images(self, page) -> List[str]:
        """Extract hotel images."""
        images = []
        try:
            self.logger.info("🖼️ Extracting hotel images")
            
            # Working selectors from backup
            image_selectors = [
                "img[data-testid='hotel-photo']",
                ".bh-photo-grid img",
                ".hp-gallery img",
                ".hotel-photo img", 
                "img[src*='bstatic']",
                ".gallery-image img",
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
            
            for i, selector in enumerate(image_selectors):
                try:
                    img_elements = page.locator(selector)
                    count = await img_elements.count()
                    self.logger.info(f"   Selector {i+1}: {selector} -> {count} images")
                    
                    if count > 0:
                        for j in range(min(count, 15)):  # Limit images
                            try:
                                img = img_elements.nth(j)
                                src = await img.get_attribute("src") or await img.get_attribute("data-src")
                                if src and src not in seen_urls:
                                    fixed_url = self._fix_image_url(src)
                                    if fixed_url and 'bstatic.com' in fixed_url:  # Valid Booking.com images
                                        images.append(fixed_url)
                                        seen_urls.add(src)
                            except:
                                continue
                        
                        if images:
                            break
                except:
                    continue
            
            self.logger.info(f"✅ Extracted {len(images)} hotel images")
            return images[:10]  # Limit to 10 best images
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting images: {e}")
            return []

    async def _extract_location_data(self, page) -> Optional[Dict[str, Any]]:
        """Extract Google Maps URL and coordinates using the working method from backup."""
        try:
            self.logger.info("🗺️ Starting location data extraction (working method)")
            location_data = {}
            
            # Strategy 1: Extract coordinates from JavaScript environment (WORKING METHOD)
            self.logger.info("  🔍 Strategy 1: JavaScript environment coordinate extraction")
            coordinates = await page.evaluate("""
                () => {
                    // Try multiple methods to get coordinates (from working backup code)
                    try {
                        // Method 1: window.B.env (primary method that was working)
                        if (window.B && window.B.env) {
                            const env = window.B.env;
                            if (env.b_map_center_latitude && env.b_map_center_longitude) {
                                console.log('Found coordinates in window.B.env:', env.b_map_center_latitude, env.b_map_center_longitude);
                                return {
                                    lat: parseFloat(env.b_map_center_latitude),
                                    lng: parseFloat(env.b_map_center_longitude),
                                    source: 'window.B.env'
                                };
                            }
                        }
                        
                        // Method 2: Alternative Booking.com global objects
                        if (window.page && window.page.b_map_center_latitude) {
                            return {
                                lat: parseFloat(window.page.b_map_center_latitude),
                                lng: parseFloat(window.page.b_map_center_longitude),
                                source: 'window.page'
                            };
                        }
                        
                        // Method 3: Check data attributes on map elements
                        const mapElements = document.querySelectorAll('[data-lat], [data-latitude]');
                        for (let element of mapElements) {
                            const lat = element.getAttribute('data-lat') || element.getAttribute('data-latitude');
                            const lng = element.getAttribute('data-lng') || element.getAttribute('data-longitude');
                            if (lat && lng) {
                                return {
                                    lat: parseFloat(lat),
                                    lng: parseFloat(lng),
                                    source: 'data-attributes'
                                };
                            }
                        }
                        
                        // Method 4: Parse from window.map_center or similar globals
                        if (window.map_center) {
                            return {
                                lat: parseFloat(window.map_center.lat || window.map_center.latitude),
                                lng: parseFloat(window.map_center.lng || window.map_center.longitude),
                                source: 'window.map_center'
                            };
                        }
                        
                        // Method 5: Check for coordinates in any global Booking objects
                        const globals = Object.keys(window);
                        for (let key of globals) {
                            if (key.includes('booking') || key.includes('map') || key.includes('coord')) {
                                const obj = window[key];
                                if (obj && typeof obj === 'object') {
                                    if (obj.latitude && obj.longitude) {
                                        return {
                                            lat: parseFloat(obj.latitude),
                                            lng: parseFloat(obj.longitude),
                                            source: key
                                        };
                                    }
                                    if (obj.lat && obj.lng) {
                                        return {
                                            lat: parseFloat(obj.lat),
                                            lng: parseFloat(obj.lng),
                                            source: key
                                        };
                                    }
                                }
                            }
                        }
                        
                        console.log('No coordinates found in JavaScript environment');
                        return null;
                    } catch (error) {
                        console.error('Error extracting coordinates:', error);
                        return null;
                    }
                }
            """)
            
            if coordinates:
                self.logger.info(f"✅ Found coordinates via {coordinates.get('source', 'unknown')}: {coordinates['lat']}, {coordinates['lng']}")
                # Validate Dubai area coordinates
                lat, lng = coordinates['lat'], coordinates['lng']
                if 24.0 < lat < 26.0 and 54.0 < lng < 56.0:
                    location_data['latitude'] = lat
                    location_data['longitude'] = lng
                    location_data['google_maps_url'] = f"https://www.google.com/maps/search/{lat},{lng}"
                    self.logger.info(f"✅ Valid Dubai coordinates: {lat}, {lng}")
                else:
                    self.logger.info(f"  ❌ Coordinates outside Dubai area: {lat}, {lng}")
            
            # Strategy 2: Try to find actual Google Maps links (backup method)
            if not location_data:
                self.logger.info("  🔍 Strategy 2: Direct Google Maps link detection")
                try:
                    maps_link = page.locator("a[href*='maps.google'], a[href*='google.com/maps']")
                    if await maps_link.is_visible():
                        href = await maps_link.get_attribute('href')
                        if href:
                            location_data['google_maps_url'] = href
                            self.logger.info(f"✅ Found direct Google Maps link: {href}")
                except Exception as e:
                    self.logger.debug(f"Direct maps link search failed: {e}")
            
            # Strategy 3: Enhanced script-based search as final fallback
            if not location_data:
                self.logger.info("  🔍 Strategy 3: Enhanced script content search")
                try:
                    scripts = page.locator("script")
                    count = await scripts.count()
                    
                    for i in range(min(count, 10)):  # Check fewer scripts but more targeted
                        try:
                            script_content = await scripts.nth(i).inner_text()
                            if len(script_content) < 100:  # Skip tiny scripts
                                continue
                                
                            # Look for specific Booking.com coordinate patterns
                            patterns = [
                                r'b_map_center_latitude["\']?\s*[:=]\s*["\']?([0-9.-]+)',
                                r'b_map_center_longitude["\']?\s*[:=]\s*["\']?([0-9.-]+)',
                                r'"latitude"\s*:\s*([0-9.-]+)',
                                r'"longitude"\s*:\s*([0-9.-]+)'
                            ]
                            
                            lat_match = None
                            lng_match = None
                            
                            for pattern in patterns[:2]:  # Booking-specific patterns first
                                if 'latitude' in pattern:
                                    lat_match = re.search(pattern, script_content)
                                else:
                                    lng_match = re.search(pattern, script_content)
                            
                            if not lat_match or not lng_match:
                                for pattern in patterns[2:]:  # Generic patterns as backup
                                    if 'latitude' in pattern:
                                        lat_match = re.search(pattern, script_content)
                                    else:
                                        lng_match = re.search(pattern, script_content)
                            
                            if lat_match and lng_match:
                                lat = float(lat_match.group(1))
                                lng = float(lng_match.group(1))
                                
                                if 24.0 < lat < 26.0 and 54.0 < lng < 56.0:
                                    location_data['latitude'] = lat
                                    location_data['longitude'] = lng
                                    location_data['google_maps_url'] = f"https://www.google.com/maps/search/{lat},{lng}"
                                    self.logger.info(f"✅ Found coordinates from script parsing: {lat}, {lng}")
                                    break
                        except Exception as e:
                            self.logger.debug(f"Script {i} parsing failed: {e}")
                            continue
                except Exception as e:
                    self.logger.debug(f"Script-based search failed: {e}")
            
            return location_data if location_data else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Location extraction error: {e}")
            return None

    async def _extract_description(self, page) -> Optional[str]:
        """Extract hotel description with comprehensive approach."""
        self.logger.info("📝 Starting description extraction")
        
        description_selectors = [
            # Updated 2025 selectors based on DOM exploration
            "[data-testid='property-description']",
            "[data-testid*='description']",
            "#hp_hotel_description",
            ".hp-description",
            "[class*='description']",
            "[data-testid='property-about']",
            "*[class*='about'] p",
            ".summary",
            ".hotel-info p",
            "[data-testid='property-summary']"
        ]
        
        for i, selector in enumerate(description_selectors):
            try:
                self.logger.info(f"  🔍 Testing description selector {i+1}: {selector}")
                elements = page.locator(selector)
                count = await elements.count()
                self.logger.info(f"  📊 Found {count} elements for selector {i+1}")
                
                if count > 0:
                    # Try first few elements
                    for j in range(min(count, 3)):
                        try:
                            element = elements.nth(j)
                            is_visible = await element.is_visible()
                            self.logger.info(f"    Element {j+1} visible: {is_visible}")
                            
                            if is_visible:
                                text = await element.inner_text()
                                if text and len(text.strip()) > 50:
                                    description = text.strip()[:1500]  # Longer descriptions
                                    self.logger.info(f"✅ Found description ({len(description)} chars): {description[:100]}...")
                                    return description
                            else:
                                # Try to get text even if not visible (sometimes works)
                                text = await element.inner_text()
                                if text and len(text.strip()) > 50:
                                    description = text.strip()[:1500]
                                    self.logger.info(f"✅ Found hidden description ({len(description)} chars): {description[:100]}...")
                                    return description
                        except Exception as e:
                            self.logger.debug(f"    Error with element {j+1}: {e}")
                            continue
            except Exception as e:
                self.logger.debug(f"  Selector {i+1} failed: {e}")
                continue
        
        # Strategy 2: Look for any large text blocks that might be descriptions
        try:
            self.logger.info("  🔍 Trying fallback: large text blocks")
            text_elements = page.locator("p, div")
            count = await text_elements.count()
            
            for i in range(min(count, 20)):  # Check first 20 text elements
                try:
                    element = text_elements.nth(i)
                    text = await element.inner_text()
                    
                    # Look for descriptive text (long paragraphs with hotel keywords)
                    if (text and len(text.strip()) > 100 and 
                        any(keyword in text.lower() for keyword in ['hotel', 'room', 'guest', 'accommodation', 'luxury', 'experience'])):
                        description = text.strip()[:1500]
                        self.logger.info(f"✅ Found fallback description ({len(description)} chars): {description[:100]}...")
                        return description
                except:
                    continue
        except Exception as e:
            self.logger.debug(f"Fallback strategy failed: {e}")
        
        self.logger.info("📝 No description found")
        return None

    async def _apply_search_filters(self, page, params: Dict[str, Any]):
        """Apply search filters on the search results page."""
        try:
            # Price filters
            if params.get("min_price") or params.get("max_price"):
                await self._set_price_filters(page, params.get("min_price"), params.get("max_price"))
            
            # Rating filter
            if params.get("min_rating"):
                await self._set_rating_filter(page, params["min_rating"])
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error applying filters: {e}")

    async def _set_price_filters(self, page, min_price: Optional[int], max_price: Optional[int]):
        """Set price range filters."""
        try:
            if min_price:
                min_input = page.locator("input[data-testid='filters-group-price-min']").first
                if await min_input.is_visible():
                    await min_input.fill(str(min_price))
                    
            if max_price:
                max_input = page.locator("input[data-testid='filters-group-price-max']").first
                if await max_input.is_visible():
                    await max_input.fill(str(max_price))
                    
            # Apply filters
            apply_button = page.locator("button:has-text('Apply')").first
            if await apply_button.is_visible():
                await apply_button.click()
                await page.wait_for_timeout(3000)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error setting price filters: {e}")

    async def _set_rating_filter(self, page, min_rating: float):
        """Set minimum rating filter."""
        try:
            # Look for rating filter checkboxes
            rating_filters = page.locator("[data-testid*='rating']")
            count = await rating_filters.count()
            
            for i in range(count):
                try:
                    filter_element = rating_filters.nth(i)
                    filter_text = await filter_element.inner_text()
                    
                    # Check if this filter matches our minimum rating
                    if str(int(min_rating)) in filter_text:
                        await filter_element.click()
                        await page.wait_for_timeout(2000)
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Error setting rating filter: {e}")

    async def _handle_popups(self, page):
        """Handle common popups and overlays."""
        try:
            popup_selectors = [
                "button:has-text('Accept')",
                "button:has-text('OK')",
                "[data-testid='close-dialog']",
                ".modal-mask .close-button"
            ]
            
            for selector in popup_selectors:
                try:
                    button = page.locator(selector).first
                    if await button.is_visible():
                        await button.click()
                        await page.wait_for_timeout(1000)
                except:
                    continue
                    
        except:
            pass

    def _extract_price_number(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text."""
        if not price_text:
            return None
        
        try:
            # Remove currency symbols and common text
            text = re.sub(r'From\s+|per\s+night|USD|AED|SAR|€|\$|,', '', price_text, flags=re.IGNORECASE)
            
            # Extract numeric value
            numbers = re.findall(r'[\d,]+\.?\d*', text)
            if numbers:
                price_str = numbers[0].replace(',', '')
                price = float(price_str)
                
                # Validate price range (10-50000)
                if 10 <= price <= 50000:
                    return price
                    
        except (ValueError, IndexError):
            pass
        
        return None

    def _extract_rating_number(self, rating_text: str) -> Optional[float]:
        """Extract numeric rating from text."""
        if not rating_text:
            return None
        
        try:
            # Look for decimal numbers
            numbers = re.findall(r'\d+\.?\d*', rating_text)
            if numbers:
                rating = float(numbers[0])
                # Validate rating range (1-10)
                if 1 <= rating <= 10:
                    return rating
        except (ValueError, IndexError):
            pass
        
        return None

    def _extract_review_count(self, reviews_text: str) -> Optional[int]:
        """Extract review count from text."""
        if not reviews_text:
            return None
        
        try:
            # Look for numbers followed by "review"
            match = re.search(r'(\d+)\s*review', reviews_text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        except (ValueError, AttributeError):
            pass
        
        return None

    def _extract_hotel_id_from_url(self, url: str) -> Optional[str]:
        """Generate unique hotel ID from booking URL."""
        if not url:
            return None
        
        try:
            # Extract hotel identifier from URL
            parsed = urlparse(url)
            path_parts = parsed.path.split('/')
            
            for part in path_parts:
                if part and len(part) > 5 and not part.isdigit():
                    # Create hash of the identifier
                    return hashlib.md5(part.encode()).hexdigest()[:8]
                    
            # Fallback: hash the entire URL
            return hashlib.md5(url.encode()).hexdigest()[:8]
            
        except:
            return None

    def _is_valid_review_text(self, text: str) -> bool:
        """Validate if text is a proper review."""
        if not text or len(text.strip()) < 10:
            return False
        
        # Check for meaningful content
        words = text.split()
        return len(words) >= 3

    def _is_valid_reviewer_name(self, name: str) -> bool:
        """Validate if text is a proper reviewer name."""
        if not name or len(name.strip()) < 2:
            return False
        
        # Remove fake/generic names
        invalid_patterns = [
            'wonderful', 'excellent', 'great', 'good', 'nice',
            'anonymous', 'guest', 'user', 'reviewer', 'customer'
        ]
        
        name_lower = name.lower()
        for pattern in invalid_patterns:
            if pattern in name_lower:
                return False
        
        # Basic validation - should contain letters
        if not re.search(r'[a-zA-Z]', name):
            return False
        
        return True

    def _fix_image_url(self, url: str) -> str:
        """Fix and normalize image URL."""
        if not url:
            return ""
        
        # Remove size parameters for higher quality
        url = re.sub(r'\?k=[^&]+', '', url)
        url = re.sub(r'&k=[^&]+', '', url)
        
        # Ensure HTTPS
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('http:'):
            url = url.replace('http:', 'https:')
            
        return url

