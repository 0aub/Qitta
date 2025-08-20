"""
Professional Booking.com Hotel Scraper
====================================

A modern, robust scraper that efficiently extracts comprehensive hotel data
from Booking.com using advanced GraphQL interception and intelligent fallbacks.

Architecture:
- Clean separation of concerns
- Professional error handling  
- Comprehensive data extraction
- Smart fallback mechanisms
- Data quality validation

Author: Claude Code
Version: 2.0 (Complete Rewrite)
"""

import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs


class BookingHotelsTask:
    """Professional Booking.com hotel scraper with advanced GraphQL interception."""
    
    BASE_URL = "https://www.booking.com"
    
    # Modern price extraction patterns
    PRICE_PATTERNS = [
        r'SAR\s*[\d,]+\.?\d*',
        r'[\d,]+\.?\d*\s*SAR', 
        r'\$\s*[\d,]+\.?\d*',
        r'[\d,]+\.?\d*\s*\$',
        r'€\s*[\d,]+\.?\d*',
        r'[\d,]+\.?\d*\s*€',
        r'ر\.س\s*[\d,]+\.?\d*',
        r'[\d,]+\.?\d*\s*ر\.س'
    ]
    
    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point - uses MINIMAL weekend deals only scraper."""
        try:
            # Force cache refresh - minimal scraper integrated directly
            logger.info("🚀 Using MINIMAL weekend deals only scraper to avoid European hotel pollution")
            
            # Validate parameters
            clean_params = BookingHotelsTask._validate_params(params)
            
            # Initialize minimal scraper
            scraper = MinimalScraperEngine(browser, logger)
            
            # Execute ONLY weekend deals extraction
            hotels = await scraper.scrape_weekend_deals_only(clean_params)
            
            # Calculate metrics
            success_rate = len([h for h in hotels if h.get('price_per_night')]) / len(hotels) if hotels else 0
            avg_price = sum(h.get('price_per_night', 0) for h in hotels if h.get('price_per_night')) / len([h for h in hotels if h.get('price_per_night')]) if hotels else 0
            avg_rating = sum(h.get('rating', 0) for h in hotels if h.get('rating')) / len([h for h in hotels if h.get('rating')]) if hotels else 0
            
            logger.info(f"🏁 MINIMAL scraper completed: {len(hotels)} Saudi hotels, {success_rate:.1%} with prices")
            
            result = {
                "search_metadata": {
                    "location": clean_params["location"],
                    "check_in": clean_params["check_in"],
                    "check_out": clean_params["check_out"],
                    "nights": clean_params["nights"],
                    "guests": {
                        "adults": clean_params["adults"],
                        "children": clean_params["children"],
                        "rooms": clean_params["rooms"]
                    },
                    "extraction_method": "weekend_deals_only",
                    "total_found": len(hotels),
                    "success_rate": success_rate,
                    "average_price": avg_price,
                    "average_rating": avg_rating,
                    "search_completed_at": datetime.now().isoformat()
                },
                "hotels": hotels
            }
            
            # Save data to job output directory if provided
            if job_output_dir and hotels:
                import os
                output_file = os.path.join(job_output_dir, "hotels_data.json")
                os.makedirs(job_output_dir, exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                logger.info(f"💾 Saved hotel data to {output_file}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Minimal scraper failed: {e}")
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
        
        # Default to tomorrow + 3 days if not provided
        if not check_in:
            check_in = (datetime.now() + timedelta(days=1)).date()
        if not check_out:
            check_out = check_in + timedelta(days=3)
        
        nights = (check_out - check_in).days
        
        return {
            "location": location,
            "check_in": check_in.isoformat(),
            "check_out": check_out.isoformat(),
            "nights": nights,
            "adults": max(1, int(params.get("adults", 2))),
            "children": max(0, int(params.get("children", 0))),
            "rooms": max(1, int(params.get("rooms", 1)))
        }
    
    @staticmethod
    def _validate_hotel_data(hotels: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Validate and clean hotel data for quality assurance."""
        validated = []
        
        for hotel in hotels:
            # Skip hotels without basic required data
            if not hotel.get('name') or hotel['name'] == 'Unknown Hotel':
                continue
                
            # Clean and validate price
            price = hotel.get('price_per_night')
            if price and (not isinstance(price, (int, float)) or price <= 0 or price > 50000):
                hotel['price_per_night'] = None
                
            # Clean and validate rating
            rating = hotel.get('rating')
            if rating and (not isinstance(rating, (int, float)) or rating < 0 or rating > 10):
                hotel['rating'] = None
                
            # Ensure required fields exist
            hotel.setdefault('images', [])
            hotel.setdefault('amenities', [])
            hotel.setdefault('reviews', [])
            hotel.setdefault('booking_url', None)
            
            validated.append(hotel)
        
        logger.info(f"✅ Validated {len(validated)} hotels from {len(hotels)} raw results")
        return validated
    
    @staticmethod
    def _calculate_completeness(hotel: Dict[str, Any]) -> float:
        """Calculate data completeness score for a hotel."""
        fields = ['name', 'price_per_night', 'rating', 'review_count', 'address', 'images', 'amenities']
        filled = sum(1 for field in fields if hotel.get(field) and 
                    (not isinstance(hotel[field], list) or len(hotel[field]) > 0))
        return filled / len(fields)


class BookingScraperEngine:
    """Core scraper engine with advanced GraphQL interception."""
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        self.intercepted_data = {
            "requests": [],
            "responses": [],
            "hotels": []
        }
    
    async def scrape_hotels(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute multi-phase hotel scraping with intelligent fallbacks."""
        
        # Create optimized browser context
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
        )
        
        page = await context.new_page()
        
        try:
            # Phase 1: Advanced GraphQL Interception (PRIMARY - if this works, use ONLY this)
            hotels = await self._phase1_graphql_interception(page, params)
            
            if len(hotels) >= 5:  # Success! Use ONLY GraphQL results
                self.logger.info(f"🇸🇦 SUCCESS: Phase 1 found {len(hotels)} Saudi hotels - using ONLY GraphQL results")
                enhanced_hotels = await self._phase4_data_enhancement(page, hotels)
                return enhanced_hotels
            
            # Fallback: If Phase 1 failed, try HTML extraction
            self.logger.warning(f"⚠️ Phase 1 only found {len(hotels)} hotels, trying HTML fallback...")
            html_hotels = await self._phase2_html_extraction(page, params)
            
            if len(html_hotels) >= 3:  # HTML found good results
                self.logger.info(f"🔄 Using HTML results: {len(html_hotels)} hotels")
                enhanced_hotels = await self._phase4_data_enhancement(page, html_hotels)
                return enhanced_hotels
            
            # Last resort: Try direct search scraping
            self.logger.warning(f"⚠️ HTML extraction also failed, trying direct search...")
            search_hotels = await self._phase3_search_scraping(page, params)
            
            # Use whatever we got
            final_hotels = hotels + html_hotels + search_hotels
            enhanced_hotels = await self._phase4_data_enhancement(page, final_hotels)
            
            return enhanced_hotels
            
        finally:
            await context.close()
    
    async def _phase1_graphql_interception(self, page, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Phase 1: Advanced GraphQL API interception with modern parsing."""
        self.logger.info("🔥 Phase 1: Advanced GraphQL Interception")
        
        # Set up intelligent request/response interception
        await self._setup_advanced_interception(page)
        
        # Navigate and trigger search
        await page.goto(self.BASE_URL, wait_until="networkidle")
        await self._handle_popups(page)
        await self._perform_search(page, params)
        
        # Wait for API responses
        await page.wait_for_timeout(8000)
        
        # Parse intercepted data with advanced algorithms
        hotels = await self._parse_graphql_responses()
        
        self.logger.info(f"✅ Phase 1 extracted {len(hotels)} hotels via GraphQL")
        return hotels
    
    async def _setup_advanced_interception(self, page):
        """Set up advanced GraphQL request/response interception."""
        
        async def handle_request(request):
            if "/dml/graphql" in request.url:
                self.intercepted_data["requests"].append({
                    "url": request.url,
                    "method": request.method,
                    "headers": dict(request.headers),
                    "payload": request.post_data,
                    "timestamp": datetime.now().isoformat()
                })
        
        async def handle_response(response):
            if "/dml/graphql" in response.url and response.status == 200:
                try:
                    data = await response.json()
                    
                    # NUCLEAR OPTION: Completely reject acid cards responses
                    if "data" in data and isinstance(data["data"], dict):
                        if "searchQueries" in data["data"] and isinstance(data["data"]["searchQueries"], dict):
                            if "searchAcidCarousel" in data["data"]["searchQueries"]:
                                self.logger.warning(f"🚫 BLOCKING acid cards response - contains European garbage")
                                return  # Don't even store this response
                    
                    self.intercepted_data["responses"].append({
                        "url": response.url,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    })
                    self.logger.debug(f"📊 Intercepted GraphQL response: {len(str(data))} bytes")
                except Exception as e:
                    self.logger.debug(f"Failed to parse GraphQL response: {e}")
        
        page.on("request", handle_request)
        page.on("response", handle_response)
    
    async def _handle_popups(self, page):
        """Handle cookie consent and other popups intelligently."""
        try:
            # Cookie consent
            selectors = [
                "button[data-testid*='cookie']",
                "button:has-text('Accept')",
                ".bui-button--primary",
                "#onetrust-accept-btn-handler"
            ]
            
            for selector in selectors:
                try:
                    await page.click(selector, timeout=2000)
                    self.logger.debug(f"✅ Handled popup: {selector}")
                    break
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Popup handling: {e}")
    
    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform intelligent search with proper form filling."""
        try:
            # Find and fill location
            await page.fill("input[name='ss']", params["location"])
            await page.wait_for_timeout(1000)
            
            # Select first autocomplete result
            try:
                await page.click("[data-testid='autocomplete-result']", timeout=3000)
            except:
                pass
            
            # Handle dates if date picker is available
            try:
                await page.click("[data-testid='date-display-field-start']", timeout=2000)
                await page.wait_for_timeout(1000)
            except:
                pass
            
            # Submit search
            await page.click("button[type='submit']:has-text('Search')", timeout=10000)
            await page.wait_for_load_state("networkidle", timeout=15000)
            
            self.logger.info("✅ Search executed successfully")
            
        except Exception as e:
            self.logger.warning(f"Search execution issue: {e}")
    
    async def _parse_graphql_responses(self) -> List[Dict[str, Any]]:
        """Parse GraphQL responses - ONLY weekend deals, FILTER OUT acid cards."""
        hotels = []
        parser = GraphQLDataParser(self.logger)
        
        weekend_deals_found = 0
        acid_cards_rejected = 0
        
        for response in self.intercepted_data["responses"]:
            try:
                data = response["data"]
                
                # CHECK: Does this response contain weekend deals? (ACCEPT)
                weekend_deals = parser._get_nested_value(data, ["data", "weekendDeals", "weekendDealsProperties"])
                if weekend_deals and isinstance(weekend_deals, list) and len(weekend_deals) > 0:
                    weekend_deals_found += 1
                    response_hotels = parser.parse_response(data)
                    hotels.extend(response_hotels)
                    self.logger.info(f"✅ Processed weekend deals response with {len(response_hotels)} Saudi hotels")
                    continue
                
                # CHECK: Does this response contain acid cards? (REJECT)
                acid_cards = parser._get_nested_value(data, ["data", "searchQueries", "searchAcidCarousel", "acidCards"])
                if acid_cards and isinstance(acid_cards, list) and len(acid_cards) > 0:
                    acid_cards_rejected += 1
                    self.logger.warning(f"❌ REJECTED acid cards response with {len(acid_cards)} irrelevant hotels")
                    continue
                
                # CHECK: Does this response contain direct search results? (ACCEPT conditionally)
                search_results = (
                    parser._get_nested_value(data, ["data", "searchQueries", "search", "results"]) or
                    parser._get_nested_value(data, ["data", "searchQueries", "searchResultsList", "results"])
                )
                if search_results and isinstance(search_results, list) and len(search_results) > 0:
                    response_hotels = parser.parse_response(data)
                    if response_hotels:  # Only add if parsing found Saudi hotels
                        hotels.extend(response_hotels)
                        self.logger.info(f"✅ Processed search results with {len(response_hotels)} Saudi hotels")
                    
            except Exception as e:
                self.logger.debug(f"GraphQL parsing error: {e}")
        
        self.logger.info(f"📊 GraphQL Summary: {weekend_deals_found} weekend deals accepted, {acid_cards_rejected} acid cards rejected")
        
        # Deduplicate hotels by ID
        seen_ids = set()
        unique_hotels = []
        for hotel in hotels:
            hotel_id = hotel.get('id')
            if hotel_id and hotel_id not in seen_ids:
                seen_ids.add(hotel_id)
                unique_hotels.append(hotel)
        
        return unique_hotels
    
    async def _phase2_html_extraction(self, page, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Phase 2: Intelligent HTML extraction from search results."""
        self.logger.info("🌐 Phase 2: Intelligent HTML Extraction")
        
        try:
            # Ensure we're on search results page
            await page.wait_for_selector("[data-testid='property-card']", timeout=10000)
            
            # Extract hotel containers
            containers = await page.query_selector_all("[data-testid='property-card']")
            self.logger.info(f"🎯 Found {len(containers)} hotel containers")
            
            hotels = []
            parser = HtmlDataParser(self.logger)
            
            for i, container in enumerate(containers[:15]):  # Limit to 15 for performance
                try:
                    hotel_data = await parser.parse_hotel_container(container, i)
                    if hotel_data:
                        hotels.append(hotel_data)
                except Exception as e:
                    self.logger.debug(f"Container {i} parsing error: {e}")
            
            self.logger.info(f"✅ Phase 2 extracted {len(hotels)} hotels via HTML")
            return hotels
            
        except Exception as e:
            self.logger.warning(f"Phase 2 HTML extraction failed: {e}")
            return []
    
    async def _phase3_search_scraping(self, page, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Phase 3: Direct search results scraping as final fallback."""
        self.logger.info("🔍 Phase 3: Direct Search Results Scraping")
        
        try:
            # Navigate to a fresh search if needed
            current_url = page.url
            if "searchresults" not in current_url:
                await self._perform_search(page, params)
            
            # Use aggressive selectors for hotel extraction
            aggressive_selectors = [
                ".sr_item",
                ".sr-hotel",
                "[data-hotelid]",
                ".bui-card",
                ".sr_property_block"
            ]
            
            hotels = []
            for selector in aggressive_selectors:
                try:
                    containers = await page.query_selector_all(selector)
                    if containers:
                        self.logger.info(f"🎯 Using selector: {selector} ({len(containers)} items)")
                        parser = HtmlDataParser(self.logger)
                        
                        for i, container in enumerate(containers[:10]):
                            try:
                                hotel_data = await parser.parse_hotel_container(container, i)
                                if hotel_data:
                                    hotels.append(hotel_data)
                            except Exception:
                                continue
                        break
                except Exception:
                    continue
            
            self.logger.info(f"✅ Phase 3 extracted {len(hotels)} hotels via search scraping")
            return hotels
            
        except Exception as e:
            self.logger.warning(f"Phase 3 search scraping failed: {e}")
            return []
    
    async def _phase4_data_enhancement(self, page, hotels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Phase 4: Enhance hotel data with additional details."""
        self.logger.info(f"✨ Phase 4: Enhancing {len(hotels)} hotels")
        
        enhancer = DataEnhancer(self.logger)
        
        for hotel in hotels:
            try:
                await enhancer.enhance_hotel_data(hotel, page)
            except Exception as e:
                self.logger.debug(f"Enhancement error for {hotel.get('name', 'unknown')}: {e}")
        
        return hotels


class GraphQLDataParser:
    """Advanced GraphQL response parser with intelligent data extraction."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def parse_response(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse GraphQL response - ONLY weekend deals, REJECT acid cards."""
        hotels = []
        
        # STEP 1: Try weekend deals ONLY (proven to work)
        weekend_deals = self._get_nested_value(data, ["data", "weekendDeals", "weekendDealsProperties"])
        if weekend_deals and isinstance(weekend_deals, list):
            self.logger.info(f"🎯 Found weekend deals with {len(weekend_deals)} hotels")
            for i, item in enumerate(weekend_deals):
                hotel = self._parse_hotel_item(item, i, "weekend_deals")
                if hotel and self._is_relevant_location(hotel):
                    hotels.append(hotel)
                    self.logger.info(f"✅ Added Saudi hotel: {hotel.get('name')} - ${hotel.get('price_per_night')} - {hotel.get('address')}")
            
            if len(hotels) >= 5:
                self.logger.info(f"🇸🇦 SUCCESS: Found {len(hotels)} Saudi hotels from weekend deals")
                return hotels
        
        # STEP 2: Try direct search results as backup
        search_paths = [
            ["data", "searchQueries", "search", "results"], 
            ["data", "searchQueries", "searchResultsList", "results"]
        ]
        
        for path in search_paths:
            try:
                hotel_array = self._get_nested_value(data, path)
                if hotel_array and isinstance(hotel_array, list):
                    self.logger.info(f"🔄 Trying search results with {len(hotel_array)} hotels")
                    for i, item in enumerate(hotel_array):
                        hotel = self._parse_hotel_item(item, i, f"search_{path[-1]}")
                        if hotel and self._is_relevant_location(hotel):
                            hotels.append(hotel)
                            self.logger.info(f"✅ Added Saudi hotel: {hotel.get('name')} - {hotel.get('address')}")
            except Exception as e:
                self.logger.debug(f"Error parsing {path}: {e}")
                continue
        
        if len(hotels) > 0:
            self.logger.info(f"🇸🇦 Found {len(hotels)} total Saudi hotels")
            return hotels
        else:
            self.logger.error(f"❌ FAILED: No Saudi hotels found in any data source")
            return []
    
    def _is_relevant_location(self, hotel: Dict[str, Any]) -> bool:
        """STRICT filtering - ONLY accept hotels clearly in Saudi Arabia."""
        if not hotel:
            return False
            
        address = hotel.get('address', '').lower()
        name = hotel.get('name', '').lower()
        
        # STRICT Saudi Arabia indicators - must be explicit
        saudi_indicators = [
            'riyadh', 'saudi arabia', 'al khobar', 'khobar', 'dammam', 
            'jeddah', 'mecca', 'medina', 'الرياض', 'السعودية'
        ]
        
        # EXPANDED European/irrelevant location indicators - aggressive rejection
        irrelevant_indicators = [
            'poland', 'zakopane', 'krakow', 'kiszkowo', 'hungary', 'budapest', 'terézváros',
            'germany', 'trier', 'lübbenau', 'simonsberg', 'london', 'uk', 'england',
            'france', 'gérardmer', 'italy', 'florence', 'amsterdam', 'netherlands',
            'portugal', 'lisbon', 'australia', 'gembrook', 'sweden', 'västergårds',
            'citytranslations', 'domki', 'ranczo', 'tiny house', 'agriturismo',
            'chambres', 'sapinette', 'shelters', 'dolinie'
        ]
        
        # Check for irrelevant locations first (aggressive rejection)
        text_to_check = f"{address} {name}"
        for indicator in irrelevant_indicators:
            if indicator in text_to_check:
                return False
        
        # STRICT requirement: Must have explicit Saudi indicator
        for indicator in saudi_indicators:
            if indicator in text_to_check:
                return True
                
        # Check country code as backup
        try:
            country_code = hotel.get('country_code', '').lower()
            if country_code == 'sa':
                return True
        except:
            pass
            
        # STRICT: Reject anything without clear Saudi indicators
        return False
    
    def _get_nested_value(self, data: Dict[str, Any], path: List[str]) -> Any:
        """Safely get nested dictionary value."""
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def _parse_hotel_item(self, item: Dict[str, Any], index: int, source: str) -> Optional[Dict[str, Any]]:
        """Parse individual hotel item from GraphQL."""
        try:
            # Extract name from various possible locations
            name = (
                item.get("propertyName") or 
                item.get("name") or
                self._get_nested_value(item, ["basicPropertyData", "name"]) or
                self._get_nested_value(item, ["basicPropertyData", "displayName", "text"]) or
                self._get_nested_value(item, ["translatedName"]) or
                "Unknown Hotel"
            )
            
            if name == "Unknown Hotel":
                return None
            
            # Extract price with enhanced patterns for SAR
            price = self._extract_price_advanced(item)
            
            # Extract rating from reviews
            rating = self._extract_rating(item)
            
            # Extract review count
            review_count = self._extract_review_count(item)
            
            # Extract location/address
            address = self._extract_address(item)
            
            # Extract country code for location filtering
            country_code = self._extract_country_code(item)
            
            # Extract images
            images = self._extract_images(item)
            
            # Generate hotel ID
            hotel_id = (
                item.get("propertyId") or
                item.get("id") or
                self._get_nested_value(item, ["basicPropertyData", "propertyId"]) or
                self._get_nested_value(item, ["basicPropertyData", "id"]) or
                f"{source}_{index}"
            )
            
            return {
                "id": str(hotel_id),
                "name": name,
                "price_per_night": price,
                "rating": rating,
                "review_count": review_count,
                "address": address,
                "country_code": country_code,
                "images": images,
                "amenities": [],
                "booking_url": None,
                "source": source,
                "reviews": []
            }
            
        except Exception as e:
            self.logger.debug(f"Hotel parsing error: {e}")
            return None
    
    def _extract_country_code(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract country code from hotel data."""
        try:
            # Check various locations for country code
            country_paths = [
                ["basicPropertyData", "location", "countryCode"],
                ["location", "countryCode"],
                ["countryCode"],
                ["landing", "countryCode"]
            ]
            
            for path in country_paths:
                country_code = self._get_nested_value(item, path)
                if country_code and isinstance(country_code, str):
                    return country_code.lower()
                    
        except Exception:
            pass
        return None
    
    def _extract_price_advanced(self, item: Dict[str, Any]) -> Optional[float]:
        """Advanced price extraction with multiple strategies."""
        
        # Strategy 1: Handle the new response format from your example
        price_fields = [
            # From your manual request format
            ["priceDisplayInfoIrene", "displayPrice", "amountPerStay", "amountRounded"],
            ["priceDisplayInfoIrene", "displayPrice", "amountPerStay", "amount"],
            # Traditional formats
            ["price", "formattedPrice"],
            ["price", "amount"],
            ["priceInfo", "formattedPrice"],
            ["priceInfo", "amount"],
            ["priceDisplayInfoIrene", "formattedPrice"],
            ["priceDisplayInfoIrene", "amount"],
            ["displayPrice", "amount"],
            ["basePrice", "amount"]
        ]
        
        for field_path in price_fields:
            try:
                price_value = self._get_nested_value(item, field_path)
                if price_value:
                    if isinstance(price_value, str):
                        # Extract numeric value from formatted strings like "SAR 1,519"
                        price = self._extract_numeric_price(price_value)
                        if price:
                            return price
                    elif isinstance(price_value, (int, float)) and price_value > 0:
                        return float(price_value)
            except Exception:
                continue
        
        return None
    
    def _extract_numeric_price(self, price_str: str) -> Optional[float]:
        """Extract numeric price from formatted string."""
        try:
            # Remove non-breaking spaces and other whitespace
            clean_str = price_str.replace('\xa0', ' ').replace(',', '')
            
            # Find numeric value
            import re
            number_match = re.search(r'[\d]+\.?\d*', clean_str)
            if number_match:
                price = float(number_match.group())
                # Validate reasonable price range
                if 10 <= price <= 50000:
                    return price
        except Exception:
            pass
        return None
    
    def _extract_rating(self, item: Dict[str, Any]) -> Optional[float]:
        """Extract rating from various fields."""
        rating_fields = [
            # From your manual request format
            ["basicPropertyData", "reviews", "totalScore"],
            ["reviews", "totalScore"],
            # Traditional formats
            ["review", "score"],
            ["review", "rating"],
            ["ratingInfo", "score"],
            ["ratingInfo", "rating"],
            ["rating"],
            ["score"]
        ]
        
        for field_path in rating_fields:
            try:
                rating_value = self._get_nested_value(item, field_path)
                if isinstance(rating_value, (int, float)) and 0 <= rating_value <= 10:
                    return float(rating_value)
                elif isinstance(rating_value, str):
                    import re
                    rating_match = re.search(r'(\d+\.?\d*)', rating_value)
                    if rating_match:
                        rating = float(rating_match.group(1))
                        if 0 <= rating <= 10:
                            return rating
            except Exception:
                continue
        
        return None
    
    def _extract_review_count(self, item: Dict[str, Any]) -> Optional[int]:
        """Extract review count from various fields."""
        count_fields = [
            # From your manual request format
            ["basicPropertyData", "reviews", "reviewsCount"],
            ["reviews", "reviewsCount"],
            # Traditional formats
            ["review", "reviewCount"],
            ["review", "count"],
            ["reviewInfo", "count"],
            ["reviewInfo", "reviewCount"],
            ["nbReviews"],
            ["reviewCount"]
        ]
        
        for field_path in count_fields:
            try:
                count_value = self._get_nested_value(item, field_path)
                if isinstance(count_value, (int, float)) and count_value > 0:
                    return int(count_value)
                elif isinstance(count_value, str):
                    import re
                    count_match = re.search(r'([\d,]+)', count_value.replace(',', ''))
                    if count_match:
                        return int(count_match.group(1))
            except Exception:
                continue
        
        return None
    
    def _extract_address(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract address/location information."""
        address_fields = [
            # Direct fields
            ["address"],
            ["subtitle"],
            ["location", "displayName"],
            ["locationInfo", "displayName"],
            ["districtName"],
            ["cityName"],
            # From GraphQL acid cards (no specific address field, use other indicators)
            ["localizedDistanceToCityCenter"],  # This might give location hints
        ]
        
        for field_path in address_fields:
            try:
                address_value = self._get_nested_value(item, field_path)
                if isinstance(address_value, str) and address_value.strip():
                    return address_value.strip()
            except Exception:
                continue
        
        # For acid cards, try to determine location from country code
        country_code = self._extract_country_code(item)
        if country_code == 'sa':
            return "Saudi Arabia"
        
        return None
    
    def _extract_images(self, item: Dict[str, Any]) -> List[str]:
        """Extract hotel images."""
        images = []
        
        image_fields = [
            # From your manual request format
            ["basicPropertyData", "photos", "main", "lowResJpegUrl", "relativeUrl"],
            # Traditional formats
            ["imageUrl"],
            ["carouselImage"],
            ["mainImage", "url"],
            ["images", 0, "url"],
            ["photos", 0, "url"]
        ]
        
        for field_path in image_fields:
            try:
                image_url = self._get_nested_value(item, field_path)
                if isinstance(image_url, str) and image_url.strip():
                    # Ensure full URL
                    if image_url.startswith("//"):
                        image_url = "https:" + image_url
                    elif image_url.startswith("/"):
                        image_url = "https://cf.bstatic.com" + image_url
                    
                    if image_url not in images:
                        images.append(image_url)
            except Exception:
                continue
        
        return images[:3]  # Limit to 3 images


class HtmlDataParser:
    """Intelligent HTML parser for hotel data extraction."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    async def parse_hotel_container(self, container, index: int) -> Optional[Dict[str, Any]]:
        """Parse hotel data from HTML container."""
        try:
            # Extract name
            name = await self._extract_name(container)
            if not name or name == "Unknown Hotel":
                return None
            
            # Extract price
            price = await self._extract_price_html(container)
            
            # Extract rating
            rating = await self._extract_rating_html(container)
            
            # Extract other data
            address = await self._extract_address_html(container)
            images = await self._extract_images_html(container)
            booking_url = await self._extract_booking_url(container)
            
            return {
                "id": f"html_hotel_{index}",
                "name": name,
                "price_per_night": price,
                "rating": rating,
                "review_count": None,
                "address": address,
                "images": images,
                "amenities": [],
                "booking_url": booking_url,
                "source": "html_extraction",
                "reviews": []
            }
            
        except Exception as e:
            self.logger.debug(f"HTML container parsing error: {e}")
            return None
    
    async def _extract_name(self, container) -> Optional[str]:
        """Extract hotel name from HTML."""
        name_selectors = [
            "[data-testid='title']",
            ".sr-hotel__name",
            ".bui-card__title",
            "h3", "h2", "h4",
            "[class*='name']",
            "[class*='title']"
        ]
        
        for selector in name_selectors:
            try:
                element = await container.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text and text.strip():
                        return text.strip()
            except Exception:
                continue
        
        return None
    
    async def _extract_price_html(self, container) -> Optional[float]:
        """Extract price from HTML with advanced pattern matching."""
        
        # Get all text content from container
        try:
            container_text = await container.text_content()
            if container_text:
                # Use advanced price extraction patterns
                for pattern in BookingHotelsTask.PRICE_PATTERNS:
                    import re
                    matches = re.findall(pattern, container_text, re.IGNORECASE)
                    for match in matches:
                        price = self._extract_numeric_price(match)
                        if price and 10 <= price <= 50000:
                            return price
        except Exception:
            pass
        
        return None
    
    def _extract_numeric_price(self, price_str: str) -> Optional[float]:
        """Extract numeric price from string."""
        try:
            import re
            numbers = re.findall(r'[\d,]+\.?\d*', price_str.replace(',', '').replace('\xa0', ''))
            if numbers:
                return float(numbers[0])
        except Exception:
            pass
        return None
    
    async def _extract_rating_html(self, container) -> Optional[float]:
        """Extract rating from HTML."""
        try:
            container_text = await container.text_content()
            if container_text:
                import re
                # Look for rating patterns like "8.5", "9.0"
                rating_match = re.search(r'(\d\.\d)', container_text)
                if rating_match:
                    rating = float(rating_match.group(1))
                    if 0 <= rating <= 10:
                        return rating
        except Exception:
            pass
        return None
    
    async def _extract_address_html(self, container) -> Optional[str]:
        """Extract address from HTML."""
        address_selectors = [
            "[data-testid='address']",
            ".sr-hotel__address",
            "[class*='location']",
            "[class*='address']"
        ]
        
        for selector in address_selectors:
            try:
                element = await container.query_selector(selector)
                if element:
                    text = await element.text_content()
                    if text and text.strip():
                        return text.strip()
            except Exception:
                continue
        
        return None
    
    async def _extract_images_html(self, container) -> List[str]:
        """Extract images from HTML."""
        images = []
        
        try:
            img_elements = await container.query_selector_all("img")
            for img in img_elements[:3]:  # Limit to 3 images
                src = await img.get_attribute("src")
                if src and "bstatic.com" in src:
                    if src.startswith("//"):
                        src = "https:" + src
                    images.append(src)
        except Exception:
            pass
        
        return images
    
    async def _extract_booking_url(self, container) -> Optional[str]:
        """Extract booking URL from HTML."""
        try:
            link_element = await container.query_selector("a[href*='/hotel/']")
            if link_element:
                href = await link_element.get_attribute("href")
                if href:
                    if href.startswith("/"):
                        return f"https://www.booking.com{href}"
                    return href
        except Exception:
            pass
        return None


class DataEnhancer:
    """Enhance hotel data with additional information."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    async def enhance_hotel_data(self, hotel: Dict[str, Any], page) -> None:
        """Enhance hotel data with additional details."""
        try:
            # Add any missing fields
            if not hotel.get('currency') and hotel.get('price_per_night'):
                hotel['currency'] = 'SAR'  # Default for Saudi searches
            
            # Add data quality score
            hotel['quality_score'] = self._calculate_quality_score(hotel)
            
        except Exception as e:
            self.logger.debug(f"Enhancement error: {e}")
    
    def _calculate_quality_score(self, hotel: Dict[str, Any]) -> float:
        """Calculate quality score for hotel data."""
        score = 0
        
        # Basic data (40%)
        if hotel.get('name'):
            score += 10
        if hotel.get('price_per_night'):
            score += 15
        if hotel.get('rating'):
            score += 10
        if hotel.get('address'):
            score += 5
        
        # Enhanced data (40%)
        if hotel.get('images') and len(hotel['images']) > 0:
            score += 15
        if hotel.get('review_count'):
            score += 10
        if hotel.get('amenities') and len(hotel['amenities']) > 0:
            score += 10
        if hotel.get('booking_url'):
            score += 5
        
        # Completeness bonus (20%)
        filled_fields = sum(1 for field in ['name', 'price_per_night', 'rating', 'address', 'images'] 
                           if hotel.get(field))
        score += (filled_fields / 5) * 20
        
        return score


class MinimalScraperEngine:
    """Minimal scraper engine - weekend deals API ONLY."""
    
    BASE_URL = "https://www.booking.com"
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        self.weekend_deals_responses = []
    
    async def scrape_weekend_deals_only(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute minimal scraping - weekend deals ONLY."""
        
        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        page = await context.new_page()
        
        try:
            # Set up weekend deals ONLY interception
            await self._setup_weekend_deals_interception(page)
            
            # Navigate and search
            await page.goto("https://www.booking.com", wait_until="networkidle")
            await self._handle_popups(page)
            await self._perform_search(page, params)
            
            # Wait for weekend deals responses
            await page.wait_for_timeout(10000)
            
            # Parse ONLY weekend deals
            hotels = await self._parse_weekend_deals_only()
            
            self.logger.info(f"✅ MINIMAL scraper extracted {len(hotels)} hotels from weekend deals only")
            return hotels
            
        finally:
            await context.close()
    
    async def _setup_weekend_deals_interception(self, page):
        """Set up interception that ONLY captures weekend deals responses."""
        
        async def handle_response(response):
            if "/dml/graphql" in response.url and response.status == 200:
                try:
                    data = await response.json()
                    
                    # ONLY accept weekend deals responses
                    if "data" in data and isinstance(data["data"], dict):
                        if "weekendDeals" in data["data"]:
                            weekend_deals = data["data"]["weekendDeals"]
                            if "weekendDealsProperties" in weekend_deals:
                                properties = weekend_deals["weekendDealsProperties"]
                                if properties and len(properties) > 0:
                                    self.weekend_deals_responses.append(data)
                                    self.logger.info(f"✅ Captured weekend deals response with {len(properties)} hotels")
                                    return
                    
                    # Log and ignore everything else
                    if "data" in data and "searchQueries" in str(data):
                        if "searchAcidCarousel" in str(data):
                            self.logger.warning(f"🚫 IGNORED acid cards response")
                        else:
                            self.logger.debug(f"🔍 Ignored other GraphQL response")
                    
                except Exception as e:
                    self.logger.debug(f"Response parsing error: {e}")
        
        page.on("response", handle_response)
    
    async def _handle_popups(self, page):
        """Handle cookie consent and popups."""
        try:
            selectors = [
                "button[data-testid*='cookie']",
                "button:has-text('Accept')",
                ".bui-button--primary",
                "#onetrust-accept-btn-handler"
            ]
            
            for selector in selectors:
                try:
                    await page.click(selector, timeout=2000)
                    self.logger.debug(f"✅ Handled popup: {selector}")
                    break
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Popup handling: {e}")
    
    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform search to trigger weekend deals API."""
        try:
            # Fill location
            await page.fill("input[name='ss']", params["location"])
            await page.wait_for_timeout(1000)
            
            # Select autocomplete
            try:
                await page.click("[data-testid='autocomplete-result']", timeout=3000)
            except:
                pass
            
            # Handle dates
            try:
                await page.click("[data-testid='date-display-field-start']", timeout=2000)
                await page.wait_for_timeout(1000)
            except:
                pass
            
            # Submit search
            await page.click("button[type='submit']:has-text('Search')", timeout=10000)
            await page.wait_for_load_state("networkidle", timeout=15000)
            
            self.logger.info("✅ Search executed successfully")
            
        except Exception as e:
            self.logger.warning(f"Search execution issue: {e}")
    
    async def _parse_weekend_deals_only(self) -> List[Dict[str, Any]]:
        """Parse ONLY weekend deals responses."""
        hotels = []
        parser = WeekendDealsParser(self.logger)
        
        for response_data in self.weekend_deals_responses:
            try:
                weekend_deals = response_data["data"]["weekendDeals"]["weekendDealsProperties"]
                for i, item in enumerate(weekend_deals):
                    hotel = parser.parse_weekend_deal_item(item, i)
                    if hotel:
                        hotels.append(hotel)
                        self.logger.info(f"✅ Parsed: {hotel['name']} - ${hotel.get('price_per_night', 'N/A')} SAR")
            except Exception as e:
                self.logger.debug(f"Weekend deals parsing error: {e}")
        
        return hotels


class WeekendDealsParser:
    """Parser for weekend deals data only."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def parse_weekend_deal_item(self, item: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
        """Parse individual weekend deal item."""
        try:
            # Extract name
            name = item.get("propertyName", "Unknown Hotel")
            if name == "Unknown Hotel":
                return None
            
            # Extract price
            price = self._extract_price(item)
            
            # Extract rating
            rating = None
            if "review" in item and item["review"]:
                review_data = item["review"]
                if "score" in review_data:
                    rating = float(review_data["score"])
            
            # Extract review count
            review_count = None
            if "review" in item and item["review"] and "reviewCount" in item["review"]:
                review_count = int(item["review"]["reviewCount"])
            
            # Extract address
            address = item.get("subtitle", "Saudi Arabia")
            
            # Extract images
            images = []
            if "imageUrl" in item and item["imageUrl"]:
                image_url = item["imageUrl"]
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
                elif image_url.startswith("/"):
                    image_url = "https://cf.bstatic.com" + image_url
                images.append(image_url)
            
            # Extract property ID
            property_id = item.get("propertyId", f"weekend_deal_{index}")
            
            return {
                "id": str(property_id),
                "name": name,
                "price_per_night": price,
                "rating": rating,
                "review_count": review_count,
                "address": address,
                "images": images,
                "amenities": [],
                "booking_url": None,
                "source": "weekend_deals_only",
                "reviews": []
            }
            
        except Exception as e:
            self.logger.debug(f"Weekend deal item parsing error: {e}")
            return None
    
    def _extract_price(self, item: Dict[str, Any]) -> Optional[float]:
        """Extract price from weekend deal item."""
        try:
            if "price" in item and item["price"]:
                price_data = item["price"]
                
                # Try formattedPrice first
                if "formattedPrice" in price_data:
                    formatted_price = price_data["formattedPrice"]
                    if isinstance(formatted_price, str):
                        # Extract numeric value from strings like "SAR 939"
                        numbers = re.findall(r'[\d,]+\.?\d*', formatted_price.replace(',', '').replace('\xa0', ''))
                        if numbers:
                            price = float(numbers[0])
                            if 10 <= price <= 50000:  # Reasonable price range
                                return price
                
                # Try amount field
                if "amount" in price_data and isinstance(price_data["amount"], (int, float)):
                    price = float(price_data["amount"])
                    if 10 <= price <= 50000:
                        return price
                        
        except Exception as e:
            self.logger.debug(f"Price extraction error: {e}")
        
        return None


def _log(logger: logging.Logger, level: str, message: str):
    """Utility function for consistent logging."""
    getattr(logger, level)(message)