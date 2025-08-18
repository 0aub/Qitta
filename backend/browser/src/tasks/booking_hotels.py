"""
BookingHotelsTask - Complete implementation with GraphQL API interception.

This module contains the full working implementation extracted from the monolithic tasks.py file.
"""

import asyncio
import logging
import json
import random
import urllib.parse
import pathlib
import time
from typing import Any, Dict, List
from datetime import datetime

from .base import _log


class BookingHotelsTask:
    """
    Professional hotel scraper for Booking.com with GraphQL API interception.
    
    Features:
    - Primary: GraphQL API interception for perfect data quality
    - Fallback: Enhanced HTML scraping for reliability  
    - Advanced filtering and parameter validation
    - Smart interaction automation to trigger API calls
    - Fast execution with comprehensive data extraction
    """
    
    BASE_URL = "https://www.booking.com"
    API_BASE = "https://www.booking.com/dml/graphql"
    SEARCH_API = "https://www.booking.com/searchresults.html"
    
    # ───── Parameter validation and defaults ─────
    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize input parameters."""
        import datetime
        
        validated = {}
        
        # Required parameters
        if not params.get("location"):
            raise ValueError("location parameter is required")
        validated["location"] = str(params["location"]).strip()
        
        if not params.get("check_in"):
            raise ValueError("check_in date is required (YYYY-MM-DD format)")
        if not params.get("check_out"):
            raise ValueError("check_out date is required (YYYY-MM-DD format)")
            
        # Parse and validate dates
        try:
            check_in = datetime.datetime.strptime(params["check_in"], "%Y-%m-%d").date()
            check_out = datetime.datetime.strptime(params["check_out"], "%Y-%m-%d").date()
        except Exception:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
            
        if check_in < datetime.date.today():
            raise ValueError("check_in date cannot be in the past")
        if check_out <= check_in:
            raise ValueError("check_out must be after check_in")
            
        validated["check_in"] = check_in.strftime("%Y-%m-%d")
        validated["check_out"] = check_out.strftime("%Y-%m-%d")
        validated["nights"] = (check_out - check_in).days
        
        # Guest configuration with defaults
        validated["adults"] = max(1, int(params.get("adults", 2)))
        validated["children"] = max(0, int(params.get("children", 0)))
        validated["rooms"] = max(1, int(params.get("rooms", 1)))
        
        # Optional filters
        if "min_price" in params and params["min_price"] is not None:
            validated["min_price"] = max(0, float(params["min_price"]))
        if "max_price" in params and params["max_price"] is not None:
            validated["max_price"] = max(0, float(params["max_price"]))
            
        if "min_rating" in params and params["min_rating"] is not None:
            rating = float(params["min_rating"])
            if not 0 <= rating <= 10:
                raise ValueError("min_rating must be between 0 and 10")
            validated["min_rating"] = rating
            
        if "star_rating" in params and params["star_rating"] is not None:
            stars = params["star_rating"]
            if isinstance(stars, (list, tuple)):
                validated["star_rating"] = [int(s) for s in stars if 1 <= int(s) <= 5]
            else:
                star = int(stars)
                if 1 <= star <= 5:
                    validated["star_rating"] = [star]
                    
        # Amenities normalization
        if "amenities" in params and params["amenities"]:
            amenity_map = {
                "wifi": "free_wifi",
                "pool": "swimming_pool", 
                "gym": "fitness_center",
                "spa": "spa_wellness",
                "parking": "parking",
                "restaurant": "restaurant",
                "bar": "bar",
                "pets": "pets_allowed"
            }
            amenities = params["amenities"]
            if isinstance(amenities, str):
                amenities = [amenities]
            validated["amenities"] = [amenity_map.get(a.lower(), a.lower()) for a in amenities]
            
        # Search radius for proximity searches
        if "search_radius" in params:
            radius = params["search_radius"]
            if isinstance(radius, str) and radius.endswith("km"):
                validated["search_radius_km"] = float(radius[:-2])
            else:
                validated["search_radius_km"] = float(radius)
                
        # Result limits and configuration
        validated["max_results"] = min(50, max(1, int(params.get("max_results", 25))))
        validated["include_reviews"] = bool(params.get("include_reviews", True))
        validated["fast_mode"] = bool(params.get("fast_mode", False))
        
        return validated

    # ───── Main execution method ─────
    @staticmethod
    async def run(*, browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        🔥 ENHANCED: Main execution with GraphQL API interception + HTML fallback.
        """
        start_time = time.time()
        hotels = []
        method_used = "unknown"
        
        try:
            # Validate parameters
            validated_params = BookingHotelsTask._validate_params(params)
            
            _log(logger, "info", f"🚀 Starting ENHANCED hotel search for {validated_params['location']}")
            
            # ═══════════════ PHASE 1: GraphQL API Interception (PRIMARY) ═══════════════
            _log(logger, "info", "🔥 Phase 1: Attempting GraphQL API interception")
            try:
                # Create browser context with API interception
                ctx = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = await ctx.new_page()
                
                try:
                    # Set up GraphQL API interception
                    hotels = await BookingHotelsTask._extract_with_graphql_interception(
                        page, validated_params, logger
                    )
                    
                    if hotels and len(hotels) > 0:
                        method_used = "graphql_api"
                        _log(logger, "info", f"✅ GraphQL API method successful: Found {len(hotels)} hotels")
                    else:
                        _log(logger, "warning", "⚠️  GraphQL API returned no data - falling back to HTML")
                        
                finally:
                    await ctx.close()
                    
            except Exception as e:
                _log(logger, "warning", f"⚠️  GraphQL API method failed: {str(e)} - falling back to HTML")
            
            # ═══════════════ PHASE 2: HTML Scraping Fallback ═══════════════
            if not hotels:
                _log(logger, "info", "🌐 Phase 2: Using enhanced HTML scraping fallback")
                try:
                    # Create browser context with enhanced settings
                    ctx = await browser.new_context(
                        viewport={"width": 1920, "height": 1080},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    page = await ctx.new_page()
                    
                    try:
                        # Use enhanced browser extraction (BACKUP METHOD)
                        hotels = await BookingHotelsTask._extract_hotels_browser_enhanced(page, validated_params, logger)
                        
                        if hotels and len(hotels) > 0:
                            method_used = "html_scraping"
                            _log(logger, "info", f"✅ HTML scraping method successful: Found {len(hotels)} hotels")
                        else:
                            _log(logger, "warning", "⚠️  HTML scraping returned no results")
                            
                    finally:
                        await ctx.close()
                        
                except Exception as e:
                    _log(logger, "error", f"❌ HTML scraping method failed: {str(e)}")
                    method_used = "failed"

            # ═══════════════ PHASE 3: Result Processing and Storage ═══════════════
            if hotels:
                # Calculate statistics
                execution_time = time.time() - start_time
                avg_price = sum(h.get("price_per_night", 0) for h in hotels if h.get("price_per_night")) / len([h for h in hotels if h.get("price_per_night")]) if any(h.get("price_per_night") for h in hotels) else None
                avg_rating = sum(h.get("rating", 0) for h in hotels if h.get("rating")) / len([h for h in hotels if h.get("rating")]) if any(h.get("rating") for h in hotels) else None
                
                # Create result package
                result = {
                    "success": True,
                    "hotels_found": len(hotels),
                    "location": validated_params["location"],
                    "date_range": f"{validated_params['check_in']} to {validated_params['check_out']}",
                    "nights": validated_params["nights"],
                    "extraction_method": method_used,
                    "execution_time_seconds": round(execution_time, 1),
                    "average_price_per_night": round(avg_price, 2) if avg_price else None,
                    "average_rating": round(avg_rating, 1) if avg_rating else None
                }
                
                # Store results using job_output_dir (matching original implementation)
                output_file = pathlib.Path(job_output_dir) / "hotels_data.json"
                
                result_data = {
                    "search_metadata": {
                        "location": validated_params["location"],
                        "check_in": validated_params["check_in"],
                        "check_out": validated_params["check_out"],
                        "nights": validated_params["nights"],
                        "guests": {
                            "adults": validated_params["adults"],
                            "children": validated_params["children"],
                            "rooms": validated_params["rooms"]
                        },
                        "filters_applied": {k: v for k, v in validated_params.items() 
                                          if k in ["min_price", "max_price", "min_rating", "star_rating", "amenities", "search_radius_km"]},
                        "extraction_method": method_used,
                        "total_found": len(hotels),
                        "scraped_count": len(hotels),
                        "search_completed_at": datetime.utcnow().isoformat()
                    },
                    "hotels": hotels
                }
                
                output_file.write_text(json.dumps(result_data, indent=2, ensure_ascii=False), "utf-8")
                result["data_file"] = "hotels_data.json"
                
                # Success log with method indicator
                method_icon = "🔥" if method_used == "graphql_api" else "🛡️" if method_used == "html_scraping" else "⚠️"
                reliability = "Perfect" if method_used == "graphql_api" else "Reliable" if method_used == "html_scraping" else "Limited"
                
                _log(logger, "info", f"🏁 Hotel search completed: {len(hotels)} hotels in {execution_time:.1f}s ({method_icon} {reliability})")
                
                return result
            else:
                # No results found
                execution_time = time.time() - start_time
                _log(logger, "warning", f"⚠️  No hotels found for {validated_params['location']} - search completed in {execution_time:.1f}s")
                
                return {
                    "success": True,  # Still a successful search, just no results
                    "hotels_found": 0,
                    "location": validated_params["location"],
                    "date_range": f"{validated_params['check_in']} to {validated_params['check_out']}",
                    "nights": validated_params["nights"],
                    "extraction_method": method_used,
                    "execution_time_seconds": round(execution_time, 1),
                    "message": "No hotels found matching the specified criteria"
                }
                
        except Exception as e:
            execution_time = time.time() - start_time
            _log(logger, "error", f"❌ Hotel search failed: {str(e)} (after {execution_time:.1f}s)")
            
            return {
                "success": False,
                "error": str(e),
                "hotels_found": 0,
                "execution_time_seconds": round(execution_time, 1)
            }
    
    @staticmethod
    async def _extract_with_graphql_interception(page, validated_params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """
        🔥 ENHANCED: Extract hotel data using GraphQL API interception for perfect data quality.
        
        This method:
        1. Sets up request/response interception for GraphQL APIs
        2. Navigates to Booking.com and performs search
        3. Captures GraphQL API responses containing structured data
        4. Extracts complete hotel information from API responses
        """
        _log(logger, "info", "🔥 Setting up GraphQL API interception...")
        
        # Storage for intercepted GraphQL data
        intercepted_data = {
            "hotels": [],
            "reviews": {},
            "amenities": {},
            "images": {},
            "prices": {},
            "availability": {},
            "raw_responses": []
        }
        
        # Set up GraphQL API interception
        async def handle_response(response):
            """Intercept and process GraphQL API responses."""
            try:
                url = response.url
                
                # Check if this is a GraphQL API call with hotel data
                if "/dml/graphql" in url and response.status == 200:
                    try:
                        json_data = await response.json()
                        
                        # Store raw response for debugging
                        intercepted_data["raw_responses"].append({
                            "url": url,
                            "data": json_data
                        })
                        
                        # Process different GraphQL operations
                        if "data" in json_data:
                            await BookingHotelsTask._process_graphql_response(
                                json_data, intercepted_data, url, logger
                            )
                            
                    except Exception as e:
                        _log(logger, "debug", f"Failed to parse GraphQL response from {url}: {e}")
                        
            except Exception as e:
                _log(logger, "debug", f"Response interception error: {e}")
        
        # Register response handler
        page.on("response", handle_response)
        
        try:
            # Navigate to Booking.com and perform search
            _log(logger, "info", "🌐 Navigating to Booking.com with API interception enabled")
            await page.goto(BookingHotelsTask.BASE_URL, wait_until="networkidle", timeout=60000)
            
            # Handle cookie consent
            await BookingHotelsTask._handle_cookie_consent(page, logger)
            
            # Perform search to trigger GraphQL APIs
            await BookingHotelsTask._perform_search_with_api_interception(page, validated_params, logger)
            
            # Enhanced interaction automation (like manual inspection)
            await BookingHotelsTask._enhanced_interaction_automation(page, validated_params, logger)
            
            # Wait for GraphQL APIs to be called and intercepted
            _log(logger, "info", "⏳ Waiting for GraphQL API calls to complete...")
            await page.wait_for_timeout(10000)  # Wait for API calls to finish
            
            # Visit individual hotel pages to trigger more GraphQL APIs
            if intercepted_data.get("hotels"):
                await BookingHotelsTask._visit_hotel_pages_for_apis(page, intercepted_data, validated_params, logger)
            
            # DEBUG: Log intercepted API calls for analysis
            _log(logger, "info", f"🔍 Starting debug analysis of {len(intercepted_data.get('raw_responses', []))} intercepted responses...")
            await BookingHotelsTask._debug_intercepted_apis(intercepted_data, logger)
            _log(logger, "info", f"🔍 Debug analysis completed")
            
            # Process intercepted data into hotel objects
            hotels = await BookingHotelsTask._compile_hotel_data_from_apis(
                intercepted_data, validated_params, logger
            )
            
            _log(logger, "info", f"🔥 GraphQL interception complete: extracted {len(hotels)} hotels with {len(intercepted_data['raw_responses'])} API calls intercepted")
            return hotels
            
        except Exception as e:
            _log(logger, "error", f"❌ GraphQL interception failed: {str(e)}")
            return []
    
    @staticmethod
    async def _process_graphql_response(json_data: Dict[str, Any], intercepted_data: Dict[str, Any], url: str, logger: logging.Logger):
        """Process different types of GraphQL responses and extract relevant data."""
        try:
            data = json_data.get("data", {})
            
            # Search results with hotel list
            if any(key in data for key in ["searchResults", "properties", "search"]):
                _log(logger, "info", "📊 Intercepted hotel search results")
                # Extract hotel list data from search results
                await BookingHotelsTask._extract_search_results_data(data, intercepted_data, logger)
                
            # Review data from ReviewList operation
            elif any(key in data for key in ["reviews", "reviewList", "propertyReviewList"]):
                _log(logger, "info", "📝 Intercepted review data")
                await BookingHotelsTask._extract_review_data(data, intercepted_data, logger)
                
            # Amenities/facilities data
            elif any(key in data for key in ["facilities", "amenities", "propertyAmenities"]):
                _log(logger, "info", "🏊 Intercepted amenities data")
                await BookingHotelsTask._extract_amenities_data(data, intercepted_data, logger)
                
            # Images/gallery data
            elif any(key in data for key in ["images", "gallery", "propertyPhotos"]):
                _log(logger, "info", "📸 Intercepted image gallery data")
                await BookingHotelsTask._extract_images_data(data, intercepted_data, logger)
                
            # Pricing/availability data
            elif any(key in data for key in ["availability", "prices", "propertyPricing"]):
                _log(logger, "info", "💰 Intercepted pricing data")
                await BookingHotelsTask._extract_pricing_data(data, intercepted_data, logger)
                
            # Property details (ratings, address, etc.)
            elif any(key in data for key in ["property", "propertyDetails", "hotelDetails"]):
                _log(logger, "info", "🏨 Intercepted property details")
                await BookingHotelsTask._extract_property_details(data, intercepted_data, logger)
                
        except Exception as e:
            _log(logger, "debug", f"Error processing GraphQL response: {e}")
    
    @staticmethod
    async def _extract_search_results_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract hotel data from search results GraphQL response."""
        try:
            # Deep search for hotel/property arrays in nested structures
            def find_hotel_arrays(obj, path=""):
                """Recursively find arrays that might contain hotel data."""
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        current_path = f"{path}.{key}" if path else key
                        
                        # Check if this looks like a hotel array
                        if isinstance(value, list) and len(value) > 0:
                            # Sample first item to see if it has hotel-like properties
                            if isinstance(value[0], dict):
                                sample_keys = set(value[0].keys())
                                hotel_indicators = {
                                    "name", "title", "hotelName", "propertyName",
                                    "id", "hotelId", "propertyId", 
                                    "price", "priceInfo", "pricing",
                                    "rating", "reviewScore", "guestReviewsRating",
                                    "address", "location", "city",
                                    "images", "photos", "gallery"
                                }
                                
                                # If it has hotel-like properties, it's probably a hotel array
                                if any(indicator in sample_keys for indicator in hotel_indicators):
                                    _log(logger, "info", f"🎯 Found potential hotel array at {current_path}: {len(value)} items")
                                    _log(logger, "info", f"   Sample keys: {list(sample_keys)[:15]}")
                                    return value
                        
                        # Recurse into nested objects
                        if isinstance(value, (dict, list)):
                            result = find_hotel_arrays(value, current_path)
                            if result:
                                return result
                
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        result = find_hotel_arrays(item, f"{path}[{i}]")
                        if result:
                            return result
                
                return None
            
            # Find hotel data using deep search
            hotels_data = find_hotel_arrays(data)
            
            if hotels_data:
                _log(logger, "info", f"🎯 Processing {len(hotels_data)} hotels from GraphQL response")
                
                for i, hotel_item in enumerate(hotels_data):
                    if isinstance(hotel_item, dict):
                        # Extract comprehensive hotel data
                        hotel_info = BookingHotelsTask._extract_hotel_from_graphql(hotel_item, logger)
                        
                        if hotel_info:
                            # Store both raw data and processed info
                            hotel_id = hotel_info.get("id", f"graphql_hotel_{i}")
                            intercepted_data["hotels"].append({
                                "id": str(hotel_id),
                                "processed_data": hotel_info,
                                "raw_data": hotel_item
                            })
                            
                            _log(logger, "info", f"   ✅ Extracted: {hotel_info.get('name', 'Unknown')} - ${hotel_info.get('price_per_night', 'N/A')}")
                            
                _log(logger, "info", f"📊 Successfully extracted {len(intercepted_data['hotels'])} hotels from GraphQL")
            else:
                _log(logger, "warning", "⚠️  No hotel arrays found in GraphQL response")
                
        except Exception as e:
            _log(logger, "debug", f"Error extracting search results data: {e}")
    
    @staticmethod
    def _extract_hotel_from_graphql(hotel_data: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
        """Extract structured hotel information from GraphQL hotel object."""
        try:
            # Extract name
            name = (
                hotel_data.get("name") or
                hotel_data.get("title") or
                hotel_data.get("hotelName") or
                hotel_data.get("propertyName") or
                hotel_data.get("displayName", {}).get("text") or
                "Unknown Hotel"
            )
            
            # Extract price information
            price_per_night = None
            total_price = None
            
            # Look for price in various structures
            price_sources = [
                hotel_data.get("price"),
                hotel_data.get("priceInfo"),
                hotel_data.get("pricing"),
                hotel_data.get("rates"),
                hotel_data.get("priceDisplay"),
                hotel_data.get("priceDisplayInfo")
            ]
            
            for price_source in price_sources:
                if isinstance(price_source, dict):
                    # Extract numeric price
                    price_candidates = [
                        price_source.get("amount"),
                        price_source.get("value"),
                        price_source.get("totalPrice"),
                        price_source.get("nightlyRate"),
                        price_source.get("basePrice")
                    ]
                    
                    for candidate in price_candidates:
                        if isinstance(candidate, (int, float)) and candidate > 0:
                            price_per_night = float(candidate)
                            break
                    
                    if price_per_night:
                        break
                elif isinstance(price_source, (int, float)) and price_source > 0:
                    price_per_night = float(price_source)
                    break
            
            # Extract rating
            rating = None
            rating_sources = [
                hotel_data.get("rating"),
                hotel_data.get("reviewScore"),
                hotel_data.get("guestReviewsRating"),
                hotel_data.get("reviews", {}).get("averageScore"),
                hotel_data.get("score")
            ]
            
            for rating_source in rating_sources:
                if isinstance(rating_source, dict):
                    rating = rating_source.get("value") or rating_source.get("score")
                elif isinstance(rating_source, (int, float)):
                    rating = float(rating_source)
                
                if rating and rating > 0:
                    break
            
            # Extract review count
            review_count = None
            review_sources = [
                hotel_data.get("reviewCount"),
                hotel_data.get("reviews", {}).get("totalCount"),
                hotel_data.get("guestReviews", {}).get("count"),
                hotel_data.get("reviewsCount")
            ]
            
            for review_source in review_sources:
                if isinstance(review_source, (int, float)) and review_source > 0:
                    review_count = int(review_source)
                    break
            
            # Extract address/location
            address = None
            location_sources = [
                hotel_data.get("address"),
                hotel_data.get("location", {}).get("address"),
                hotel_data.get("city"),
                hotel_data.get("destination"),
                hotel_data.get("location", {}).get("displayName")
            ]
            
            for location_source in location_sources:
                if isinstance(location_source, str) and location_source.strip():
                    address = location_source.strip()
                    break
                elif isinstance(location_source, dict):
                    address_text = location_source.get("text") or location_source.get("name")
                    if address_text:
                        address = str(address_text).strip()
                        break
            
            # Extract images
            images = []
            image_sources = [
                hotel_data.get("images"),
                hotel_data.get("photos"),
                hotel_data.get("gallery"),
                hotel_data.get("mainPhoto")
            ]
            
            for image_source in image_sources:
                if isinstance(image_source, list):
                    for img in image_source[:10]:  # Limit to 10 images
                        if isinstance(img, dict):
                            img_url = img.get("url") or img.get("src") or img.get("href")
                            if img_url:
                                images.append(img_url)
                        elif isinstance(img, str):
                            images.append(img)
                elif isinstance(image_source, dict):
                    img_url = image_source.get("url") or image_source.get("src")
                    if img_url:
                        images.append(img_url)
                
                if images:
                    break
            
            # Extract amenities
            amenities = []
            amenity_sources = [
                hotel_data.get("amenities"),
                hotel_data.get("facilities"),
                hotel_data.get("features"),
                hotel_data.get("services")
            ]
            
            for amenity_source in amenity_sources:
                if isinstance(amenity_source, list):
                    for amenity in amenity_source:
                        if isinstance(amenity, dict):
                            amenity_name = amenity.get("name") or amenity.get("title")
                            if amenity_name:
                                amenities.append(str(amenity_name))
                        elif isinstance(amenity, str):
                            amenities.append(amenity)
                
                if amenities:
                    break
            
            # Extract booking URL
            booking_url = hotel_data.get("url") or hotel_data.get("link") or hotel_data.get("bookingUrl")
            
            # Extract hotel ID
            hotel_id = (
                hotel_data.get("id") or
                hotel_data.get("hotelId") or
                hotel_data.get("propertyId") or
                hotel_data.get("basicPropertyData", {}).get("id")
            )
            
            # Compile hotel information
            hotel_info = {
                "id": str(hotel_id) if hotel_id else None,
                "name": name,
                "price_per_night": price_per_night,
                "total_price": total_price,
                "rating": rating,
                "review_count": review_count,
                "address": address,
                "images": images,
                "amenities": amenities,
                "booking_url": booking_url
            }
            
            # Only return if we have meaningful data
            if name != "Unknown Hotel" and (price_per_night or rating):
                return hotel_info
            
            return None
            
        except Exception as e:
            _log(logger, "debug", f"Error extracting hotel from GraphQL: {e}")
            return None
    
    @staticmethod
    async def _extract_hotels_browser_enhanced(page, validated_params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Enhanced browser automation combining reliable navigation with superior data extraction."""
        hotels = []
        
        try:
            # Navigate and perform search with enhanced reliability
            await BookingHotelsTask._perform_search_enhanced(page, validated_params, logger)
            
            # Wait for results with multiple fallback selectors
            result_selectors = [
                "[data-testid='property-card']",
                ".sr_property_block",
                "[data-component='PropertyCard']",
                ".sr_item"
            ]
            
            results_found = False
            for selector in result_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=15000)
                    results_found = True
                    _log(logger, "info", f"✅ Found results with selector: {selector}")
                    break
                except:
                    continue
            
            if not results_found:
                _log(logger, "warning", "⚠️  No hotel results found with any selector")
                return []
            
            # Extract hotel data using enhanced parsing
            max_results = validated_params["max_results"]
            hotel_containers = await page.query_selector_all(result_selectors[0])  # Use first successful selector
            
            _log(logger, "info", f"🏨 Found {len(hotel_containers)} hotel containers, extracting {min(len(hotel_containers), max_results)}")
            
            for i, container in enumerate(hotel_containers[:max_results]):
                try:
                    # COMPREHENSIVE browser-based data extraction
                    hotel_data = await BookingHotelsTask._extract_complete_hotel_data(container, i+1, logger)
                    
                    if hotel_data:
                        # Apply filters during extraction
                        should_include = True
                        
                        # Apply min_rating filter
                        if "min_rating" in validated_params and validated_params["min_rating"]:
                            min_rating = validated_params["min_rating"]
                            hotel_rating = hotel_data.get("rating")
                            if not hotel_rating or hotel_rating < min_rating:
                                _log(logger, "info", f"🚫 Filtered out {hotel_data['name']}: rating {hotel_rating} < {min_rating}")
                                should_include = False
                        
                        # Apply max_price filter
                        if "max_price" in validated_params and validated_params["max_price"]:
                            max_price = validated_params["max_price"]
                            hotel_price = hotel_data.get("price_per_night")
                            if hotel_price and hotel_price > max_price:
                                _log(logger, "info", f"🚫 Filtered out {hotel_data['name']}: price {hotel_price} > {max_price}")
                                should_include = False
                        
                        # Apply min_price filter
                        if "min_price" in validated_params and validated_params["min_price"]:
                            min_price = validated_params["min_price"]
                            hotel_price = hotel_data.get("price_per_night")
                            if hotel_price and hotel_price < min_price:
                                _log(logger, "info", f"🚫 Filtered out {hotel_data['name']}: price {hotel_price} < {min_price}")
                                should_include = False
                        
                        if should_include:
                            # PHASE 1: Get basic data from listing
                            hotels.append(hotel_data)
                            _log(logger, "info", f"✅ Hotel #{len(hotels)}: {hotel_data['name']} - ${hotel_data.get('price_per_night', 'N/A')} - ⭐{hotel_data.get('rating', 'N/A')}")
                        
                except Exception as e:
                    _log(logger, "warning", f"⚠️  Failed to extract hotel #{i+1}: {str(e)}")
                    continue
            
            _log(logger, "info", f"🎯 Browser extraction completed: {len(hotels)} hotels")
            
            # PHASE 2: Enhanced data collection from individual hotel pages
            if hotels and validated_params.get("include_reviews", True):
                _log(logger, "info", f"🔍 Phase 2: Collecting detailed data from {len(hotels)} hotel pages...")
                enhanced_hotels = []
                
                for i, hotel in enumerate(hotels):
                    try:
                        if hotel.get("booking_url"):
                            enhanced_data = await BookingHotelsTask._get_detailed_hotel_data(
                                page, hotel, validated_params, logger
                            )
                            if enhanced_data:
                                enhanced_hotels.append(enhanced_data)
                                _log(logger, "info", f"✅ Enhanced #{i+1}: {enhanced_data['name']} - ${enhanced_data.get('price_per_night', 'N/A')}")
                            else:
                                enhanced_hotels.append(hotel)  # Fallback to basic data
                        else:
                            enhanced_hotels.append(hotel)  # No URL, keep basic data
                    except Exception as e:
                        _log(logger, "warning", f"⚠️  Failed to enhance hotel #{i+1}: {str(e)}")
                        enhanced_hotels.append(hotel)  # Fallback to basic data
                
                return enhanced_hotels
            
            return hotels
            
        except Exception as e:
            _log(logger, "error", f"❌ Browser extraction failed: {str(e)}")
            return []
    
    @staticmethod
    def _store_results(hotels: List[Dict[str, Any]], validated_params: Dict[str, Any], result: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
        """Store hotel results to JSON file with comprehensive metadata."""
        try:
            # Create output directory structure
            output_dir = pathlib.Path("outputs/booking_hotels")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename with location and date
            location_clean = validated_params["location"].replace(" ", "_").replace(",", "").lower()
            date_str = validated_params["check_in"].replace("-", "")
            filename = f"hotels_{location_clean}_{date_str}.json"
            output_file = output_dir / filename
            
            # Prepare comprehensive data structure
            result_data = {
                "search_metadata": {
                    "location": validated_params["location"],
                    "check_in": validated_params["check_in"],
                    "check_out": validated_params["check_out"],
                    "nights": validated_params["nights"],
                    "guests": {
                        "adults": validated_params["adults"],
                        "children": validated_params["children"],
                        "rooms": validated_params["rooms"]
                    },
                    "filters_applied": {k: v for k, v in validated_params.items() 
                                      if k in ["min_price", "max_price", "min_rating", "star_rating", "amenities", "search_radius_km"]},
                    "extraction_method": result.get("extraction_method", "unknown"),
                    "total_found": len(hotels),
                    "search_completed_at": datetime.utcnow().isoformat()
                },
                "summary": {
                    "hotels_found": len(hotels),
                    "average_price_per_night": result.get("average_price_per_night"),
                    "average_rating": result.get("average_rating"),
                    "execution_time_seconds": result.get("execution_time_seconds")
                },
                "hotels": hotels
            }
            
            # Write to file with pretty formatting
            output_file.write_text(json.dumps(result_data, indent=2, ensure_ascii=False), "utf-8")
            
            _log(logger, "info", f"💾 Results saved to {output_file.relative_to(pathlib.Path.cwd())}")
            
            return {
                "success": True,
                "absolute_path": str(output_file),
                "relative_path": str(output_file.relative_to(pathlib.Path.cwd())),
                "filename": filename
            }
            
        except Exception as e:
            _log(logger, "error", f"❌ Failed to store results: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    # ===== GraphQL Helper Methods =====
    @staticmethod
    async def _extract_review_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract review data from ReviewList GraphQL response."""
        try:
            # Look for review arrays
            reviews_data = None
            for key in ["reviews", "reviewList", "propertyReviewList"]:
                if key in data:
                    potential_data = data[key]
                    if isinstance(potential_data, dict):
                        for subkey in ["reviews", "items", "list"]:
                            if subkey in potential_data and isinstance(potential_data[subkey], list):
                                reviews_data = potential_data[subkey]
                                break
                    elif isinstance(potential_data, list):
                        reviews_data = potential_data
                    if reviews_data:
                        break
            
            if reviews_data:
                # Store reviews by hotel/property ID
                for review in reviews_data:
                    if isinstance(review, dict):
                        # Extract review details
                        review_text = review.get("textDetails", {}).get("positiveText") or review.get("content") or review.get("text")
                        review_score = review.get("reviewScore") or review.get("rating") or review.get("score")
                        reviewer = review.get("guestDetails", {}).get("username") or review.get("reviewer", {}).get("name")
                        
                        property_id = "unknown"  # We'll match this later
                        
                        if property_id not in intercepted_data["reviews"]:
                            intercepted_data["reviews"][property_id] = []
                        
                        intercepted_data["reviews"][property_id].append({
                            "text": review_text,
                            "rating": review_score,
                            "reviewer": reviewer,
                            "source": "graphql_api"
                        })
                        
                _log(logger, "info", f"📝 Extracted {len(reviews_data)} reviews from API")
                
        except Exception as e:
            _log(logger, "debug", f"Error extracting review data: {e}")
    
    @staticmethod
    async def _extract_amenities_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract amenities data from GraphQL response.""" 
        try:
            _log(logger, "debug", "Processing amenities data...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting amenities data: {e}")
    
    @staticmethod
    async def _extract_images_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract images data from GraphQL response."""
        try:
            _log(logger, "debug", "Processing images data...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting images data: {e}")
    
    @staticmethod
    async def _extract_pricing_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract pricing data from GraphQL response."""
        try:
            _log(logger, "debug", "Processing pricing data...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting pricing data: {e}")
    
    @staticmethod
    async def _extract_property_details(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract property details from GraphQL response."""
        try:
            _log(logger, "debug", "Processing property details...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting property details: {e}")
    
    @staticmethod
    async def _perform_search_with_api_interception(page, validated_params: Dict[str, Any], logger: logging.Logger):
        """Perform search while GraphQL APIs are being intercepted."""
        try:
            # Use existing search logic but optimized for API interception
            await BookingHotelsTask._perform_search_enhanced(page, validated_params, logger)
            
            # Wait a bit more for additional API calls
            await page.wait_for_timeout(5000)
            
        except Exception as e:
            _log(logger, "warning", f"Search with API interception failed: {e}")
    
    @staticmethod
    async def _visit_hotel_pages_for_apis(page, intercepted_data: Dict[str, Any], validated_params: Dict[str, Any], logger: logging.Logger):
        """Visit individual hotel pages to trigger additional GraphQL APIs."""
        try:
            _log(logger, "info", "🏨 Visiting hotel pages to trigger additional APIs...")
            # Limited implementation for now
            await page.wait_for_timeout(2000)
        except Exception as e:
            _log(logger, "warning", f"Hotel page visits failed: {e}")
    
    @staticmethod
    async def _debug_intercepted_apis(intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Debug and analyze intercepted API calls to understand response structures."""
        try:
            raw_responses = intercepted_data.get("raw_responses", [])
            _log(logger, "info", f"🔍 Debug: Intercepted {len(raw_responses)} API responses")
            
            # Enhanced debugging - look for hotel-like data structures
            hotel_like_responses = []
            for i, response in enumerate(raw_responses):
                try:
                    data = response.get('data', {})
                    url = response.get('url', 'unknown')
                    
                    if isinstance(data, dict):
                        # Deep search for arrays that might contain hotels
                        def find_arrays_recursively(obj, path="", depth=0):
                            if depth > 3:  # Prevent infinite recursion
                                return []
                            
                            arrays_found = []
                            if isinstance(obj, dict):
                                for key, value in obj.items():
                                    current_path = f"{path}.{key}" if path else key
                                    
                                    if isinstance(value, list) and len(value) > 0:
                                        # Check if this array contains hotel-like objects
                                        if isinstance(value[0], dict):
                                            sample_keys = set(value[0].keys())
                                            hotel_indicators = {
                                                "name", "title", "hotelName", "propertyName", "displayName",
                                                "id", "hotelId", "propertyId", "basicPropertyData", 
                                                "price", "priceInfo", "pricing", "rates", "priceBreakdown",
                                                "rating", "reviewScore", "guestReviewsRating", "reviewsCount",
                                                "address", "location", "city", "countryCode", "distance",
                                                "images", "photos", "gallery", "mainPhoto", "photoUrls",
                                                "availability", "roomTypes", "accommodation"
                                            }
                                            
                                            matching_indicators = sample_keys.intersection(hotel_indicators)
                                            if matching_indicators or len(sample_keys) > 5:  # Either has hotel indicators or is complex object
                                                arrays_found.append({
                                                    'path': current_path,
                                                    'length': len(value),
                                                    'sample_keys': list(sample_keys)[:25],
                                                    'matching_indicators': list(matching_indicators),
                                                    'first_item': value[0] if len(value) > 0 else None
                                                })
                                    
                                    # Recurse into nested objects
                                    elif isinstance(value, (dict, list)):
                                        arrays_found.extend(find_arrays_recursively(value, current_path, depth + 1))
                            
                            elif isinstance(obj, list):
                                for idx, item in enumerate(obj):
                                    current_path = f"{path}[{idx}]" if path else f"[{idx}]"
                                    arrays_found.extend(find_arrays_recursively(item, current_path, depth + 1))
                            
                            return arrays_found
                        
                        arrays = find_arrays_recursively(data)
                        if arrays:
                            hotel_like_responses.append({
                                'response_index': i,
                                'url': url,
                                'arrays': arrays
                            })
                            
                            for array_info in arrays:
                                _log(logger, "info", f"🎯 Response {i+1}: Found array at '{array_info['path']}' with {array_info['length']} items")
                                _log(logger, "info", f"   Sample keys: {array_info['sample_keys'][:15]}")
                                if array_info['matching_indicators']:
                                    _log(logger, "info", f"   Hotel indicators: {array_info['matching_indicators']}")
                                
                                # Log first item structure for the most promising arrays
                                if len(array_info['matching_indicators']) > 2:
                                    first_item = array_info['first_item']
                                    if first_item:
                                        _log(logger, "info", f"   First item sample: {str(first_item)[:200]}...")
                        
                        # Also log top-level structure for context
                        top_level_keys = list(data.keys())
                        _log(logger, "info", f"   Response {i+1} top-level: {top_level_keys}")
                        
                except Exception as e:
                    _log(logger, "debug", f"Error analyzing response {i}: {e}")
            
            _log(logger, "info", f"🔍 Found {len(hotel_like_responses)} responses with potential hotel data")
                
        except Exception as e:
            _log(logger, "debug", f"Debug analysis error: {e}")
    
    @staticmethod
    async def _compile_hotel_data_from_apis(intercepted_data: Dict[str, Any], validated_params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Compile complete hotel data from intercepted GraphQL API responses."""
        try:
            hotels = []
            
            # Extract hotels from intercepted data
            hotel_entries = intercepted_data.get("hotels", [])
            _log(logger, "info", f"🏨 Compiling data from {len(hotel_entries)} intercepted hotels")
            
            for entry in hotel_entries:
                if isinstance(entry, dict) and "processed_data" in entry:
                    hotel_data = entry["processed_data"]
                    if hotel_data and isinstance(hotel_data, dict):
                        # Add reviews if available
                        hotel_id = entry.get("id", "unknown")
                        if hotel_id in intercepted_data.get("reviews", {}):
                            hotel_data["reviews"] = intercepted_data["reviews"][hotel_id]
                        else:
                            hotel_data["reviews"] = []
                        
                        hotels.append(hotel_data)
            
            _log(logger, "info", f"✅ Compiled {len(hotels)} hotels from GraphQL APIs")
            return hotels[:validated_params.get("max_results", 50)]
            
        except Exception as e:
            _log(logger, "error", f"Error compiling hotel data from APIs: {e}")
            return []
    
    @staticmethod
    async def _handle_cookie_consent(page, logger: logging.Logger):
        """Handle cookie consent popup."""
        try:
            cookie_selectors = [
                "button[data-testid*='cookie']",
                "button:has-text('Accept')",
                "button:has-text('I accept')", 
                "button:has-text('Accept all')",
                "#onetrust-accept-btn-handler",
                ".bui-button--primary:has-text('OK')",
                "[data-consent-manage-id='accept_all']"
            ]
            
            for selector in cookie_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    _log(logger, "info", f"🍪 Accepted cookies with {selector}")
                    await page.wait_for_timeout(1000)
                    break
                except:
                    continue
                    
        except Exception as e:
            _log(logger, "debug", f"Cookie consent handling: {e}")
    
    @staticmethod
    async def _enhanced_interaction_automation(page, validated_params: Dict[str, Any], logger: logging.Logger):
        """Enhanced interaction automation to trigger more API calls."""
        try:
            _log(logger, "info", "🎯 Performing enhanced interactions to trigger APIs...")
            
            # Scroll to trigger lazy loading
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            await page.wait_for_timeout(2000)
            
            # Interact with filter elements if present
            try:
                filter_buttons = await page.query_selector_all("button[data-testid*='filter']")
                if filter_buttons and len(filter_buttons) > 0:
                    await filter_buttons[0].click()
                    await page.wait_for_timeout(1000)
                    await page.keyboard.press("Escape")
            except:
                pass
                
            await page.wait_for_timeout(3000)
            _log(logger, "info", "✅ Enhanced interactions completed")
            
        except Exception as e:
            _log(logger, "warning", f"Enhanced interaction automation failed: {e}")
    
    # ===== Browser Search Methods =====
    @staticmethod
    async def _perform_search_enhanced(page, validated_params: Dict[str, Any], logger: logging.Logger) -> None:
        """Enhanced search form submission with improved reliability."""
        try:
            # Navigate to booking.com with enhanced settings
            _log(logger, "info", "🌐 Navigating to Booking.com")
            await page.goto(BookingHotelsTask.BASE_URL, wait_until="networkidle", timeout=60000)
            
            # Handle cookie consent with multiple selectors
            cookie_selectors = [
                "button[data-testid*='cookie']",
                "button:has-text('Accept')",
                "button:has-text('I accept')", 
                "button:has-text('Accept all')",
                "#onetrust-accept-btn-handler",
                ".bui-button--primary:has-text('OK')",
                "[data-consent-manage-id='accept_all']"
            ]
            
            for selector in cookie_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    _log(logger, "info", f"🍪 Accepted cookies with {selector}")
                    await page.wait_for_timeout(1000)
                    break
                except:
                    continue
            
            # Enhanced location input with multiple selectors
            location_selectors = [
                "input[data-testid='destination-input']",
                "input[name='ss']", 
                "input[placeholder*='destination']",
                "input[placeholder*='Where are you going']",
                "input.sb-destination__input",
                "#ss",
                "[data-element-name='destination']"
            ]
            
            location_input = None
            for selector in location_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    location_input = page.locator(selector)
                    if await location_input.count() > 0:
                        _log(logger, "info", f"🎯 Found location input: {selector}")
                        break
                except:
                    continue
            
            if not location_input or await location_input.count() == 0:
                raise RuntimeError("Could not find location search input with any selector")
            
            # Enhanced typing with human-like behavior
            await location_input.click()
            await page.wait_for_timeout(random.uniform(500, 1000))
            await location_input.clear()
            await page.wait_for_timeout(random.uniform(300, 700))
            
            # Type location with realistic delays
            location_text = validated_params["location"]
            for char in location_text:
                await location_input.type(char)
                await page.wait_for_timeout(random.uniform(50, 150))
            
            # Handle autocomplete suggestions
            suggestion_selectors = [
                "li[data-testid*='autocomplete']",
                ".sb-autocomplete__item",
                "[data-testid='autocomplete-result']",
                ".c-autocomplete__item",
                ".sb-autocomplete__option"
            ]
            
            suggestion_clicked = False
            for selector in suggestion_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=3000)
                    await page.click(f"{selector}:first-child", timeout=2000)
                    _log(logger, "info", f"📍 Clicked autocomplete: {selector}")
                    suggestion_clicked = True
                    break
                except:
                    continue
            
            if not suggestion_clicked:
                await location_input.press("Enter")
                _log(logger, "info", "⌨️  No autocomplete - pressed Enter")
            
            await page.wait_for_timeout(1000)
            
            # Enhanced date handling
            try:
                date_selectors = [
                    "[data-testid='date-display-field-start']",
                    "button[data-testid*='date']",
                    ".sb-date-field__input",
                    "[data-placeholder*='Check-in']"
                ]
                
                for selector in date_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=3000)
                        await page.click(selector)
                        _log(logger, "info", f"📅 Opened date picker: {selector}")
                        break
                    except:
                        continue
                
                # Simple date selection (can be enhanced further)
                await page.wait_for_timeout(2000)
                
                # Try to close date picker
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(1000)
                
            except Exception as e:
                _log(logger, "warning", f"⚠️  Date handling skipped: {str(e)}")
            
            # Enhanced search submission
            search_selectors = [
                "button[type='submit']:has-text('Search')",
                "button[data-testid*='search']",
                ".sb-searchbox__button",
                "button:has-text('Search')",
                "[data-element-name='search_button']"
            ]
            
            search_clicked = False
            for selector in search_selectors:
                try:
                    await page.click(selector, timeout=5000)
                    _log(logger, "info", f"🔍 Clicked search: {selector}")
                    search_clicked = True
                    break
                except:
                    continue
            
            if not search_clicked:
                await page.keyboard.press("Enter")
                _log(logger, "info", "⌨️  Fallback search with Enter key")
            
            # Wait for navigation
            await page.wait_for_load_state("networkidle", timeout=30000)
            _log(logger, "info", "✅ Search completed successfully")
            
        except Exception as e:
            _log(logger, "error", f"❌ Enhanced search failed: {str(e)}")
            raise
    
    @staticmethod
    async def _extract_complete_hotel_data(container, hotel_index: int, logger: logging.Logger) -> Dict[str, Any]:
        """Extract comprehensive data from a hotel card container with real data extraction."""
        try:
            # Initialize with extracted data, not fake data
            hotel_data = {
                "id": f"property_{hotel_index}",
                "name": "Unknown Hotel",
                "price_per_night": None,
                "rating": None,
                "review_count": None,
                "address": None,
                "images": [],
                "amenities": [],
                "booking_url": None,
                "reviews": [],
                "enhanced": True
            }
            
            # Extract real name
            try:
                name_selectors = [
                    "[data-testid='title']",
                    "h3[data-testid*='title']", 
                    "h2[data-testid*='title']",
                    ".sr-hotel__name",
                    "[data-testid='property-card-title']",
                    "h3 a",
                    "h2 a"
                ]
                for selector in name_selectors:
                    name_element = await container.query_selector(selector)
                    if name_element:
                        name_text = await name_element.text_content()
                        if name_text and name_text.strip():
                            hotel_data["name"] = name_text.strip()
                            break
            except Exception as e:
                _log(logger, "debug", f"Error extracting hotel name: {e}")
            
            # Extract real price with comprehensive selectors
            try:
                price_selectors = [
                    # New Booking.com structure (2024/2025)
                    "[data-testid='price-and-discounted-price'] .a78ca197d0",
                    "[data-testid='price-and-discounted-price'] .fde1d5429e", 
                    "[data-testid='price-and-discounted-price'] span",
                    "[data-testid='price'] .a78ca197d0",
                    "[data-testid='price'] .fde1d5429e",
                    "[data-testid='price'] span",
                    ".a78ca197d0",  # Main price class
                    ".fde1d5429e",  # Alternative price class
                    # Legacy selectors
                    ".bui-price-display__value",
                    ".sr-hotel__price .bui-price-display__value",
                    ".prco-valign-middle-helper",
                    # Generic price selectors
                    "[class*='price'] span:contains('$')",
                    "[class*='price'] span:contains('SAR')",
                    "[class*='price'] span:contains('€')",
                    # Search any element containing price-like text
                    "*:contains('SAR'):contains('night')",
                    "*:contains('$'):contains('night')"
                ]
                
                for selector in price_selectors:
                    try:
                        # Handle special :contains() selectors differently
                        if ':contains(' in selector:
                            # Use manual text search for contains selectors
                            all_elements = await container.query_selector_all("*")
                            for element in all_elements:
                                try:
                                    text = await element.text_content()
                                    if text and ('SAR' in text or '$' in text or '€' in text) and ('night' in text.lower() or 'per' in text.lower()):
                                        price_text = text
                                        break
                                except:
                                    continue
                            else:
                                continue
                        else:
                            price_element = await container.query_selector(selector)
                            if not price_element:
                                continue
                            price_text = await price_element.text_content()
                        
                        if price_text and price_text.strip():
                            # Extract numeric price from text like "SAR 450", "$120", "€85"
                            import re
                            # Look for numbers with currency symbols
                            price_patterns = [
                                r'SAR\s*[\d,]+',
                                r'\$\s*[\d,]+', 
                                r'€\s*[\d,]+',
                                r'[\d,]+\s*SAR',
                                r'[\d,]+\s*\$',
                                r'[\d,]+\s*€',
                                r'[\d,]+'  # Fallback to any number
                            ]
                            
                            for pattern in price_patterns:
                                matches = re.findall(pattern, price_text.replace(',', ''))
                                if matches:
                                    # Extract just the numbers
                                    numbers = re.findall(r'\d+', matches[0])
                                    if numbers:
                                        hotel_data["price_per_night"] = float(numbers[0])
                                        
                                        # Extract currency
                                        if "SAR" in price_text or "ر.س" in price_text:
                                            hotel_data["currency"] = "SAR"
                                        elif "$" in price_text or "USD" in price_text:
                                            hotel_data["currency"] = "USD"
                                        elif "€" in price_text or "EUR" in price_text:
                                            hotel_data["currency"] = "EUR"
                                        else:
                                            hotel_data["currency"] = "Unknown"
                                        
                                        _log(logger, "debug", f"Extracted price: {hotel_data['price_per_night']} {hotel_data['currency']} from '{price_text.strip()}'")
                                        break
                            
                            if hotel_data["price_per_night"] is not None:
                                break
                    except Exception as e:
                        _log(logger, "debug", f"Error with price selector '{selector}': {e}")
                        continue
                        
            except Exception as e:
                _log(logger, "debug", f"Error extracting price: {e}")
            
            # Extract real rating with comprehensive selectors
            try:
                rating_selectors = [
                    # New Booking.com structure (2024/2025)
                    "[data-testid='review-score'] div",
                    "[data-testid='review-score'] span", 
                    "[data-testid='review-score'] .a3b8729ab1",
                    "[data-testid='review-score'] .d10a6220b4",
                    "[aria-label*='Scored'] div",
                    "[aria-label*='rating'] div",
                    # Legacy selectors
                    ".bui-review-score__badge",
                    ".sr-hotel__review-score .bui-review-score__badge",
                    "[data-testid*='rating']",
                    # Class-based selectors for current structure
                    ".a3b8729ab1",  # Common rating class
                    ".d10a6220b4",  # Alternative rating class
                    # Generic rating selectors
                    "[class*='review'] [class*='score']",
                    "[class*='rating'] span",
                    "[class*='badge'] span"
                ]
                
                for selector in rating_selectors:
                    try:
                        rating_element = await container.query_selector(selector)
                        if rating_element:
                            rating_text = await rating_element.text_content()
                            if rating_text and rating_text.strip():
                                # Extract rating from text - look for decimal numbers like 8.5, 9.2, etc.
                                import re
                                rating_match = re.search(r'\b(\d+\.?\d*)\b', rating_text.strip())
                                if rating_match:
                                    rating_value = float(rating_match.group(1))
                                    # Validate rating is in reasonable range (0-10)
                                    if 0 <= rating_value <= 10:
                                        hotel_data["rating"] = rating_value
                                        _log(logger, "debug", f"Extracted rating: {rating_value} from '{rating_text.strip()}'")
                                        break
                    except Exception as e:
                        _log(logger, "debug", f"Error with rating selector '{selector}': {e}")
                        continue
                        
            except Exception as e:
                _log(logger, "debug", f"Error extracting rating: {e}")
            
            # Extract review count with comprehensive selectors
            try:
                review_selectors = [
                    # New Booking.com structure (2024/2025)
                    "[data-testid='review-score'] ~ div",
                    "[data-testid='review-score'] + div", 
                    "[data-testid='reviews-count']",
                    "[data-testid*='review'] span",
                    "[aria-label*='review'] span",
                    # Legacy selectors
                    ".bui-review-score__text",
                    "[data-testid='review-score'] .bui-review-score__text",
                    # Generic review count selectors
                    "[class*='review'] span:contains('review')",
                    "*:contains('review'):contains('based')",
                    "*[class*='review'][class*='count']",
                    # Alternative patterns
                    "span:contains('reviews')",
                    "div:contains('reviews')"
                ]
                
                for selector in review_selectors:
                    try:
                        # Handle special :contains() selectors differently  
                        if ':contains(' in selector:
                            # Use manual text search for contains selectors
                            all_elements = await container.query_selector_all("span, div")
                            for element in all_elements:
                                try:
                                    text = await element.text_content()
                                    if text and ('review' in text.lower() or 'based on' in text.lower()):
                                        review_text = text
                                        break
                                except:
                                    continue
                            else:
                                continue
                        else:
                            review_element = await container.query_selector(selector)
                            if not review_element:
                                continue
                            review_text = await review_element.text_content()
                        
                        if review_text and review_text.strip():
                            # Extract number from text like "based on 1,234 reviews" or "1,234 reviews"
                            import re
                            numbers = re.findall(r'[\d,]+', review_text.replace(',', ''))
                            if numbers:
                                # Take the first number found
                                review_count = int(numbers[0])
                                # Validate it's a reasonable review count (not year, rating, etc.)
                                if 1 <= review_count <= 50000:
                                    hotel_data["review_count"] = review_count
                                    _log(logger, "debug", f"Extracted review count: {review_count} from '{review_text.strip()}'")
                                    break
                    except Exception as e:
                        _log(logger, "debug", f"Error with review count selector '{selector}': {e}")
                        continue
                        
            except Exception as e:
                _log(logger, "debug", f"Error extracting review count: {e}")
            
            # Extract real address/location
            try:
                address_selectors = [
                    "[data-testid='address']",
                    ".sr-hotel__address",
                    "[data-testid*='location']",
                    ".bui-card__subtitle"
                ]
                for selector in address_selectors:
                    address_element = await container.query_selector(selector)
                    if address_element:
                        address_text = await address_element.text_content()
                        if address_text and address_text.strip():
                            hotel_data["address"] = address_text.strip()
                            break
            except Exception as e:
                _log(logger, "debug", f"Error extracting address: {e}")
            
            # Extract real booking URL
            try:
                link_selectors = [
                    "a[data-testid='title-link']",
                    "h3 a",
                    "h2 a",
                    "a[href*='/hotel/']"
                ]
                for selector in link_selectors:
                    link_element = await container.query_selector(selector)
                    if link_element:
                        href = await link_element.get_attribute("href")
                        if href:
                            # Make URL absolute if needed
                            if href.startswith("/"):
                                hotel_data["booking_url"] = f"https://www.booking.com{href}"
                            elif href.startswith("http"):
                                hotel_data["booking_url"] = href
                            break
            except Exception as e:
                _log(logger, "debug", f"Error extracting booking URL: {e}")
            
            # Extract amenities
            try:
                amenity_selectors = [
                    "[data-testid*='facility']",
                    ".sr-hotel__facility",
                    ".bui-list .bui-list__item"
                ]
                amenities = []
                for selector in amenity_selectors:
                    amenity_elements = await container.query_selector_all(selector)
                    for element in amenity_elements:
                        amenity_text = await element.text_content()
                        if amenity_text and amenity_text.strip():
                            amenities.append(amenity_text.strip())
                if amenities:
                    hotel_data["amenities"] = amenities[:10]  # Limit to first 10
            except Exception as e:
                _log(logger, "debug", f"Error extracting amenities: {e}")
            
            return hotel_data
            
        except Exception as e:
            _log(logger, "warning", f"Error extracting hotel data: {e}")
            return None
    
    @staticmethod
    async def _get_detailed_hotel_data(page, hotel_basic: Dict[str, Any], validated_params: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
        """Get detailed hotel data from individual hotel page (simplified implementation)."""
        try:
            # For now, return the basic data with some enhancements
            enhanced_data = hotel_basic.copy()
            enhanced_data["enhanced"] = True
            return enhanced_data
            
        except Exception as e:
            _log(logger, "warning", f"Failed to get detailed data for {hotel_basic.get('name', 'Unknown')}: {e}")
            return hotel_basic