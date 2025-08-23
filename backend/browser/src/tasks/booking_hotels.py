"""
Enhanced Booking.com Hotel Scraper
===================================

A comprehensive scraper that extracts complete hotel data including:
- Correct hotel URLs
- Full reviews with photos
- Complete image galleries  
- FAQs and amenities
- Location/surroundings data
- Room details

Version: 3.0 (Complete Enhancement)
"""

import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, quote


class BookingHotelsTask:
    """Enhanced Booking.com hotel scraper with complete data extraction."""
    
    BASE_URL = "https://www.booking.com"
    
    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point with enhanced data extraction."""
        try:
            # Validate parameters
            clean_params = BookingHotelsTask._validate_params(params)
            
            # Check if deep scraping is requested
            deep_scrape = params.get("deep_scrape", False) or params.get("deep_scrape_enabled", False)
            
            if deep_scrape:
                logger.info("🔥 DEEP SCRAPING ENABLED - Extracting comprehensive data with reviews, FAQs, and surroundings")
                scraper = EnhancedScraperEngine(browser, logger)
                hotels = await scraper.scrape_hotels_with_full_details(clean_params)
                extraction_method = "deep_scraping_enhanced"
            else:
                logger.info("🚀 Using QUICK SEARCH for basic hotel data")
                scraper = EnhancedScraperEngine(browser, logger)
                hotels = await scraper.scrape_hotels_basic(clean_params)
                extraction_method = "weekend_deals_basic"
            
            # Calculate metrics
            success_rate = len([h for h in hotels if h.get('price_per_night')]) / len(hotels) if hotels else 0
            avg_price = sum(h.get('price_per_night', 0) for h in hotels if h.get('price_per_night')) / len([h for h in hotels if h.get('price_per_night')]) if hotels else 0
            
            logger.info(f"🏁 Scraping completed: {len(hotels)} hotels with {success_rate:.1%} price data")
            
            result = {
                "search_metadata": {
                    "location": clean_params["location"],
                    "check_in": clean_params["check_in"],
                    "check_out": clean_params["check_out"],
                    "extraction_method": extraction_method,
                    "deep_scrape_enabled": deep_scrape,
                    "total_found": len(hotels),
                    "success_rate": success_rate,
                    "average_price": avg_price,
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
                logger.info(f"💾 Saved comprehensive hotel data to {output_file}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Scraper failed: {e}")
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
        
        # Default dates if not provided
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
            "rooms": max(1, int(params.get("rooms", 1))),
            "max_results": max(1, int(params.get("max_results", 10)))
        }


class EnhancedScraperEngine:
    """Enhanced scraper engine with complete data extraction capabilities."""
    
    BASE_URL = "https://www.booking.com"
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        self.intercepted_data = {
            "weekend_deals": [],
            "search_results": [],
            "graphql_responses": []
        }
    
    async def scrape_hotels_basic(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Basic scraping - weekend deals only."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Set up interception
            await self._setup_interception(page)
            
            # Perform search
            await page.goto(self.BASE_URL, wait_until="networkidle")
            await self._handle_popups(page)
            await self._perform_search(page, params)
            
            # Wait for responses
            await page.wait_for_timeout(10000)
            
            # Parse weekend deals
            hotels = await self._parse_weekend_deals()
            
            # Extract proper URLs from search results page
            hotels = await self._fix_hotel_urls(page, hotels)
            
            self.logger.info(f"✅ Extracted {len(hotels)} hotels with basic data")
            return hotels
            
        finally:
            await context.close()
    
    async def scrape_hotels_with_full_details(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Deep scraping with reviews, FAQs, surroundings, and complete data."""
        # First get basic hotels
        basic_hotels = await self.scrape_hotels_basic(params)
        
        # Limit to max_results
        max_results = params.get("max_results", 10)
        hotels_to_process = basic_hotels[:max_results]
        
        # Now enhance each hotel with complete data
        enhanced_hotels = []
        
        for i, hotel in enumerate(hotels_to_process, 1):
            try:
                self.logger.info(f"🏨 Deep scraping hotel {i}/{len(hotels_to_process)}: {hotel.get('name')}")
                
                # Extract comprehensive data
                enhanced_hotel = await self._extract_complete_hotel_data(hotel, params)
                enhanced_hotels.append(enhanced_hotel)
                
                # Respectful delay
                if i < len(hotels_to_process):
                    await asyncio.sleep(2)
                    
            except Exception as e:
                self.logger.error(f"❌ Deep scraping failed for {hotel.get('name')}: {e}")
                hotel['error'] = str(e)
                enhanced_hotels.append(hotel)
        
        self.logger.info(f"🎯 Deep scraping completed: {len(enhanced_hotels)} hotels with comprehensive data")
        return enhanced_hotels
    
    async def _extract_complete_hotel_data(self, hotel: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Extract complete data for a single hotel."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Start with basic hotel data
            enhanced_hotel = hotel.copy()
            
            # Navigate to hotel page
            hotel_url = hotel.get('booking_url')
            if not hotel_url:
                self.logger.warning(f"⚠️ No URL for hotel: {hotel.get('name')}")
                return enhanced_hotel
            
            self.logger.info(f"🌐 Navigating to: {hotel_url}")
            
            # Set up GraphQL interception for this page
            graphql_data = {"reviews": None, "faq": None, "surroundings": None}
            
            async def handle_response(response):
                if "/dml/graphql" in response.url:
                    try:
                        data = await response.json()
                        # Capture different GraphQL responses
                        if "reviewListFrontend" in str(data):
                            graphql_data["reviews"] = data
                        elif "propertyFaq" in str(data):
                            graphql_data["faq"] = data
                        elif "propertySurroundings" in str(data):
                            graphql_data["surroundings"] = data
                    except:
                        pass
            
            page.on("response", handle_response)
            
            # Navigate to hotel page
            await page.goto(hotel_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
            
            # Extract hotel ID from URL for GraphQL queries
            hotel_id = self._extract_hotel_id(page.url, hotel)
            if hotel_id:
                enhanced_hotel['hotel_id'] = hotel_id
            
            # 1. Extract Images
            images = await self._extract_all_images(page)
            enhanced_hotel['images'] = images
            enhanced_hotel['image_count'] = len(images)
            
            # 2. Extract Basic Info from Page
            basic_info = await self._extract_basic_info(page)
            enhanced_hotel.update(basic_info)
            
            # 3. Execute GraphQL queries for detailed data
            if hotel_id:
                # Get Reviews
                reviews_data = await self._fetch_reviews(page, hotel_id)
                if reviews_data:
                    enhanced_hotel['reviews'] = reviews_data['reviews']
                    enhanced_hotel['review_count'] = reviews_data['total_count']
                    enhanced_hotel['rating_breakdown'] = reviews_data['rating_breakdown']
                
                # Get FAQs
                faq_data = await self._fetch_faqs(page, hotel_url)
                if faq_data:
                    enhanced_hotel['faqs'] = faq_data
                
                # Get Surroundings
                surroundings_data = await self._fetch_surroundings(page, hotel_id)
                if surroundings_data:
                    enhanced_hotel['surroundings'] = surroundings_data
            
            # 4. Extract Amenities
            amenities = await self._extract_amenities(page)
            enhanced_hotel['amenities'] = amenities
            
            # 5. Extract Room Information
            rooms = await self._extract_rooms(page)
            enhanced_hotel['rooms'] = rooms
            
            # Mark as fully scraped
            enhanced_hotel['data_completeness'] = self._calculate_completeness(enhanced_hotel)
            enhanced_hotel['scraping_timestamp'] = datetime.now().isoformat()
            
            self.logger.info(f"✅ Extracted comprehensive data for {enhanced_hotel.get('name')}: "
                           f"{len(images)} images, {len(enhanced_hotel.get('reviews', []))} reviews, "
                           f"{len(enhanced_hotel.get('faqs', []))} FAQs")
            
            return enhanced_hotel
            
        except Exception as e:
            self.logger.error(f"❌ Error extracting complete data: {e}")
            hotel['extraction_error'] = str(e)
            return hotel
        finally:
            await context.close()
    
    async def _setup_interception(self, page):
        """Set up request/response interception."""
        async def handle_response(response):
            if "/dml/graphql" in response.url and response.status == 200:
                try:
                    data = await response.json()
                    
                    # Capture weekend deals
                    if "weekendDeals" in str(data):
                        self.intercepted_data["weekend_deals"].append(data)
                        deals = self._extract_weekend_deals_count(data)
                        if deals > 0:
                            self.logger.info(f"✅ Captured weekend deals response with {deals} hotels")
                    
                    # Store all GraphQL responses for analysis
                    self.intercepted_data["graphql_responses"].append(data)
                    
                except Exception as e:
                    self.logger.debug(f"Response parsing error: {e}")
        
        page.on("response", handle_response)
    
    async def _parse_weekend_deals(self) -> List[Dict[str, Any]]:
        """Parse weekend deals responses."""
        hotels = []
        
        for response_data in self.intercepted_data["weekend_deals"]:
            try:
                # Extract weekend deals
                deals = self._get_nested_value(response_data, ["data", "weekendDeals", "weekendDealsProperties"])
                if deals and isinstance(deals, list):
                    for item in deals:
                        hotel = self._parse_hotel_item(item)
                        if hotel:
                            hotels.append(hotel)
                            self.logger.info(f"✅ Parsed: {hotel['name']} - ${hotel.get('price_per_night', 'N/A')} SAR")
            except Exception as e:
                self.logger.debug(f"Weekend deals parsing error: {e}")
        
        return hotels
    
    async def _fix_hotel_urls(self, page, hotels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract correct hotel URLs from search results page."""
        try:
            # Get all hotel links from the page
            hotel_links = await page.query_selector_all("a[href*='/hotel/']")
            url_map = {}
            
            for link in hotel_links:
                try:
                    href = await link.get_attribute("href")
                    if href and "/hotel/sa/" in href:
                        # Extract hotel ID or name from URL
                        match = re.search(r'/hotel/sa/([^\.]+)\.', href)
                        if match:
                            hotel_identifier = match.group(1)
                            # Clean URL
                            clean_url = href.split('?')[0] if '?' in href else href
                            if not clean_url.startswith("http"):
                                clean_url = f"https://www.booking.com{clean_url}"
                            
                            # Try to match with hotel name from title
                            title_element = await link.query_selector("[data-testid='title']")
                            if title_element:
                                title = await title_element.text_content()
                                if title:
                                    url_map[title.strip()] = clean_url
                            
                            # Also store by potential ID
                            url_map[hotel_identifier] = clean_url
                except Exception:
                    continue
            
            # Update hotels with correct URLs
            for hotel in hotels:
                # Try to find URL by name match
                hotel_name = hotel.get('name', '')
                if hotel_name in url_map:
                    hotel['booking_url'] = url_map[hotel_name]
                # Try by ID
                elif hotel.get('id') in url_map:
                    hotel['booking_url'] = url_map[hotel.get('id')]
                # Try partial name match
                else:
                    for name, url in url_map.items():
                        if hotel_name and (hotel_name in name or name in hotel_name):
                            hotel['booking_url'] = url
                            break
            
        except Exception as e:
            self.logger.debug(f"URL extraction error: {e}")
        
        return hotels
    
    async def _extract_all_images(self, page) -> List[str]:
        """Extract all hotel images including gallery."""
        images = []
        
        try:
            # Try to open photo gallery
            gallery_selectors = [
                "button[data-testid*='photo']",
                "a[data-testid*='photo']",
                "[data-testid='property-gallery-opener']",
                "button:has-text('photos')",
                ".photo_carousel_track"
            ]
            
            for selector in gallery_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    await page.wait_for_timeout(2000)
                    self.logger.debug(f"✅ Opened photo gallery")
                    break
                except:
                    continue
            
            # Extract all images
            img_selectors = [
                "img[data-testid*='image']",
                ".bh-photo-grid img",
                ".slick-slide img",
                "[data-testid='property-gallery'] img",
                "img[src*='bstatic.com']"
            ]
            
            for selector in img_selectors:
                img_elements = await page.query_selector_all(selector)
                for img in img_elements:
                    try:
                        src = await img.get_attribute("src") or await img.get_attribute("data-src")
                        if src and "bstatic" in src:
                            # Clean and format URL
                            if src.startswith("//"):
                                src = "https:" + src
                            if src not in images:
                                images.append(src)
                    except:
                        continue
            
            # Try to extract high-res versions
            high_res_images = []
            for img_url in images[:50]:  # Limit to prevent too many images
                # Convert to high-res version
                high_res = img_url.replace("/square60/", "/max1280x900/")
                high_res = high_res.replace("/square200/", "/max1280x900/")
                high_res = high_res.replace("/square600/", "/max1280x900/")
                if high_res not in high_res_images:
                    high_res_images.append(high_res)
            
            return high_res_images[:30]  # Return max 30 images
            
        except Exception as e:
            self.logger.debug(f"Image extraction error: {e}")
            return images[:10] if images else []
    
    async def _extract_basic_info(self, page) -> Dict[str, Any]:
        """Extract basic hotel information from the page."""
        info = {}
        
        try:
            # Extract description
            desc_selectors = [
                "[data-testid='property-description']",
                ".hp_desc_main_content",
                ".property_description_content"
            ]
            for selector in desc_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        info['description'] = await element.text_content()
                        break
                except:
                    continue
            
            # Extract check-in/check-out times
            try:
                checkin_element = await page.query_selector(".bui-list__description:has-text('Check-in')")
                if checkin_element:
                    info['check_in_time'] = await checkin_element.text_content()
                
                checkout_element = await page.query_selector(".bui-list__description:has-text('Check-out')")
                if checkout_element:
                    info['check_out_time'] = await checkout_element.text_content()
            except:
                pass
            
            # Extract address if not already present
            try:
                address_element = await page.query_selector("[data-testid='address']")
                if address_element:
                    info['full_address'] = await address_element.text_content()
            except:
                pass
            
        except Exception as e:
            self.logger.debug(f"Basic info extraction error: {e}")
        
        return info
    
    async def _fetch_reviews(self, page, hotel_id: str) -> Optional[Dict[str, Any]]:
        """Fetch reviews using GraphQL query."""
        try:
            # Build GraphQL query for reviews
            query = """
            query ReviewList($input: ReviewListFrontendInput!) {
                reviewListFrontend(input: $input) {
                    ... on ReviewListFrontendResult {
                        reviewsCount
                        reviewCard {
                            reviewScore
                            textDetails {
                                title
                                positiveText
                                negativeText
                            }
                            guestDetails {
                                username
                                countryName
                            }
                            reviewedDate
                            photos {
                                urls {
                                    url
                                }
                            }
                        }
                        ratingScores {
                            name
                            value
                        }
                    }
                }
            }
            """
            
            # Execute query
            result = await page.evaluate("""
                async (hotelId) => {
                    try {
                        const response = await fetch('/dml/graphql', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                operationName: 'ReviewList',
                                query: arguments[1],
                                variables: {
                                    input: {
                                        hotelId: parseInt(hotelId),
                                        limit: 25,
                                        skip: 0
                                    }
                                }
                            })
                        });
                        return await response.json();
                    } catch (e) {
                        return null;
                    }
                }
            """, hotel_id, query)
            
            if result and "data" in result:
                review_data = result["data"].get("reviewListFrontend", {})
                
                # Parse reviews
                reviews = []
                for review_card in review_data.get("reviewCard", [])[:20]:  # Limit to 20 reviews
                    review = {
                        "score": review_card.get("reviewScore"),
                        "title": review_card.get("textDetails", {}).get("title"),
                        "positive": review_card.get("textDetails", {}).get("positiveText"),
                        "negative": review_card.get("textDetails", {}).get("negativeText"),
                        "reviewer": review_card.get("guestDetails", {}).get("username"),
                        "country": review_card.get("guestDetails", {}).get("countryName"),
                        "date": review_card.get("reviewedDate"),
                        "photos": [
                            url["url"] for photo in review_card.get("photos", [])
                            for url in photo.get("urls", [])
                        ][:3]  # Limit to 3 photos per review
                    }
                    reviews.append(review)
                
                # Extract rating breakdown
                rating_breakdown = {}
                for score in review_data.get("ratingScores", []):
                    rating_breakdown[score["name"]] = score["value"]
                
                return {
                    "reviews": reviews,
                    "total_count": review_data.get("reviewsCount", 0),
                    "rating_breakdown": rating_breakdown
                }
                
        except Exception as e:
            self.logger.debug(f"Review fetch error: {e}")
        
        return None
    
    async def _fetch_faqs(self, page, hotel_url: str) -> Optional[List[Dict[str, str]]]:
        """Fetch FAQs using GraphQL query."""
        try:
            # Extract path from URL
            path = urlparse(hotel_url).path
            
            # Execute GraphQL query
            result = await page.evaluate("""
                async (path) => {
                    try {
                        const response = await fetch('/dml/graphql', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                operationName: 'PropertyFaq',
                                query: `query PropertyFaq($input: LandingQueriesInput!) {
                                    landingContent(input: $input) {
                                        propertyFaq {
                                            questions {
                                                question
                                                answer
                                            }
                                        }
                                    }
                                }`,
                                variables: {
                                    input: {
                                        originalUri: path
                                    }
                                }
                            })
                        });
                        return await response.json();
                    } catch (e) {
                        return null;
                    }
                }
            """, path)
            
            if result and "data" in result:
                faq_data = result["data"].get("landingContent", {}).get("propertyFaq", {})
                faqs = []
                for q in faq_data.get("questions", []):
                    faqs.append({
                        "question": q.get("question"),
                        "answer": q.get("answer")
                    })
                return faqs
                
        except Exception as e:
            self.logger.debug(f"FAQ fetch error: {e}")
        
        return None
    
    async def _fetch_surroundings(self, page, hotel_id: str) -> Optional[Dict[str, Any]]:
        """Fetch surroundings/location data using GraphQL query."""
        try:
            # Execute GraphQL query for surroundings
            result = await page.evaluate("""
                async (hotelId) => {
                    try {
                        const response = await fetch('/dml/graphql', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                operationName: 'PropertySurroundingsBlockDesktop',
                                query: `query PropertySurroundingsBlockDesktop($input: PropertySurroundingsInput!) {
                                    propertySurroundings(input: $input) {
                                        airports(input: {limit: 3, maxDistanceKm: 100}) {
                                            name
                                            distance
                                            distanceLocalized
                                        }
                                        landmarks {
                                            nearby(input: {limit: 10, maxDistanceKm: 20}) {
                                                name
                                                distance
                                                distanceLocalized
                                            }
                                        }
                                        dining {
                                            restaurants(input: {limit: 5, maxDistanceKm: 50}) {
                                                name
                                                distanceLocalized
                                            }
                                        }
                                    }
                                }`,
                                variables: {
                                    input: {
                                        hotelId: parseInt(hotelId)
                                    }
                                }
                            })
                        });
                        return await response.json();
                    } catch (e) {
                        return null;
                    }
                }
            """, hotel_id)
            
            if result and "data" in result:
                surroundings_data = result["data"].get("propertySurroundings", {})
                
                surroundings = {
                    "airports": [],
                    "landmarks": [],
                    "restaurants": []
                }
                
                # Parse airports
                for airport in surroundings_data.get("airports", []):
                    surroundings["airports"].append({
                        "name": airport.get("name"),
                        "distance": airport.get("distanceLocalized")
                    })
                
                # Parse landmarks
                landmarks_data = surroundings_data.get("landmarks", {})
                for landmark in landmarks_data.get("nearby", [])[:5]:
                    surroundings["landmarks"].append({
                        "name": landmark.get("name"),
                        "distance": landmark.get("distanceLocalized")
                    })
                
                # Parse restaurants
                dining_data = surroundings_data.get("dining", {})
                for restaurant in dining_data.get("restaurants", [])[:5]:
                    surroundings["restaurants"].append({
                        "name": restaurant.get("name"),
                        "distance": restaurant.get("distanceLocalized")
                    })
                
                return surroundings
                
        except Exception as e:
            self.logger.debug(f"Surroundings fetch error: {e}")
        
        return None
    
    async def _extract_amenities(self, page) -> List[str]:
        """Extract hotel amenities."""
        amenities = []
        
        try:
            amenity_selectors = [
                "[data-testid='property-highlights'] li",
                ".hp_desc_important_facilities li",
                ".important_facilities li",
                ".facilitiesChecklist li",
                ".hotel-facilities-group li"
            ]
            
            for selector in amenity_selectors:
                elements = await page.query_selector_all(selector)
                for element in elements:
                    try:
                        text = await element.text_content()
                        if text and text.strip() and text.strip() not in amenities:
                            amenities.append(text.strip())
                    except:
                        continue
                        
                if amenities:
                    break
            
        except Exception as e:
            self.logger.debug(f"Amenities extraction error: {e}")
        
        return amenities[:20]  # Limit to 20 amenities
    
    async def _extract_rooms(self, page) -> List[Dict[str, Any]]:
        """Extract room information."""
        rooms = []
        
        try:
            room_selectors = [
                ".hprt-table tbody tr",
                "[data-testid='room-type']",
                ".room-table tbody tr"
            ]
            
            for selector in room_selectors:
                room_elements = await page.query_selector_all(selector)
                
                for element in room_elements[:10]:  # Limit to 10 rooms
                    try:
                        room = {}
                        
                        # Extract room name
                        name_element = await element.query_selector(".hprt-roomtype-name, .room-name")
                        if name_element:
                            room['name'] = await name_element.text_content()
                        
                        # Extract room price
                        price_element = await element.query_selector(".hprt-price-final, .room-price")
                        if price_element:
                            price_text = await price_element.text_content()
                            if price_text:
                                import re
                                price_match = re.search(r'([\d,]+\.?\d*)', price_text.replace(',', ''))
                                if price_match:
                                    room['price'] = float(price_match.group(1))
                        
                        # Extract occupancy
                        occupancy_element = await element.query_selector(".hprt-roomtype-occupancy")
                        if occupancy_element:
                            room['occupancy'] = await occupancy_element.text_content()
                        
                        if room.get('name'):
                            rooms.append(room)
                            
                    except:
                        continue
                
                if rooms:
                    break
                    
        except Exception as e:
            self.logger.debug(f"Rooms extraction error: {e}")
        
        return rooms
    
    def _extract_hotel_id(self, url: str, hotel: Dict[str, Any]) -> Optional[str]:
        """Extract hotel ID from URL or hotel data."""
        # Try from URL
        match = re.search(r'/hotel/sa/[^/]+\.html', url)
        if match:
            # Try to extract numeric ID from the URL
            id_match = re.search(r'(\d{5,})', url)
            if id_match:
                return id_match.group(1)
        
        # Try from hotel data
        if hotel.get('id'):
            return str(hotel['id'])
        
        return None
    
    def _parse_hotel_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse individual hotel item from weekend deals."""
        try:
            # Extract name
            name = item.get("propertyName", "Unknown Hotel")
            if name == "Unknown Hotel":
                return None
            
            # Extract price
            price = None
            if "price" in item and item["price"]:
                price_data = item["price"]
                if "formattedPrice" in price_data:
                    price_str = price_data["formattedPrice"]
                    import re
                    numbers = re.findall(r'[\d,]+\.?\d*', price_str.replace(',', ''))
                    if numbers:
                        price = float(numbers[0])
                elif "amount" in price_data:
                    price = float(price_data["amount"])
            
            # Extract rating
            rating = None
            if "review" in item and item["review"]:
                rating = item["review"].get("score")
            
            # Extract review count
            review_count = None
            if "review" in item and item["review"]:
                review_count = item["review"].get("reviewCount")
            
            # Extract address
            address = item.get("subtitle", "Saudi Arabia")
            
            # Extract images
            images = []
            if "imageUrl" in item:
                image_url = item["imageUrl"]
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
                images.append(image_url)
            
            # Extract property ID
            property_id = item.get("propertyId", f"hotel_{name.replace(' ', '_')}")
            
            return {
                "id": str(property_id),
                "name": name,
                "price_per_night": price,
                "rating": rating,
                "review_count": review_count,
                "address": address,
                "images": images,
                "amenities": [],
                "booking_url": None,  # Will be fixed later
                "source": "weekend_deals"
            }
            
        except Exception as e:
            self.logger.debug(f"Hotel item parsing error: {e}")
            return None
    
    def _get_nested_value(self, data: Dict[str, Any], path: List[str]) -> Any:
        """Safely get nested dictionary value."""
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def _extract_weekend_deals_count(self, data: Dict[str, Any]) -> int:
        """Extract count of weekend deals from response."""
        deals = self._get_nested_value(data, ["data", "weekendDeals", "weekendDealsProperties"])
        return len(deals) if deals and isinstance(deals, list) else 0
    
    def _calculate_completeness(self, hotel: Dict[str, Any]) -> float:
        """Calculate data completeness score."""
        fields = [
            'name', 'price_per_night', 'rating', 'address', 
            'images', 'amenities', 'reviews', 'faqs', 
            'surroundings', 'rooms', 'description'
        ]
        
        filled = 0
        for field in fields:
            if hotel.get(field):
                if isinstance(hotel[field], list):
                    if len(hotel[field]) > 0:
                        filled += 1
                else:
                    filled += 1
        
        return round((filled / len(fields)) * 100, 2)
    
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
    
    async def _handle_popups(self, page):
        """Handle cookie consent and other popups."""
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
        """Perform search on Booking.com."""
        try:
            # Fill location
            await page.fill("input[name='ss']", params["location"])
            await page.wait_for_timeout(1000)
            
            # Select autocomplete result
            try:
                await page.click("[data-testid='autocomplete-result']", timeout=3000)
            except:
                pass
            
            # Submit search
            await page.click("button[type='submit']:has-text('Search')", timeout=10000)
            await page.wait_for_load_state("networkidle", timeout=15000)
            
            self.logger.info("✅ Search executed successfully")
            
        except Exception as e:
            self.logger.warning(f"Search execution issue: {e}")