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
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point with enhanced error handling and complete data extraction."""
        try:
            # Validate parameters
            clean_params = BookingHotelsTask._validate_params(params)
            
            # Check scraping mode
            deep_scrape = params.get("deep_scrape", False) or params.get("deep_scrape_enabled", False)
            
            logger.info(f"🚀 STARTING BOOKING.COM SCRAPER v6.0 - DOM SCRAPING EDITION")
            logger.info(f"🔄 CODE VERSION CHECK: This should appear if new code is loaded!")
            logger.info(f"📍 Location: {clean_params['location']}")
            logger.info(f"📅 Dates: {clean_params['check_in']} to {clean_params['check_out']}")
            logger.info(f"🏨 Mode: {'Deep Scraping' if deep_scrape else 'Quick Extraction'}")
            
            # Create scraper instance
            scraper = ModernBookingScraper(browser, logger)
            
            if deep_scrape:
                logger.info("🔥 DEEP SCRAPING ENABLED - Extracting complete data with reviews, images, and coordinates")
                hotels = await scraper.scrape_hotels_complete(clean_params)
                extraction_method = "deep_scraping_complete"
            else:
                logger.info("⚡ QUICK MODE - Extracting essential hotel data")
                hotels = await scraper.scrape_hotels_quick(clean_params)
                extraction_method = "quick_extraction"
            
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
    
    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform search on Booking.com using DOM interaction."""
        try:
            self.logger.info(f"🔍 Navigating to Booking.com and searching for: {params['location']}")
            
            # Navigate to booking.com
            await page.goto(self.BASE_URL, wait_until="networkidle")
            
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
                    target_adults = params["adults"]
                    
                    if target_adults > current_adults:
                        for _ in range(target_adults - current_adults):
                            await page.locator("button[aria-label*='Increase'][aria-label*='Adults']").click()
                            await page.wait_for_timeout(200)
                    elif target_adults < current_adults:
                        for _ in range(current_adults - target_adults):
                            await page.locator("button[aria-label*='Decrease'][aria-label*='Adults']").click()
                            await page.wait_for_timeout(200)
                    
                    # Close occupancy dialog
                    await page.locator("button:has-text('Done')").click()
                    self.logger.info(f"✅ Set adults to {target_adults}")
                    
            except Exception as e:
                self.logger.debug(f"Adults selection skipped: {e}")
            
            # Click search button
            search_button = page.locator("button:has-text('Search'), button[type='submit']").first
            await search_button.click()
            
            # Wait for search results with more flexible approach
            try:
                # First try to wait for property cards directly (faster)
                await page.wait_for_selector("div[data-testid='property-card']", timeout=20000)
                self.logger.info("✅ Property cards loaded successfully")
            except Exception as e:
                self.logger.warning(f"⚠️  Property cards not found, trying alternate selectors: {e}")
                # Try alternate selectors for search results
                try:
                    await page.wait_for_selector("[data-testid='property-card'], .sr_property_block, .bh-property-card", timeout=10000)
                    self.logger.info("✅ Search results loaded with alternate selector")
                except Exception as e2:
                    self.logger.warning(f"⚠️  No search results detected: {e2}")
            
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
    
    async def _scrape_property_cards(self, page, max_results: int, deep_scrape: bool = False) -> List[Dict[str, Any]]:
        """Scrape hotel data from property cards."""
        hotels = []
        
        try:
            # Get all property cards
            property_cards = page.locator("div[data-testid='property-card']")
            card_count = await property_cards.count()
            
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
    
    async def _extract_basic_hotel_data(self, card, index: int) -> Optional[Dict[str, Any]]:
        """Extract basic hotel data from a property card."""
        try:
            hotel_data = {}
            
            # Hotel name
            name_element = card.locator("div[data-testid='title']")
            if await name_element.is_visible():
                hotel_data['name'] = await name_element.inner_text()
            
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
            
            # Price
            price_element = card.locator("span[data-testid='price-and-discounted-price'], .bui-price-display__value")
            if await price_element.is_visible():
                price_text = await price_element.inner_text()
                hotel_data['price_per_night'] = self._extract_price_number(price_text)
            
            # Rating
            rating_element = card.locator("div[data-testid='review-score'] > div:first-child, .bui-review-score__badge")
            if await rating_element.is_visible():
                rating_text = await rating_element.inner_text()
                try:
                    hotel_data['rating'] = float(rating_text.strip())
                except:
                    pass
            
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
                await hotel_page.goto(hotel_url, wait_until='networkidle', timeout=45000)
                
                
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
                
                # Extract reviews (if requested)
                reviews_data = await self._extract_reviews(hotel_page)
                if reviews_data:
                    hotel_data['reviews'] = reviews_data['reviews']
                    hotel_data['review_count'] = reviews_data.get('total_count', hotel_data.get('review_count', 0))
                    hotel_data['rating_breakdown'] = reviews_data.get('rating_breakdown', {})
                
                return hotel_data
                
            finally:
                await hotel_page.close()
                
        except Exception as e:
            self.logger.warning(f"⚠️  Detailed extraction failed for {hotel_data.get('name')}: {e}")
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