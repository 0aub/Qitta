"""
Enhanced Booking.com Hotel Scraper v5.0
========================================

Production-ready scraper with complete data extraction:
- Working reviews extraction with correct selectors
- Full amenities and surroundings extraction
- Proper image validation and deduplication
- Actual Google Maps URLs extraction
- Optimized and maintainable code structure

Version: 5.0 (Production Ready)
Author: Enhanced Implementation
"""

import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote, unquote
import hashlib


class BookingHotelsTask:
    """Production-ready Booking.com hotel scraper with complete data extraction."""
    
    BASE_URL = "https://www.booking.com"
    
    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point with enhanced error handling and complete data extraction."""
        try:
            # Validate parameters
            clean_params = BookingHotelsTask._validate_params(params)
            
            # Check scraping mode
            deep_scrape = params.get("deep_scrape", False) or params.get("deep_scrape_enabled", False)
            
            if deep_scrape:
                logger.info("🔥 DEEP SCRAPING ENABLED - Extracting complete data with reviews, images, and coordinates")
                scraper = EnhancedScraperEngine(browser, logger)
                hotels = await scraper.scrape_hotels_complete(clean_params)
                extraction_method = "deep_scraping_complete"
            else:
                logger.info("⚡ QUICK MODE - Extracting essential hotel data")
                scraper = EnhancedScraperEngine(browser, logger)
                hotels = await scraper.scrape_hotels_quick(clean_params)
                extraction_method = "quick_extraction"
            
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
            "max_results": max(1, int(params.get("max_results", 10)))
        }


class EnhancedScraperEngine:
    """Enhanced scraper engine with improved data extraction."""
    
    BASE_URL = "https://www.booking.com"
    
    def __init__(self, browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
        self.intercepted_data = {
            "weekend_deals": [],
            "graphql_responses": [],
            "search_results_html": None
        }
        
    async def scrape_hotels_quick(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Quick extraction with essential data."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Set up interception
            await self._setup_interception(page)
            
            # Navigate and search
            await page.goto(self.BASE_URL, wait_until="networkidle")
            await self._handle_popups(page)
            await self._perform_search(page, params)
            
            # Wait for data
            await page.wait_for_timeout(10000)
            
            # Store search results HTML for URL extraction
            self.intercepted_data["search_results_html"] = await page.content()
            
            # Parse weekend deals AND actual search results
            weekend_deals = await self._parse_weekend_deals()
            search_results = await self._parse_search_results(page)
            
            # Combine and deduplicate results, prioritizing search results
            hotels = self._combine_hotel_results(search_results, weekend_deals)
            
            # Apply location validation and filtering
            hotels = self._apply_location_filtering(hotels, params)
            
            # Fix URLs and images
            hotels = await self._fix_hotel_data(page, hotels)
            
            self.logger.info(f"✅ Quick extraction completed: {len(hotels)} hotels")
            return hotels
            
        finally:
            await context.close()
    
    async def scrape_hotels_complete(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Complete extraction with all details including reviews, coordinates, etc."""
        # Get basic hotels first
        basic_hotels = await self.scrape_hotels_quick(params)
        
        # Limit to max_results
        max_results = params.get("max_results", 10)
        hotels_to_process = basic_hotels[:max_results]
        
        # Enhance each hotel
        enhanced_hotels = []
        
        for i, hotel in enumerate(hotels_to_process, 1):
            try:
                self.logger.info(f"🏨 Deep scraping hotel {i}/{len(hotels_to_process)}: {hotel.get('name')}")
                
                # Try to get or generate URL
                if not hotel.get('booking_url'):
                    hotel['booking_url'] = await self._find_hotel_url(hotel, params)
                
                if hotel.get('booking_url'):
                    # Extract comprehensive data
                    enhanced_hotel = await self._extract_complete_hotel_data(hotel, params)
                    enhanced_hotels.append(enhanced_hotel)
                else:
                    self.logger.warning(f"⚠️ Could not find URL for: {hotel.get('name')}")
                    hotel['extraction_error'] = "No booking URL found"
                    enhanced_hotels.append(hotel)
                
                # Delay between requests
                if i < len(hotels_to_process):
                    await asyncio.sleep(2)
                    
            except Exception as e:
                self.logger.error(f"❌ Error enhancing hotel {hotel.get('name')}: {e}")
                hotel['extraction_error'] = str(e)
                enhanced_hotels.append(hotel)
        
        self.logger.info(f"🎯 Complete extraction finished: {len(enhanced_hotels)} hotels")
        return enhanced_hotels
    
    async def _find_hotel_url(self, hotel: Dict[str, Any], params: Dict[str, Any]) -> Optional[str]:
        """Find hotel URL using multiple strategies."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            # Strategy 1: Search by hotel name
            search_url = f"{self.BASE_URL}/searchresults.html?ss={quote(hotel.get('name', ''))}&checkin={params['check_in']}&checkout={params['check_out']}"
            await page.goto(search_url, wait_until="networkidle", timeout=20000)
            
            # Look for exact match
            hotel_cards = await page.query_selector_all("[data-testid='property-card']")
            for card in hotel_cards[:5]:  # Check first 5 results
                try:
                    title_element = await card.query_selector("[data-testid='title']")
                    if title_element:
                        title = await title_element.text_content()
                        if title and hotel.get('name') in title:
                            link = await card.query_selector("a[href*='/hotel/']")
                            if link:
                                href = await link.get_attribute("href")
                                if href:
                                    url = href.split('?')[0] if '?' in href else href
                                    if not url.startswith("http"):
                                        url = f"https://www.booking.com{url}"
                                    self.logger.info(f"✅ Found URL via search: {url}")
                                    return url
                except:
                    continue
            
            return None
            
        except Exception as e:
            self.logger.debug(f"URL finding error: {e}")
            return None
        finally:
            await context.close()
    
    async def _extract_complete_hotel_data(self, hotel: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Extract complete data for a single hotel with improved extraction methods."""
        context = await self._create_browser_context()
        page = await context.new_page()
        
        try:
            enhanced_hotel = hotel.copy()
            hotel_url = hotel.get('booking_url')
            
            if not hotel_url:
                return enhanced_hotel
            
            self.logger.info(f"🌐 Navigating to: {hotel_url}")
            
            # Navigate to hotel page with longer timeout
            await page.goto(hotel_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(5000)  # Increased wait time
            
            # Extract hotel ID from page
            hotel_id = await self._extract_hotel_id_from_page(page)
            if hotel_id:
                enhanced_hotel['hotel_id'] = hotel_id
            
            # 1. Extract Location Coordinates and Google Maps URL
            location_data = await self._extract_location_data(page)
            if location_data:
                enhanced_hotel.update(location_data)
            
            # 2. Extract All Images (high quality) - Improved
            images = await self._extract_all_images_enhanced(page)
            enhanced_hotel['images'] = images
            enhanced_hotel['image_count'] = len(images)
            
            # 3. Extract Reviews - FIXED with correct selectors
            reviews_data = await self._extract_reviews_fixed(page)
            if reviews_data:
                enhanced_hotel['reviews'] = reviews_data['reviews']
                enhanced_hotel['review_count'] = reviews_data['total_count']
                enhanced_hotel['rating_breakdown'] = reviews_data.get('rating_breakdown', {})
            
            # 4. Extract Amenities - IMPROVED
            amenities = await self._extract_amenities_improved(page)
            enhanced_hotel['amenities'] = amenities
            
            # 5. Extract Description
            description = await self._extract_description_improved(page)
            if description:
                enhanced_hotel['description'] = description
            
            # 6. Extract Surroundings - FIXED
            surroundings = await self._extract_surroundings_fixed(page)
            if surroundings:
                enhanced_hotel['surroundings'] = surroundings
            
            # Calculate completeness
            enhanced_hotel['data_completeness'] = self._calculate_completeness(enhanced_hotel)
            enhanced_hotel['scraping_timestamp'] = datetime.now().isoformat()
            
            self.logger.info(
                f"✅ Extracted complete data for {enhanced_hotel.get('name')}: "
                f"{len(images)} images, {len(enhanced_hotel.get('reviews', []))} reviews, "
                f"coordinates: {bool(location_data)}, completeness: {enhanced_hotel['data_completeness']}%"
            )
            
            return enhanced_hotel
            
        except Exception as e:
            self.logger.error(f"❌ Error extracting complete data: {e}", exc_info=True)
            hotel['extraction_error'] = str(e)
            return hotel
        finally:
            await context.close()
    
    async def _extract_hotel_id_from_page(self, page) -> Optional[str]:
        """Extract hotel ID from the current page."""
        try:
            # Try multiple methods
            # Method 1: From JavaScript variables
            hotel_id = await page.evaluate("""
                () => {
                    if (window.B && window.B.env && window.B.env.b_hotel_id) {
                        return window.B.env.b_hotel_id;
                    }
                    if (window.booking && window.booking.env && window.booking.env.b_hotel_id) {
                        return window.booking.env.b_hotel_id;
                    }
                    // Try from page source
                    const scripts = document.querySelectorAll('script');
                    for (let script of scripts) {
                        const match = script.textContent.match(/b_hotel_id["\s:]+["']?(\d+)["']?/);
                        if (match) return match[1];
                    }
                    return null;
                }
            """)
            
            if hotel_id:
                return str(hotel_id)
            
            # Method 2: From URL
            url = page.url
            match = re.search(r'hotel_id=(\d+)', url)
            if match:
                return match.group(1)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Hotel ID extraction error: {e}")
            return None
    
    async def _extract_location_data(self, page) -> Optional[Dict[str, Any]]:
        """Extract location coordinates and actual Google Maps URL."""
        try:
            location_data = {}
            
            # Extract coordinates
            coordinates = await page.evaluate("""
                () => {
                    // Try B.env variables
                    if (window.B && window.B.env) {
                        const env = window.B.env;
                        if (env.b_map_center_latitude && env.b_map_center_longitude) {
                            return {
                                lat: parseFloat(env.b_map_center_latitude),
                                lng: parseFloat(env.b_map_center_longitude)
                            };
                        }
                    }
                    
                    // Try from map container
                    const mapContainer = document.querySelector('#hotel_sidebar_static_map, .bui-map-container, #hotel_address_map');
                    if (mapContainer) {
                        const lat = mapContainer.getAttribute('data-atlas-latlng');
                        if (lat) {
                            const [latitude, longitude] = lat.split(',').map(parseFloat);
                            return { lat: latitude, lng: longitude };
                        }
                    }
                    
                    return null;
                }
            """)
            
            if coordinates:
                location_data['latitude'] = coordinates['lat']
                location_data['longitude'] = coordinates['lng']
            
            # Extract actual Google Maps URL from the page
            google_maps_url = await page.evaluate("""
                () => {
                    // Look for Google Maps links
                    const mapLinks = document.querySelectorAll('a[href*="maps.google"], a[href*="google.com/maps"]');
                    for (let link of mapLinks) {
                        const href = link.href;
                        if (href && (href.includes('maps.google') || href.includes('google.com/maps'))) {
                            return href;
                        }
                    }
                    
                    // Check map container
                    const mapContainer = document.querySelector('.map_static_zoom');
                    if (mapContainer) {
                        const link = mapContainer.querySelector('a');
                        if (link && link.href) return link.href;
                    }
                    
                    return null;
                }
            """)
            
            if google_maps_url:
                location_data['google_maps_url'] = google_maps_url
            elif coordinates:
                # Fallback to generated URL if no actual URL found
                location_data['google_maps_url'] = f"https://www.google.com/maps/search/{coordinates['lat']},{coordinates['lng']}"
            
            return location_data if location_data else None
            
        except Exception as e:
            self.logger.debug(f"Location extraction error: {e}")
            return None
    
    async def _extract_all_images_enhanced(self, page) -> List[str]:
        """Extract all hotel images with better validation and deduplication."""
        images = []
        seen_hashes = set()
        
        try:
            # Try to open photo gallery
            gallery_opened = False
            gallery_selectors = [
                "[data-testid='property-gallery-open-gallery-button']",
                "button[aria-label*='photo']",
                ".bh-photo-grid-item a",
                "a.photo_item",
                "[data-preview-image-layout] button"
            ]
            
            for selector in gallery_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        await element.click()
                        await page.wait_for_timeout(3000)
                        gallery_opened = True
                        self.logger.debug("✅ Opened photo gallery")
                        break
                except:
                    continue
            
            # Extract images
            image_urls = await page.evaluate("""
                () => {
                    const images = new Set();
                    
                    // Get all image elements
                    const selectors = [
                        '.slick-slide img',
                        '.bh-photo-modal img',
                        '[data-testid="image"] img',
                        '.hp-gallery__item img',
                        '.hp_gallery img',
                        'img[data-highres]',
                        'img.hide'
                    ];
                    
                    selectors.forEach(selector => {
                        document.querySelectorAll(selector).forEach(img => {
                            let src = img.getAttribute('data-highres') || 
                                     img.getAttribute('data-src') || 
                                     img.src;
                            
                            if (src && src.includes('bstatic')) {
                                // Clean and normalize URL
                                if (src.startsWith('//')) src = 'https:' + src;
                                else if (src.startsWith('/')) src = 'https://cf.bstatic.com' + src;
                                
                                // Convert to high quality
                                src = src.replace(/square\d+/, 'max1024x768')
                                        .replace(/max\d+/, 'max1024x768')
                                        .replace(/thumbnail/, 'max1024x768');
                                
                                images.add(src);
                            }
                        });
                    });
                    
                    return Array.from(images);
                }
            """)
            
            # Validate and deduplicate images
            for url in image_urls:
                if url and 'bstatic' in url:
                    # Create a simple hash for deduplication
                    url_hash = hashlib.md5(url.encode()).hexdigest()
                    if url_hash not in seen_hashes:
                        seen_hashes.add(url_hash)
                        images.append(url)
            
            # Close gallery if opened
            if gallery_opened:
                try:
                    await page.keyboard.press('Escape')
                    await page.wait_for_timeout(1000)
                except:
                    pass
            
            return images[:50]  # Limit to 50 images
            
        except Exception as e:
            self.logger.debug(f"Image extraction error: {e}")
            return images[:30] if images else []
    
    async def _extract_reviews_fixed(self, page) -> Optional[Dict[str, Any]]:
        """Extract individual reviews with all available details from reviewers."""
        try:
            reviews = []
            
            # Try to navigate to a dedicated reviews page or click review opener
            try:
                # Check if there's a dedicated reviews URL
                current_url = page.url
                reviews_url = current_url.replace('.html', '/reviews.html')
                
                # Try navigating to reviews page first
                try:
                    await page.goto(reviews_url, wait_until="networkidle", timeout=10000)
                    self.logger.info(f"✅ Navigated to dedicated reviews page: {reviews_url}")
                    await page.wait_for_timeout(3000)
                except:
                    # If that doesn't work, try clicking review links/buttons
                    self.logger.debug(f"Could not navigate to reviews page, trying click approach")
                    
                    # Look for various "show reviews" buttons
                    show_reviews_selectors = [
                        'a[href*="reviews.html"]',  # Direct link to reviews
                        '[data-testid="reviews-toggle-button"]',
                        '.reviews_header_count',
                        '[data-testid="reviews-opener"]', 
                        'a[href*="#reviews"]',
                        'a:has-text("reviews")',
                        'button:has-text("review")',
                        '.bui-link--sr-only:has-text("review")',
                        '#show_reviews_trigger',
                        '.bui-review-score__link'
                    ]
                    
                    for selector in show_reviews_selectors:
                        try:
                            await page.click(selector, timeout=3000)
                            self.logger.info(f"✅ Clicked reviews opener: {selector}")
                            await page.wait_for_timeout(5000)
                            break
                        except:
                            continue
                
            except Exception as e:
                self.logger.debug(f"Could not access reviews: {e}")
            
            # Wait longer for reviews to load dynamically and scroll to trigger loading
            await page.evaluate("""
                () => {
                    // Scroll down to trigger lazy loading of reviews
                    window.scrollTo(0, document.body.scrollHeight);
                }
            """)
            await page.wait_for_timeout(5000)
            
            # Scroll to reviews section to ensure they're loaded
            await page.evaluate("""
                () => {
                    const reviewSelectors = [
                        '#reviewlist', 
                        '[data-testid="reviews-section"]', 
                        '.review_list',
                        '.reviews-container',
                        '.review-card',
                        '[data-testid="review-card"]',
                        '.review_item_block',
                        '[class*="review_item"]'
                    ];
                    
                    for (let selector of reviewSelectors) {
                        const reviewSection = document.querySelector(selector);
                        if (reviewSection) {
                            reviewSection.scrollIntoView({ behavior: 'smooth' });
                            return;
                        }
                    }
                    
                    // Scroll back to top, then down again slowly
                    window.scrollTo(0, 0);
                    setTimeout(() => window.scrollTo(0, document.body.scrollHeight), 1000);
                }
            """)
            await page.wait_for_timeout(7000)
            
            # Extract reviews using multiple selector strategies with detailed logging
            result = await page.evaluate("""
                () => {
                    const reviews = [];
                    const debug = {
                        url: window.location.href,
                        title: document.title,
                        selectors_tried: [],
                        elements_found: {}
                    };
                    
                    // Try multiple selectors for review cards, excluding UI elements
                    const cardSelectors = [
                        '[data-testid="review-card"]',
                        '.review_list_new_item_block', 
                        '.review_item_wrapper',
                        '.c-review-block',
                        '.review-card',
                        '[data-review-item]',
                        '.bk-review',
                        '.review_item',
                        '.review-section .review',
                        '.bk-review-item', 
                        '.review_list .review',
                        '.review_item_block',
                        '[class*="review_item_"]',
                        '.review_block',
                        // More specific patterns to exclude UI elements
                        '[class*="review"]:not([class*="button"]):not([class*="link"]):not([class*="footer"]):not([class*="write"]):not([class*="login"])'
                    ];
                    
                    let reviewCards = [];
                    let usedSelector = null;
                    
                    for (let selector of cardSelectors) {
                        reviewCards = document.querySelectorAll(selector);
                        debug.selectors_tried.push({selector, count: reviewCards.length});
                        if (reviewCards.length > 0) {
                            usedSelector = selector;
                            break;
                        }
                    }
                    
                    debug.final_selector = usedSelector;
                    debug.final_count = reviewCards.length;
                    
                    // If no review cards found, log available elements for debugging
                    if (reviewCards.length === 0) {
                        const allDivs = document.querySelectorAll('div[class*="review"], div[data-testid*="review"]');
                        debug.elements_found.divs_with_review = allDivs.length;
                        
                        const allElements = document.querySelectorAll('*[class*="review"]');
                        debug.elements_found.all_with_review = allElements.length;
                        
                        // Sample first few elements with review-related classes
                        const reviewElements = Array.from(allElements).slice(0, 10);
                        debug.elements_found.sample_elements = reviewElements.map(el => ({
                            tag: el.tagName,
                            class: el.className,
                            testid: el.getAttribute('data-testid'),
                            text_preview: el.textContent ? el.textContent.substring(0, 100) : ''
                        }));
                    }
                    
                    // Add detailed debug for found elements
                    debug.sample_card_details = [];
                    
                    reviewCards.forEach((card, index) => {
                        if (index >= 30) return; // Limit to 30 reviews for better performance
                        
                        const review = {
                            review_index: index + 1,
                            raw_html_classes: card.className || '',
                            card_text_preview: card.textContent ? card.textContent.substring(0, 200).replace(/\\s+/g, ' ') : '',
                            card_tag: card.tagName
                        };
                        
                        // Store debug info for first few cards
                        if (index < 3) {
                            debug.sample_card_details.push({
                                index: index + 1,
                                tag: card.tagName,
                                class: card.className,
                                text_preview: card.textContent ? card.textContent.substring(0, 300).replace(/\\s+/g, ' ') : '',
                                children_count: card.children.length,
                                has_data_testid: !!card.getAttribute('data-testid')
                            });
                        }
                        
                        // === REVIEWER INFORMATION ===
                        
                        // Extract reviewer name with multiple selectors
                        const nameSelectors = [
                            '.bui-avatar-block__title',
                            '.c-guest-name', 
                            '.reviewer-name',
                            '[data-testid="review-author-name"]',
                            '.review-author__name',
                            '.b08850ce41.f546354b44',
                            '.reviewer_info .bui-link',
                            'span[data-testid="reviewer-name"]',
                            '.review_item_reviewer .bui-link',
                            '.review-author',
                            'h3', 'h4', 'strong',  // Try generic elements that might contain names
                            '[class*="name"]', '[class*="author"]'
                        ];
                        
                        for (let selector of nameSelectors) {
                            const nameEl = card.querySelector(selector);
                            if (nameEl && nameEl.textContent.trim()) {
                                review.reviewer_name = nameEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // Extract reviewer country/location
                        const countrySelectors = [
                            '.bui-avatar-block__subtitle',
                            '.c-guest-location',
                            '.reviewer-country',
                            '.reviewer_info .country',
                            '.d838fb5f41.aea5eccb71',
                            '[data-testid="reviewer-country"]',
                            '.review_item_reviewer .bui-text--variant'
                        ];
                        
                        for (let selector of countrySelectors) {
                            const countryEl = card.querySelector(selector);
                            if (countryEl && countryEl.textContent.trim()) {
                                review.reviewer_country = countryEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // Extract reviewer avatar
                        const avatarEl = card.querySelector('.bui-avatar img, .reviewer-avatar img, img[alt*="reviewer"]');
                        if (avatarEl) {
                            review.reviewer_avatar = avatarEl.src;
                        }
                        
                        // === REVIEW SCORE ===
                        const scoreSelectors = [
                            '.bui-review-score__badge',
                            '.c-score-bar__score',
                            '[data-testid="review-score"]',
                            '.review-score-badge',
                            '.review_score .bui-review-score__badge',
                            '.f63b14ab7a.dff2e52086'
                        ];
                        
                        for (let selector of scoreSelectors) {
                            const scoreEl = card.querySelector(selector);
                            if (scoreEl) {
                                const scoreText = scoreEl.textContent;
                                const match = scoreText.match(/\\d+\\.?\\d*/);
                                if (match) {
                                    review.review_score = parseFloat(match[0]);
                                    break;
                                }
                            }
                        }
                        
                        // === REVIEW DATE ===
                        const dateSelectors = [
                            '.c-review__date',
                            '[data-testid="review-date"]',
                            '.review_item_date',
                            '.review-date',
                            '.bui-review-score .review_item_date',
                            '.review_score_date span'
                        ];
                        
                        for (let selector of dateSelectors) {
                            const dateEl = card.querySelector(selector);
                            if (dateEl && dateEl.textContent.trim()) {
                                review.review_date = dateEl.textContent.replace(/Reviewed:?\\s*/i, '').trim();
                                break;
                            }
                        }
                        
                        // === REVIEW TITLE ===
                        const titleSelectors = [
                            '[data-testid="review-title"]',
                            '.c-review__title',
                            '.review-title',
                            '.bui-review-score__title',
                            '.review_item_header_content .review_item_header_content_title'
                        ];
                        
                        for (let selector of titleSelectors) {
                            const titleEl = card.querySelector(selector);
                            if (titleEl && titleEl.textContent.trim()) {
                                review.review_title = titleEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // === POSITIVE REVIEW TEXT ===
                        const positiveSelectors = [
                            '[data-testid="review-positive-text"]',
                            '.c-review__positive',
                            '.review-positive',
                            '.c-review-text--positive .b99b6ef58f',
                            '.review_pos .review_item_review_content',
                            '.review_item_review_content_positive'
                        ];
                        
                        for (let selector of positiveSelectors) {
                            const positiveEl = card.querySelector(selector);
                            if (positiveEl) {
                                // Try to get the actual review text, not just the label
                                const textEl = positiveEl.querySelector('.b99b6ef58f, .c-review-text, .review-text') || positiveEl;
                                if (textEl && textEl.textContent.trim() && !textEl.textContent.includes('Liked most:')) {
                                    review.review_positive = textEl.textContent.trim();
                                    break;
                                }
                            }
                        }
                        
                        // === NEGATIVE REVIEW TEXT ===
                        const negativeSelectors = [
                            '[data-testid="review-negative-text"]',
                            '.c-review__negative',
                            '.review-negative',
                            '.c-review-text--negative .b99b6ef58f',
                            '.review_neg .review_item_review_content',
                            '.review_item_review_content_negative'
                        ];
                        
                        for (let selector of negativeSelectors) {
                            const negativeEl = card.querySelector(selector);
                            if (negativeEl) {
                                // Try to get the actual review text, not just the label
                                const textEl = negativeEl.querySelector('.b99b6ef58f, .c-review-text, .review-text') || negativeEl;
                                if (textEl && textEl.textContent.trim() && !textEl.textContent.includes('Liked least:')) {
                                    review.review_negative = textEl.textContent.trim();
                                    break;
                                }
                            }
                        }
                        
                        // === STAY INFORMATION ===
                        
                        // Room type
                        const roomSelectors = [
                            '[data-testid="review-room-name"]',
                            '.c-review__room-type',
                            '.review-room-type',
                            '.review_item_info_tags .review_item_info_tag'
                        ];
                        
                        for (let selector of roomSelectors) {
                            const roomEl = card.querySelector(selector);
                            if (roomEl && roomEl.textContent.trim()) {
                                review.room_type = roomEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // Traveler type
                        const travelerSelectors = [
                            '[data-testid="review-traveler-type"]',
                            '.c-review__traveler-type',
                            '.review-traveler-type',
                            '.review_item_info_tags .review_traveler_type'
                        ];
                        
                        for (let selector of travelerSelectors) {
                            const travelerEl = card.querySelector(selector);
                            if (travelerEl && travelerEl.textContent.trim()) {
                                review.traveler_type = travelerEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // Number of nights
                        const nightsSelectors = [
                            '[data-testid="review-num-nights"]',
                            '.c-review__nights',
                            '.review-nights',
                            '.review_item_info_tags .nights'
                        ];
                        
                        for (let selector of nightsSelectors) {
                            const nightsEl = card.querySelector(selector);
                            if (nightsEl && nightsEl.textContent.trim()) {
                                review.nights_stayed = nightsEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // Stay date
                        const stayDateSelectors = [
                            '[data-testid="review-stay-date"]',
                            '.c-review__stay-date',
                            '.review-stay-date'
                        ];
                        
                        for (let selector of stayDateSelectors) {
                            const stayDateEl = card.querySelector(selector);
                            if (stayDateEl && stayDateEl.textContent.trim()) {
                                review.stay_date = stayDateEl.textContent.trim();
                                break;
                            }
                        }
                        
                        // === REVIEW HELPFULNESS ===
                        const helpfulEl = card.querySelector('.review-helpful, .helpful-count, [data-testid="helpful-count"]');
                        if (helpfulEl) {
                            const helpfulMatch = helpfulEl.textContent.match(/\\d+/);
                            if (helpfulMatch) {
                                review.helpful_count = parseInt(helpfulMatch[0]);
                            }
                        }
                        
                        // Only add review if we have meaningful content
                        const hasContent = review.reviewer_name || review.review_positive || review.review_negative || review.review_score || review.review_title;
                        
                        if (hasContent) {
                            // Add timestamp
                            review.extracted_at = new Date().toISOString();
                            reviews.push(review);
                        }
                    });
                    
                    debug.extracted_count = reviews.length;
                    return { reviews, debug };
                }
            """)
            
            # Extract reviews and debug info
            reviews = result.get('reviews', [])
            debug_info = result.get('debug', {})
            
            # Log debug information for troubleshooting
            self.logger.info(f"🔍 Review extraction debug:")
            self.logger.info(f"   URL: {debug_info.get('url', 'unknown')}")
            self.logger.info(f"   Title: {debug_info.get('title', 'unknown')}")
            self.logger.info(f"   Final selector: {debug_info.get('final_selector')}")
            self.logger.info(f"   Final count: {debug_info.get('final_count', 0)}")
            
            if debug_info.get('elements_found'):
                elements = debug_info['elements_found']
                self.logger.info(f"   Elements found: {elements.get('all_with_review', 0)} total with 'review' in class")
                
                if elements.get('sample_elements'):
                    self.logger.info(f"   Sample elements:")
                    for i, el in enumerate(elements['sample_elements'][:3]):
                        self.logger.info(f"     {i+1}: {el['tag']}.{el['class'][:50]}...")
            
            if debug_info.get('sample_card_details'):
                self.logger.info(f"   Detailed card analysis:")
                for card in debug_info['sample_card_details']:
                    self.logger.info(f"     Card {card['index']}: {card['tag']}.{card['class'][:50]}")
                    self.logger.info(f"       Text: {card['text_preview'][:100]}...")
                    self.logger.info(f"       Children: {card['children_count']}, Has data-testid: {card['has_data_testid']}")
            
            # Get total review count - UPDATED selector
            total_count = await page.evaluate("""
                () => {
                    // Look for review count in the review opener element
                    const reviewOpener = document.querySelector('.fff1944c52.fb14de7f14');
                    if (reviewOpener && reviewOpener.textContent.includes('review')) {
                        const text = reviewOpener.textContent;
                        const match = text.match(/\\d[\\d,]*/);
                        if (match) return parseInt(match[0].replace(/,/g, ''));
                    }
                    
                    // Try alternative selectors for review count
                    const countSelectors = [
                        '.reviews_header_count',
                        '[data-testid="reviews-count"]', 
                        '.review_list_header_count',
                        '.bui-review-score__text'
                    ];
                    
                    for (let selector of countSelectors) {
                        const el = document.querySelector(selector);
                        if (el) {
                            const text = el.textContent;
                            const match = text.match(/\\d[\\d,]*/);
                            if (match) return parseInt(match[0].replace(/,/g, ''));
                        }
                    }
                    
                    return 0;
                }
            """)
            
            # Extract rating breakdown - UPDATED to use the correct selectors from the hidden element
            rating_breakdown = await page.evaluate("""
                () => {
                    const breakdown = {};
                    
                    // Look for rating categories in the hidden breakdown tooltip
                    const categories = document.querySelectorAll('#review_list_score_breakdown li');
                    
                    categories.forEach(cat => {
                        const label = cat.querySelector('.review_score_name');
                        const score = cat.querySelector('.review_score_value');
                        
                        if (label && score) {
                            const name = label.textContent.trim();
                            const value = parseFloat(score.textContent);
                            if (name && !isNaN(value)) {
                                breakdown[name] = value;
                            }
                        }
                    });
                    
                    // Fallback: try other possible selectors
                    if (Object.keys(breakdown).length === 0) {
                        const fallbackCategories = document.querySelectorAll('.review_list_score_breakdown li, [data-testid="review-subscore"]');
                        
                        fallbackCategories.forEach(cat => {
                            const label = cat.querySelector('.review_score_name, .c-score-bar__title');
                            const score = cat.querySelector('.review_score_value, .c-score-bar__score');
                            
                            if (label && score) {
                                const name = label.textContent.trim();
                                const value = parseFloat(score.textContent);
                                if (name && !isNaN(value)) {
                                    breakdown[name] = value;
                                }
                            }
                        });
                    }
                    
                    return breakdown;
                }
            """)
            
            if len(reviews) > 0 or len(rating_breakdown) > 0:
                return {
                    'reviews': reviews,
                    'total_count': total_count or len(reviews),
                    'rating_breakdown': rating_breakdown
                }
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Review extraction error: {e}")
            return None
    
    async def _extract_amenities_improved(self, page) -> List[str]:
        """Extract amenities with improved selectors and logic."""
        amenities = set()
        
        try:
            # First, try to expand all amenities
            try:
                show_all_button = await page.query_selector('button:has-text("Show all"), button:has-text("facilities")')
                if show_all_button:
                    await show_all_button.click()
                    await page.wait_for_timeout(2000)
            except:
                pass
            
            # Extract amenities using multiple strategies
            amenities_data = await page.evaluate("""
                () => {
                    const amenities = new Set();
                    
                    // Strategy 1: Property highlights
                    document.querySelectorAll('[data-testid="property-highlights"] li').forEach(el => {
                        const text = el.textContent.trim();
                        if (text) amenities.add(text);
                    });
                    
                    // Strategy 2: Important facilities
                    document.querySelectorAll('.important_facility, .hp-description .important_facility').forEach(el => {
                        const text = el.textContent.trim();
                        if (text) amenities.add(text);
                    });
                    
                    // Strategy 3: Facilities list
                    document.querySelectorAll('.facilitiesChecklist .facilitiesChecklistSection').forEach(section => {
                        section.querySelectorAll('li, .bui-list__item').forEach(item => {
                            const text = item.textContent.trim();
                            if (text && !text.includes('Not included')) amenities.add(text);
                        });
                    });
                    
                    // Strategy 4: Hotel facilities
                    document.querySelectorAll('[data-testid="facility"], .hotel-facilities__item').forEach(el => {
                        const text = el.textContent.trim();
                        if (text) amenities.add(text);
                    });
                    
                    // Strategy 5: Property amenities section
                    document.querySelectorAll('[data-testid="property-amenities-list"] li').forEach(el => {
                        const text = el.textContent.trim();
                        if (text) amenities.add(text);
                    });
                    
                    // Strategy 6: Most popular facilities
                    document.querySelectorAll('.hp_desc_important_facilities li').forEach(el => {
                        const text = el.textContent.trim().replace(/\\n/g, ' ').replace(/\\s+/g, ' ');
                        if (text && text.length > 2) amenities.add(text);
                    });
                    
                    return Array.from(amenities).filter(a => a.length > 2 && a.length < 100);
                }
            """)
            
            return list(amenities_data)[:50]  # Limit to 50 amenities
            
        except Exception as e:
            self.logger.debug(f"Amenities extraction error: {e}")
            return list(amenities)[:30] if amenities else []
    
    async def _extract_description_improved(self, page) -> Optional[str]:
        """Extract hotel description with improved logic."""
        try:
            description = await page.evaluate("""
                () => {
                    // Try multiple selectors
                    const selectors = [
                        '[data-testid="property-description"]',
                        '#property_description_content',
                        '.hp_desc_main_content',
                        '#summary',
                        '.property-description'
                    ];
                    
                    for (let selector of selectors) {
                        const el = document.querySelector(selector);
                        if (el) {
                            const text = el.textContent.trim()
                                .replace(/\\n+/g, ' ')
                                .replace(/\\s+/g, ' ');
                            if (text.length > 50) return text;
                        }
                    }
                    
                    // Try to get from structured data
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (let script of scripts) {
                        try {
                            const data = JSON.parse(script.textContent);
                            if (data.description) return data.description;
                        } catch (e) {}
                    }
                    
                    return null;
                }
            """)
            
            return description[:2000] if description else None  # Limit length
            
        except Exception as e:
            self.logger.debug(f"Description extraction error: {e}")
            return None
    
    async def _extract_surroundings_fixed(self, page) -> Dict[str, List]:
        """Extract surroundings with fixed selectors and logic."""
        surroundings = {
            'landmarks': [],
            'restaurants': [],
            'transport': []
        }
        
        try:
            # Scroll to surroundings section
            await page.evaluate("""
                () => {
                    const section = document.querySelector('.hp-surroundings, [data-component="surroundings"], #distance_section');
                    if (section) section.scrollIntoView();
                }
            """)
            await page.wait_for_timeout(1000)
            
            # Extract surroundings data
            surroundings_data = await page.evaluate("""
                () => {
                    const result = {
                        landmarks: [],
                        restaurants: [],
                        transport: []
                    };
                    
                    // Method 1: Look for distance section
                    const distanceSection = document.querySelector('#distance_section, .hp-surroundings');
                    if (distanceSection) {
                        const items = distanceSection.querySelectorAll('li, .bui-list__item, .hp-poi-list__item');
                        items.forEach(item => {
                            const text = item.textContent.trim();
                            if (!text) return;
                            
                            // Categorize based on content
                            if (text.match(/airport|station|metro|bus|train/i)) {
                                result.transport.push(text);
                            } else if (text.match(/restaurant|cafe|coffee|bar|food|dining/i)) {
                                result.restaurants.push(text);
                            } else if (text.match(/beach|park|museum|mall|center|square/i)) {
                                result.landmarks.push(text);
                            } else {
                                // Default to landmarks for other POIs
                                result.landmarks.push(text);
                            }
                        });
                    }
                    
                    // Method 2: Look for specific surrounding sections
                    const sections = document.querySelectorAll('.hp-poi-content__section');
                    sections.forEach(section => {
                        const title = section.querySelector('.hp-poi-content__section-title');
                        const items = section.querySelectorAll('.hp-poi-content__item');
                        
                        if (!title) return;
                        const sectionTitle = title.textContent.toLowerCase();
                        
                        items.forEach(item => {
                            const name = item.querySelector('.hp-poi-list__item-name');
                            const distance = item.querySelector('.hp-poi-list__item-distance');
                            
                            if (name) {
                                const text = name.textContent + (distance ? ' - ' + distance.textContent : '');
                                
                                if (sectionTitle.includes('transport') || sectionTitle.includes('airport')) {
                                    result.transport.push(text);
                                } else if (sectionTitle.includes('restaurant') || sectionTitle.includes('food')) {
                                    result.restaurants.push(text);
                                } else {
                                    result.landmarks.push(text);
                                }
                            }
                        });
                    });
                    
                    // Method 3: Alternative selectors
                    if (result.landmarks.length === 0 && result.restaurants.length === 0 && result.transport.length === 0) {
                        // Try alternative extraction
                        const surroundingsList = document.querySelectorAll('[data-testid="surroundings-list"] li');
                        surroundingsList.forEach(item => {
                            const text = item.textContent.trim();
                            if (text) result.landmarks.push(text);
                        });
                    }
                    
                    // Limit and clean results
                    result.landmarks = result.landmarks.slice(0, 10).map(t => t.replace(/\\s+/g, ' ').trim());
                    result.restaurants = result.restaurants.slice(0, 10).map(t => t.replace(/\\s+/g, ' ').trim());
                    result.transport = result.transport.slice(0, 10).map(t => t.replace(/\\s+/g, ' ').trim());
                    
                    return result;
                }
            """)
            
            return surroundings_data
            
        except Exception as e:
            self.logger.debug(f"Surroundings extraction error: {e}")
            return surroundings
    
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
                    
                    # Store all GraphQL responses
                    self.intercepted_data["graphql_responses"].append(data)
                    
                except Exception as e:
                    self.logger.debug(f"Response parsing error: {e}")
        
        page.on("response", handle_response)
    
    async def _parse_weekend_deals(self) -> List[Dict[str, Any]]:
        """Parse weekend deals responses."""
        hotels = []
        
        for response_data in self.intercepted_data["weekend_deals"]:
            try:
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
    
    async def _fix_hotel_data(self, page, hotels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fix hotel URLs and images."""
        try:
            # Get all hotel links from the current page
            hotel_links = await page.query_selector_all("a[href*='/hotel/']")
            url_map = {}
            
            for link in hotel_links:
                try:
                    href = await link.get_attribute("href")
                    if href and "/hotel/" in href:
                        # Extract clean URL
                        clean_url = href.split('?')[0] if '?' in href else href
                        if not clean_url.startswith("http"):
                            clean_url = f"https://www.booking.com{clean_url}"
                        
                        # Try to get hotel name
                        parent = await link.query_selector("xpath=..")
                        title_element = await parent.query_selector("[data-testid='title']") if parent else None
                        if not title_element:
                            title_element = await link.query_selector("[data-testid='title']")
                        
                        if title_element:
                            title = await title_element.text_content()
                            if title:
                                url_map[title.strip()] = clean_url
                        
                except Exception as e:
                    self.logger.debug(f"Link extraction error: {e}")
                    continue
            
            # Fix each hotel
            for hotel in hotels:
                # Fix URL
                hotel_name = hotel.get('name', '')
                if hotel_name and not hotel.get('booking_url'):
                    # Try exact match
                    if hotel_name in url_map:
                        hotel['booking_url'] = url_map[hotel_name]
                    else:
                        # Try partial match
                        for name, url in url_map.items():
                            if hotel_name in name or name in hotel_name:
                                hotel['booking_url'] = url
                                break
                
                # Fix images
                fixed_images = []
                seen_hashes = set()
                for img in hotel.get('images', []):
                    fixed_img = self._fix_image_url(img)
                    if fixed_img:
                        # Simple deduplication
                        img_hash = hashlib.md5(fixed_img.encode()).hexdigest()
                        if img_hash not in seen_hashes:
                            seen_hashes.add(img_hash)
                            fixed_images.append(fixed_img)
                hotel['images'] = fixed_images
                
            self.logger.info(f"✅ Fixed data for {len(hotels)} hotels")
            
        except Exception as e:
            self.logger.debug(f"Hotel data fixing error: {e}")
        
        return hotels
    
    def _parse_hotel_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse individual hotel item from weekend deals."""
        try:
            # Extract name
            name = item.get("propertyName", "Unknown Hotel")
            if name == "Unknown Hotel":
                return None
            
            # Extract ID
            property_id = item.get("propertyId", "")
            
            # Extract price
            price = None
            if "price" in item and item["price"]:
                price_data = item["price"]
                if "formattedPrice" in price_data:
                    price_str = price_data["formattedPrice"]
                    numbers = re.findall(r'[\d,]+\.?\d*', price_str.replace(',', ''))
                    if numbers:
                        price = float(numbers[0])
                elif "amount" in price_data:
                    price = float(price_data["amount"])
            
            # Extract rating
            rating = None
            review_count = None
            if "review" in item and item["review"]:
                rating = item["review"].get("score")
                review_count = item["review"].get("reviewCount")
            
            # Extract address
            address = item.get("subtitle", "")
            
            # Extract images (fix URLs immediately)
            images = []
            if "imageUrl" in item:
                image_url = self._fix_image_url(item["imageUrl"])
                if image_url:
                    images.append(image_url)
            
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
    
    async def _parse_search_results(self, page) -> List[Dict[str, Any]]:
        """Parse actual search results from the page - location-specific results."""
        hotels = []
        try:
            # Wait for search results to load
            await page.wait_for_selector("[data-testid='property-card']", timeout=10000)
            
            # Extract hotel data from property cards
            hotel_cards = await page.query_selector_all("[data-testid='property-card']")
            
            self.logger.info(f"📍 Found {len(hotel_cards)} location-specific search results")
            
            for card in hotel_cards:
                try:
                    hotel = await self._extract_hotel_from_card(card)
                    if hotel:
                        hotels.append(hotel)
                except Exception as e:
                    self.logger.debug(f"Error extracting hotel card: {e}")
                    continue
            
            self.logger.info(f"✅ Parsed {len(hotels)} hotels from search results")
            
        except Exception as e:
            self.logger.warning(f"Could not find search results: {e}")
        
        return hotels
    
    async def _extract_hotel_from_card(self, card) -> Optional[Dict[str, Any]]:
        """Extract hotel information from a property card."""
        try:
            # Extract hotel name
            name_element = await card.query_selector("[data-testid='title']")
            name = await name_element.inner_text() if name_element else None
            
            if not name:
                return None
            
            # Extract price
            price = None
            price_element = await card.query_selector("[data-testid='price-and-discounted-price']")
            if price_element:
                price_text = await price_element.inner_text()
                price_match = re.search(r'[\d,]+', price_text.replace(',', ''))
                if price_match:
                    price = float(price_match.group())
            
            # Extract rating
            rating = None
            rating_element = await card.query_selector("[data-testid='review-score'] div")
            if rating_element:
                rating_text = await rating_element.inner_text()
                rating_match = re.search(r'([\d.]+)', rating_text)
                if rating_match:
                    rating = float(rating_match.group(1))
            
            # Extract review count
            review_count = None
            review_element = await card.query_selector("[data-testid='review-score'] + div")
            if review_element:
                review_text = await review_element.inner_text()
                review_match = re.search(r'([\d,]+)', review_text.replace(',', ''))
                if review_match:
                    review_count = int(review_match.group(1))
            
            # Extract location/address
            address_element = await card.query_selector("[data-testid='address']")
            address = await address_element.inner_text() if address_element else None
            
            # Extract image
            img_element = await card.query_selector("img")
            image = await img_element.get_attribute("src") if img_element else None
            images = [image] if image else []
            
            # Extract hotel URL for ID
            link_element = await card.query_selector("a[data-testid='title-link']")
            booking_url = await link_element.get_attribute("href") if link_element else None
            
            # Generate hotel ID from URL or name
            hotel_id = None
            if booking_url:
                id_match = re.search(r'hotel/([^/]+)', booking_url)
                if id_match:
                    hotel_id = id_match.group(1)
            
            if not hotel_id:
                hotel_id = hashlib.md5(name.encode()).hexdigest()[:8]
            
            return {
                "id": hotel_id,
                "name": name,
                "price_per_night": price,
                "rating": rating,
                "review_count": review_count,
                "address": address,
                "images": images,
                "amenities": [],
                "booking_url": booking_url,
                "source": "search_results"
            }
            
        except Exception as e:
            self.logger.debug(f"Hotel card extraction error: {e}")
            return None
    
    def _combine_hotel_results(self, search_results: List[Dict[str, Any]], weekend_deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Combine search results with weekend deals, prioritizing search results and removing duplicates."""
        combined = []
        seen_names = set()
        
        # First add search results (location-specific)
        for hotel in search_results:
            name = hotel.get('name', '').strip().lower()
            if name and name not in seen_names:
                seen_names.add(name)
                combined.append(hotel)
        
        # Then add weekend deals that aren't duplicates
        for hotel in weekend_deals:
            name = hotel.get('name', '').strip().lower()
            if name and name not in seen_names:
                seen_names.add(name)
                combined.append(hotel)
        
        self.logger.info(f"🔄 Combined results: {len(search_results)} search + {len(weekend_deals)} weekend deals = {len(combined)} unique hotels")
        
        return combined
    
    def _apply_location_filtering(self, hotels: List[Dict[str, Any]], params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply location-based filtering to ensure results match the requested location."""
        requested_location = params.get("location", "").lower()
        filtered_hotels = []
        
        # Extract location keywords for matching
        location_keywords = self._extract_location_keywords(requested_location)
        
        for hotel in hotels:
            # Prioritize search_results over weekend_deals
            if hotel.get("source") == "search_results":
                filtered_hotels.append(hotel)
            elif hotel.get("source") == "weekend_deals":
                # For weekend deals, check if address matches requested location
                if self._location_matches(hotel.get("address", ""), location_keywords):
                    filtered_hotels.append(hotel)
                else:
                    self.logger.debug(f"Filtered out weekend deal: {hotel.get('name')} - location mismatch")
        
        self.logger.info(f"📍 Location filtering: {len(hotels)} -> {len(filtered_hotels)} hotels match '{requested_location}'")
        
        return filtered_hotels
    
    def _extract_location_keywords(self, location: str) -> List[str]:
        """Extract keywords from location string for matching."""
        # Remove common words and normalize
        common_words = {"hotel", "hotels", "in", "at", "near", "area", "district", "city"}
        location_clean = location.lower().replace(",", " ")
        
        keywords = []
        for word in location_clean.split():
            word = word.strip()
            if len(word) > 2 and word not in common_words:
                keywords.append(word)
        
        return keywords
    
    def _location_matches(self, address: str, keywords: List[str]) -> bool:
        """Check if hotel address matches the requested location keywords."""
        if not address or not keywords:
            return True  # If no address info, don't filter out
        
        address_lower = address.lower()
        
        # Check if any keyword matches
        for keyword in keywords:
            if keyword in address_lower:
                return True
        
        return False
    
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
            'surroundings': 5,
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
                "button[id*='accept']",
                "button:has-text('Accept')",
                "#onetrust-accept-btn-handler",
                "button:has-text('Got it')",
                ".bui-button--primary"
            ]
            
            for selector in selectors:
                try:
                    await page.click(selector, timeout=2000)
                    self.logger.debug(f"✅ Handled popup: {selector}")
                    await page.wait_for_timeout(500)
                    break
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Popup handling: {e}")
    
    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform search on Booking.com."""
        try:
            # Fill location
            search_input = await page.query_selector("input[name='ss']")
            if search_input:
                await search_input.fill(params["location"])
                await page.wait_for_timeout(1500)
            
            # Try to select from autocomplete
            try:
                await page.click("[data-testid='autocomplete-result']:first-child", timeout=3000)
            except:
                pass
            
            # Set dates if date inputs are visible
            try:
                checkin = await page.query_selector("[data-testid='date-display-field-start']")
                if checkin:
                    await checkin.click()
                    await page.wait_for_timeout(500)
                    # Date selection logic here if needed
            except:
                pass
            
            # Submit search
            search_button = await page.query_selector("button[type='submit']:has-text('Search')")
            if search_button:
                await search_button.click()
            else:
                # Alternative: press Enter
                await page.keyboard.press("Enter")
            
            await page.wait_for_load_state("networkidle", timeout=20000)
            
            self.logger.info("✅ Search executed successfully")
            
        except Exception as e:
            self.logger.warning(f"Search execution issue: {e}")