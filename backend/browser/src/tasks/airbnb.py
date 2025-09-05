"""
Airbnb Home Scraper - Production Ready
=====================================

Clean, optimized Airbnb scraper with 4-level extraction system:
- Level 1: Quick search (search results only)
- Level 2: Full data (property pages with complete details)
- Level 3: Basic reviews (Level 2 + 2-5 reviews per property)
- Level 4: Deep reviews (Level 2 + comprehensive review extraction)

Version: 1.0 (Production Clean)
Author: Based on booking_hotels.py best practices
"""

import json
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import quote, unquote, urlparse
import hashlib


class AirbnbTask:
    """Production-ready Airbnb home scraper with 4-level extraction."""
    
    BASE_URL = "https://www.airbnb.com"

    @staticmethod
    async def run(params: Dict[str, Any], logger: logging.Logger, browser, job_output_dir: str = None) -> Dict[str, Any]:
        """Main entry point for Airbnb home scraping."""
        try:
            # Extract nested params if present (API sends {"params": {...}})
            actual_params = params.get("params", params)
            
            # Validate and normalize parameters
            clean_params = AirbnbTask._validate_params(actual_params)
            
            # Determine scraping level  
            scrape_level = clean_params.get("level") or clean_params.get("scrape_level") or actual_params.get("level", actual_params.get("scrape_level", 2))
            deep_scrape = actual_params.get("deep_scrape", False)
            
            # Legacy deep_scrape mapping
            if deep_scrape and scrape_level == 2:
                scrape_level = 3
                
            logger.info(f"🚀 AIRBNB HOMES SCRAPER v1.0 - PRODUCTION CLEAN")
            logger.info(f"📍 Location: {clean_params['location']}")
            logger.info(f"📅 Dates: {clean_params['check_in']} to {clean_params['check_out']}")
            logger.info(f"📊 Scrape Level: {scrape_level}")
            
            # Level descriptions
            level_descriptions = {
                1: "Quick Search - Essential data only",
                2: "Full Data - Complete property details", 
                3: "Basic Reviews - Level 2 + review sampling",
                4: "Deep Reviews - Level 2 + comprehensive reviews"
            }
            
            logger.info(f"🎯 {level_descriptions.get(scrape_level, 'Unknown level')}")
            
            # Create scraper instance
            scraper = AirbnbScraper(browser, logger)
            
            # Execute based on level
            if scrape_level >= 4:
                homes = await scraper.scrape_level_4(clean_params)
                extraction_method = "level_4_deep_reviews"
            elif scrape_level >= 3:
                homes = await scraper.scrape_level_3(clean_params)
                extraction_method = "level_3_basic_reviews"
            elif scrape_level >= 2:
                homes = await scraper.scrape_level_2(clean_params)
                extraction_method = "level_2_full_data"
            else:
                homes = await scraper.scrape_level_1(clean_params)
                extraction_method = "level_1_quick_search"
            
            # Apply filters
            homes = AirbnbTask._apply_filters(homes, params, logger)
            
            # Calculate metrics
            homes_with_prices = [h for h in homes if h.get('price_per_night', 0) > 0]
            success_rate = len(homes_with_prices) / len(homes) if homes else 0
            avg_price = sum(h.get('price_per_night', 0) for h in homes_with_prices) / len(homes_with_prices) if homes_with_prices else 0
            avg_completeness = sum(h.get('extraction_level', 0) * 25 for h in homes) / len(homes) if homes else 0
            
            logger.info(f"🏁 Completed: {len(homes)} properties | {success_rate:.1%} with prices | {avg_completeness:.1f}% complete")
            
            result = {
                "search_metadata": {
                    "location": clean_params["location"],
                    "check_in": clean_params["check_in"],
                    "check_out": clean_params["check_out"],
                    "nights": clean_params["nights"],
                    "extraction_method": extraction_method,
                    "scrape_level": scrape_level,
                    "total_found": len(homes),
                    "success_rate": success_rate,
                    "average_price": avg_price,
                    "average_completeness": avg_completeness,
                    "search_completed_at": datetime.now().isoformat()
                },
                "properties": homes
            }
            
            # Save output if requested
            if job_output_dir and homes:
                AirbnbTask._save_output(result, job_output_dir, logger)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Critical error: {e}", exc_info=True)
            return {"search_metadata": {"error": str(e)}, "properties": []}

    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize input parameters."""
        location = params.get("location", "New York")
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
            "children": params.get("children", 0),
            "rooms": params.get("rooms", 1),  # Added rooms parameter
            "property_type": params.get("property_type"),  # apartment, house, etc.
            "currency": params.get("currency", "USD"),  # Added currency parameter
            "level": params.get("params", {}).get("level") or params.get("level") or params.get("scrape_level", 2)
        }

    @staticmethod
    def _apply_filters(homes: List[Dict[str, Any]], params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Apply post-processing filters to properties."""
        original_count = len(homes)
        
        # Price filters
        min_price = params.get("min_price")
        max_price = params.get("max_price")
        min_rating = params.get("min_rating")
        property_type = params.get("property_type")
        
        if min_price:
            homes = [h for h in homes if h.get('price_per_night', 0) >= min_price]
        if max_price:
            homes = [h for h in homes if h.get('price_per_night', 0) <= max_price]
        if min_rating:
            homes = [h for h in homes if h.get('rating', 0) >= min_rating]
        if property_type:
            homes = [h for h in homes if property_type.lower() in h.get('property_type', '').lower()]
        
        if len(homes) != original_count:
            logger.info(f"🔍 Applied filters: {original_count} → {len(homes)} properties")
        
        return homes

    @staticmethod
    def _save_output(result: Dict[str, Any], job_output_dir: str, logger: logging.Logger):
        """Save scraping results to file."""
        import os
        try:
            output_file = os.path.join(job_output_dir, "airbnb_data.json")
            os.makedirs(job_output_dir, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Saved data to {output_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save output: {e}")


class AirbnbScraper:
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
            
            # Extract basic property data from search results
            homes = await self._extract_search_results(page, params["max_results"])
            
            self.logger.info(f"✅ Level 1: Extracted {len(homes)} properties from search results")
            return homes
            
        finally:
            await context.close()

    async def scrape_level_2(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 2: Full data - complete property details."""
        self.logger.info("🏠 Level 2: Full data extraction")
        
        # Start with Level 1 data
        homes = await self.scrape_level_1(params)
        
        # Enhance with detailed data from individual property pages
        context = await self.browser.new_context()
        try:
            for i, home in enumerate(homes):
                self.logger.info(f"📍 Processing property {i+1}/{len(homes)}: {home.get('title', 'Unknown')}")
                
                try:
                    page = await context.new_page()
                    await page.goto(home['airbnb_url'], wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    
                    # Extract detailed data
                    detailed_data = await self._extract_property_details(page)
                    home.update(detailed_data)
                    home['extraction_level'] = 2
                    
                    await page.close()
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to get details for property {i+1}: {e}")
                    home['extraction_level'] = 1
                    
                # Mark extraction level
                home['extraction_level'] = 2
                    
            self.logger.info(f"✅ Level 2: Enhanced {len(homes)} properties with detailed data")
            return homes
            
        finally:
            await context.close()

    async def scrape_level_3(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 3: Basic reviews - Level 2 + review sampling."""
        self.logger.info("📝 Level 3: Basic reviews extraction")
        
        # Start with Level 2 data
        homes = await self.scrape_level_2(params)
        
        # Add basic reviews
        context = await self.browser.new_context()
        try:
            for i, home in enumerate(homes):
                self.logger.info(f"📝 Extracting reviews for property {i+1}/{len(homes)}")
                
                try:
                    page = await context.new_page()
                    await page.goto(home['airbnb_url'], wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    
                    # Extract basic reviews (2-5 reviews)
                    reviews_data = await self._extract_basic_reviews(page)
                    if reviews_data:
                        home.update(reviews_data)
                    
                    home['extraction_level'] = 3
                    home['reviews_extraction_target'] = "2-5 basic reviews"
                    
                    await page.close()
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to get reviews for property {i+1}: {e}")
                    home['extraction_level'] = 2
                    
            self.logger.info(f"✅ Level 3: Added reviews to {len(homes)} properties")
            return homes
            
        finally:
            await context.close()

    async def scrape_level_4(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Level 4: Deep reviews - comprehensive review extraction."""
        self.logger.info("🔥 Level 4: Deep reviews extraction")
        
        # Start with Level 2 data (skip Level 3 to avoid duplicate processing)
        homes = await self.scrape_level_2(params)
        
        # Add comprehensive reviews
        context = await self.browser.new_context()
        try:
            for i, home in enumerate(homes):
                self.logger.info(f"🔥 Deep review extraction for property {i+1}/{len(homes)}")
                
                try:
                    page = await context.new_page()
                    await page.goto(home['airbnb_url'], wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000)
                    
                    # Extract comprehensive reviews with pagination
                    reviews_data = await self._extract_comprehensive_reviews(page)
                    if reviews_data:
                        home.update(reviews_data)
                    
                    home['extraction_level'] = 4
                    home['reviews_extraction_target'] = "10-50 comprehensive reviews"
                    
                    await page.close()
                    
                except Exception as e:
                    import traceback
                    self.logger.error(f"❌ Failed deep review extraction for property {i+1}: {e}")
                    self.logger.error(f"❌ Full traceback: {traceback.format_exc()}")
                    home['extraction_level'] = 2
                    
            self.logger.info(f"✅ Level 4: Added comprehensive reviews to {len(homes)} properties")
            return homes
            
        finally:
            await context.close()

    async def _perform_search(self, page, params: Dict[str, Any]):
        """Perform property search on Airbnb."""
        location = params["location"]
        check_in = params["check_in"]
        check_out = params["check_out"]
        adults = params["adults"]
        children = params["children"]
        
        # Build search URL
        search_url = (
            f"{AirbnbTask.BASE_URL}/s/{quote(location)}/homes"
            f"?checkin={check_in}"
            f"&checkout={check_out}"
            f"&adults={adults}"
        )
        
        if children > 0:
            search_url += f"&children={children}"
        
        self.logger.info(f"🔍 Search URL: {search_url}")
        
        await page.goto(search_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)  # Airbnb loads slower than Booking
        
        # Handle popups and overlays
        await self._handle_popups(page)
        
        # Apply search filters if specified
        await self._apply_search_filters(page, params)

    async def _extract_search_results(self, page, max_results: int) -> List[Dict[str, Any]]:
        """Extract property data from search results page."""
        properties = []
        
        # Property card selectors (Airbnb-specific)
        card_selectors = [
            "[data-testid='card-container']",
            "[data-testid='listing-card']",
            "[data-testid='listing-card-v2']",
            "[class*='listing-card']",
            "[role='group'][aria-describedby]"
        ]
        
        cards = None
        for selector in card_selectors:
            try:
                test_cards = page.locator(selector)
                count = await test_cards.count()
                if count > 0:
                    cards = test_cards
                    self.logger.info(f"✅ Found {count} properties with selector: {selector}")
                    break
            except:
                continue
        
        if not cards:
            self.logger.warning("❌ No property cards found")
            return []
        
        # Extract data from each card
        card_count = min(await cards.count(), max_results)
        
        
        for i in range(card_count):
            try:
                card = cards.nth(i)
                property_data = await self._extract_basic_property_data(card, i)
                
                if property_data:
                    property_data['extraction_level'] = 1
                    properties.append(property_data)
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to extract property {i+1}: {e}")
                continue
        
        
        return properties

    async def _extract_basic_property_data(self, card, index: int) -> Optional[Dict[str, Any]]:
        """Extract basic property data from listing card."""
        try:
            property_data = {}
            
            # Property title
            title_selectors = [
                "[data-testid='listing-card-title']",
                "[data-testid='listing-card-name']",
                "h3",
                "[class*='title']",
                "a span"
            ]
            
            for selector in title_selectors:
                try:
                    title_element = card.locator(selector).first
                    if await title_element.is_visible():
                        property_data['title'] = (await title_element.inner_text()).strip()
                        break
                except:
                    continue
            
            # Property URL
            try:
                link_element = card.locator("a").first
                if await link_element.is_visible():
                    relative_url = await link_element.get_attribute("href")
                    if relative_url:
                        if relative_url.startswith('/'):
                            property_data['airbnb_url'] = f"{AirbnbTask.BASE_URL}{relative_url}"
                        else:
                            property_data['airbnb_url'] = relative_url
            except:
                pass
            
            # Enhanced price extraction with more selectors
            price_selectors = [
                "[data-testid='price-availability']",
                "[data-testid*='price']",
                "[class*='price']",
                "span:has-text('$')",
                "*:has-text('night')",
                "*:has-text('per night')",
                "[aria-label*='price']",
                "div:has-text('$') span",
                "*[class*='pricing']"
            ]
            
            # Try multiple approaches for price extraction
            price_found = False
            for selector in price_selectors:
                if price_found:
                    break
                try:
                    price_elements = card.locator(selector)
                    count = await price_elements.count()
                    
                    for j in range(min(count, 3)):  # Check multiple price elements
                        try:
                            price_element = price_elements.nth(j)
                            if await price_element.is_visible():
                                price_text = await price_element.inner_text()
                                if price_text and ('$' in price_text or 'night' in price_text.lower()):
                                    price_value = self._extract_price_number(price_text)
                                    if price_value and price_value > 0:
                                        property_data['price_per_night'] = price_value
                                        price_found = True
                                        break
                        except:
                            continue
                except:
                    continue
            
            # Fallback: search all text content for price patterns
            if not price_found:
                try:
                    card_text = await card.inner_text()
                    price_value = self._extract_price_number(card_text)
                    if price_value and price_value > 0:
                        property_data['price_per_night'] = price_value
                except:
                    pass
            
            # Enhanced rating extraction
            rating_selectors = [
                "[data-testid='listing-card-rating']",
                "[data-testid*='rating']",
                "[aria-label*='rating']", 
                "*:has-text('★')",
                "*:has-text('⭐')",
                ".rating",
                "[class*='rating']",
                "span:has-text('.')"  # Ratings often look like "4.9"
            ]
            
            rating_found = False
            for selector in rating_selectors:
                if rating_found:
                    break
                try:
                    rating_elements = card.locator(selector)
                    count = await rating_elements.count()
                    
                    for j in range(min(count, 3)):
                        try:
                            rating_element = rating_elements.nth(j)
                            if await rating_element.is_visible():
                                rating_text = await rating_element.inner_text()
                                if rating_text and ('★' in rating_text or '⭐' in rating_text or '.' in rating_text):
                                    rating_value = self._extract_rating_number(rating_text)
                                    if rating_value and 0 < rating_value <= 5:
                                        property_data['rating'] = rating_value
                                        rating_found = True
                                        break
                        except:
                            continue
                except:
                    continue
                    
            # Fallback: search card text for rating patterns
            if not rating_found:
                try:
                    card_text = await card.inner_text()
                    rating_value = self._extract_rating_number(card_text)
                    if rating_value and 0 < rating_value <= 5:
                        property_data['rating'] = rating_value
                except:
                    pass
            
            # Review count extraction
            review_selectors = [
                "[data-testid*='review']", 
                "*:has-text('review')",
                "*:has-text('Review')",
                "[aria-label*='review']"
            ]
            
            for selector in review_selectors:
                try:
                    review_elements = card.locator(selector)
                    count = await review_elements.count()
                    
                    for j in range(min(count, 2)):
                        try:
                            review_element = review_elements.nth(j)
                            if await review_element.is_visible():
                                review_text = await review_element.inner_text()
                                if 'review' in review_text.lower():
                                    review_count = self._extract_review_count(review_text)
                                    if review_count and review_count > 0:
                                        property_data['review_count'] = review_count
                                        break
                        except:
                            continue
                    if property_data.get('review_count'):
                        break
                except:
                    continue
            
            # Property type (entire place, private room, etc.)
            type_selectors = [
                "[data-testid='listing-card-subtitle']",
                "[class*='subtitle']",
                "span"
            ]
            
            for selector in type_selectors:
                try:
                    type_element = card.locator(selector).first
                    if await type_element.is_visible():
                        type_text = await type_element.inner_text()
                        if any(word in type_text.lower() for word in ['entire', 'private', 'shared', 'hotel']):
                            property_data['property_type'] = type_text.strip()
                            break
                except:
                    continue
            
            # Images
            try:
                img_element = card.locator("img").first
                if await img_element.is_visible():
                    img_src = await img_element.get_attribute("src")
                    if img_src:
                        property_data['main_image'] = self._fix_image_url(img_src)
            except:
                pass
            
            # Generate property ID
            if property_data.get('airbnb_url'):
                property_data['property_id'] = self._extract_property_id_from_url(property_data['airbnb_url'])
            
            # Set defaults
            property_data.setdefault('price_per_night', 0)
            property_data['scraping_timestamp'] = datetime.now().isoformat()
            property_data['source'] = "search_results"
            
            return property_data if property_data.get('title') else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to extract basic data for card {index}: {e}")
            return None

    async def _extract_property_details(self, page) -> Dict[str, Any]:
        """Extract detailed property information from property page."""
        details = {}
        
        try:
            # Host information
            host_data = await self._extract_host_info(page)
            if host_data:
                details.update(host_data)
            
            # Amenities
            details['amenities'] = await self._extract_amenities(page)
            
            # Images gallery
            details['images'] = await self._extract_images(page)
            
            # Location data
            location_data = await self._extract_location_data(page)
            if location_data:
                details.update(location_data)
            
            # Description
            description = await self._extract_description(page)
            if description:
                details['description'] = description
            
            # Property specifications (bedrooms, bathrooms, etc.)
            specs = await self._extract_property_specs(page)
            if specs:
                details.update(specs)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting property details: {e}")
        
        return details

    async def _extract_host_info(self, page) -> Optional[Dict[str, Any]]:
        """Extract host information."""
        try:
            host_info = {}
            
            # Comprehensive host name selectors - extract everything possible
            host_selectors = [
                # Direct host elements
                "[data-testid='host-name']",
                "[data-testid='host-profile-name']",
                "[data-testid='host-profile'] h2",
                "[data-testid='host-profile'] h3",
                
                # "Hosted by" text patterns
                "h1:has-text('Hosted by')",
                "h2:has-text('Hosted by')",
                "h3:has-text('Hosted by')",
                "h4:has-text('Hosted by')",
                "div:has-text('Hosted by')",
                "span:has-text('Hosted by')",
                "p:has-text('Hosted by')",
                
                # Host sections and profiles
                "[class*='host'] h1",
                "[class*='host'] h2",
                "[class*='host'] h3",
                "[class*='host-name']",
                "[class*='host-profile'] h2",
                
                # Host profile areas
                "*:has-text('Meet your host') h2", 
                "*:has-text('Meet your host') h3",
                "*:has-text('About the host') h2",
                "*:has-text('Your host') h2",
                
                # Superhost indicators
                "*:has-text('Superhost') h2",
                "*:has-text('Superhost') h3",
                "div:has-text('Superhost') + h2",
                "span:has-text('Superhost')"
            ]
            
            for selector in host_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        text = await element.inner_text()
                        text = text.strip()
                        
                        # Clean extraction - avoid HTML dumps
                        if "hosted by" in text.lower():
                            # Extract name after "Hosted by"
                            host_name = text.lower().replace("hosted by", "").strip()
                            # Clean common suffixes and prefixes
                            host_name = re.sub(r'\s*(superhost|verified|host since|years hosting|months hosting).*$', '', host_name, flags=re.IGNORECASE)
                            host_name = re.sub(r'^\s*(host|by)\s+', '', host_name, flags=re.IGNORECASE)
                            if host_name and len(host_name) < 50:
                                host_info['host_name'] = host_name.title()
                        elif (text and len(text) < 50 and len(text) > 2 and 
                              not any(word in text.lower() for word in ['superhost', 'year', 'hosting', 'verified', 'show', 'more', 'reviews', 'rating'])):
                            # Only accept clean, short names without common UI text
                            clean_name = re.sub(r'[^\w\s-]', '', text)  # Remove special chars except hyphens
                            if len(clean_name.split()) <= 3:  # Max 3 words for a name
                                host_info['host_name'] = clean_name.strip()
                        
                        if host_info.get('host_name'):
                            break
                except:
                    continue
            
            # Host avatar - enhanced selectors
            avatar_selectors = [
                "[data-testid='host-avatar'] img",
                "[class*='host'] img",
                "img[alt*='host']",
                "*:has-text('Hosted by') img"
            ]
            
            for selector in avatar_selectors:
                try:
                    avatar = page.locator(selector).first
                    if await avatar.is_visible():
                        src = await avatar.get_attribute("src")
                        if (src and 
                            ('airbnb' in src or src.startswith('http')) and
                            not any(exclude in src.lower() for exclude in ['icon-', 'assets/frontend', 'logo']) and
                            len(src) > 30):  # Reasonable URL length
                            host_info['host_avatar'] = self._fix_image_url(src)
                            self.logger.info(f"✅ Found host avatar: {src[:100]}...")
                            break
                except:
                    continue
            
            # Extract additional host information - response rate, verification, etc.
            await self._extract_extended_host_info(page, host_info)
            
            return host_info if host_info else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting host info: {e}")
            return None

    async def _extract_extended_host_info(self, page, host_info: Dict[str, Any]):
        """Extract extended host information - response rates, verification, etc."""
        try:
            # Extract response rate
            response_rate_selectors = [
                "*:has-text('Response rate') + *",
                "*:has-text('Response rate:') + *", 
                "*:has-text('Response rate')",
                "*:contains('Response rate')",
                "span:has-text('%'):not(:has-text('rating'))",
                "div:has-text('response') span:has-text('%')"
            ]
            
            for selector in response_rate_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        text = await element.inner_text()
                        # Extract percentage from text
                        import re
                        percent_match = re.search(r'(\d+)%', text)
                        if percent_match:
                            host_info['host_response_rate'] = f"{percent_match.group(1)}%"
                            self.logger.info(f"✅ Found host response rate: {host_info['host_response_rate']}")
                            break
                except:
                    continue
            
            # Extract response time
            response_time_selectors = [
                "*:has-text('Response time') + *",
                "*:has-text('Response time:') + *",
                "*:has-text('Response time')",
                "*:contains('Response time')",
                "*:has-text('within')",
                "div:has-text('response') span:not(:has-text('%'))"
            ]
            
            for selector in response_time_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        text = await element.inner_text()
                        # Look for time phrases
                        time_patterns = [
                            r'within\s+(\w+\s+\w+)',
                            r'(\d+\s+\w+)',
                            r'(few\s+\w+)',
                            r'(an?\s+\w+)'
                        ]
                        for pattern in time_patterns:
                            time_match = re.search(pattern, text.lower())
                            if time_match and not any(exclude in time_match.group(1) for exclude in ['rate', '%', 'rating']):
                                host_info['host_response_time'] = time_match.group(1)
                                self.logger.info(f"✅ Found host response time: {host_info['host_response_time']}")
                                break
                        if 'host_response_time' in host_info:
                            break
                except:
                    continue
            
            # Extract verification status and superhost badge
            verification_selectors = [
                "*:has-text('Superhost')",
                "span:has-text('Superhost')",
                "div:has-text('Superhost')",
                "*:has-text('Identity verified')",
                "*:has-text('verified')",
                "svg + *:has-text('Superhost')",
                "[class*='badge']",
                "[class*='superhost']"
            ]
            
            for selector in verification_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        text = await element.inner_text().lower()
                        if 'superhost' in text:
                            host_info['host_is_superhost'] = True
                            self.logger.info("✅ Host is a Superhost")
                        if 'verified' in text:
                            host_info['host_verified'] = True
                            self.logger.info("✅ Host identity verified")
                except:
                    continue
            
            # Extract hosting experience (years hosting)
            experience_selectors = [
                "*:has-text('year') + *:has-text('hosting')",
                "*:has-text('years hosting')",
                "*:has-text('hosting for')",
                "*:has-text('Host since')",
                "*:contains('year') *:contains('hosting')"
            ]
            
            for selector in experience_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        text = await element.inner_text()
                        # Extract years
                        year_patterns = [
                            r'(\d+)\s+year',
                            r'hosting\s+for\s+(\d+)',
                            r'since\s+(\d{4})'
                        ]
                        for pattern in year_patterns:
                            year_match = re.search(pattern, text.lower())
                            if year_match:
                                if 'since' in pattern:
                                    # Calculate years from year
                                    from datetime import datetime
                                    years_hosting = datetime.now().year - int(year_match.group(1))
                                    host_info['host_years_hosting'] = years_hosting
                                else:
                                    host_info['host_years_hosting'] = int(year_match.group(1))
                                self.logger.info(f"✅ Found host experience: {host_info['host_years_hosting']} years")
                                break
                        if 'host_years_hosting' in host_info:
                            break
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting extended host info: {e}")

    async def _extract_property_specs(self, page) -> Optional[Dict[str, Any]]:
        """Extract property specifications (beds, baths, etc.)."""
        try:
            specs = {}
            
            # Enhanced specification elements selectors
            spec_selectors = [
                "[data-testid='property-details']",
                "[class*='details']", 
                "li:has-text('bedroom')",
                "li:has-text('bathroom')",
                "li:has-text('bath')",
                "li:has-text('bed')",
                "span:has-text('bedroom')",
                "span:has-text('bathroom')",
                "span:has-text('bath')",
                "span:has-text('bed')",
                "div:has-text('guest')",
                "*:has-text(' · ')",  # Property specs often separated by dots
                "h1 + div",  # Specs often right after title
                "h2 + div"
            ]
            
            for selector in spec_selectors:
                try:
                    elements = page.locator(selector)
                    count = await elements.count()
                    
                    for i in range(min(count, 10)):
                        text = await elements.nth(i).inner_text()
                        text = text.lower()
                        
                        # Extract bedroom count - enhanced patterns
                        if 'bedroom' in text:
                            bedroom_patterns = [
                                r'(\d+)\s*bedroom',
                                r'(\d+)\s*bed\s*room'
                            ]
                            for pattern in bedroom_patterns:
                                bedroom_match = re.search(pattern, text, re.IGNORECASE)
                                if bedroom_match:
                                    specs['bedrooms'] = int(bedroom_match.group(1))
                                    break
                        
                        # Extract guest count for studios/efficiency apartments
                        if 'guest' in text and 'bedrooms' not in specs:
                            guest_match = re.search(r'(\d+)\s*guest', text, re.IGNORECASE)
                            if guest_match:
                                guest_count = int(guest_match.group(1))
                                # For studios with guests but no bedrooms specified
                                if 'studio' in text.lower() and guest_count <= 4:
                                    specs['bedrooms'] = 0  # Studio = 0 bedrooms
                                    specs['beds'] = 1
                        
                        # Extract bathroom count - enhanced patterns
                        if 'bathroom' in text or 'bath' in text:
                            bathroom_patterns = [
                                r'(\d+(?:\.\d+)?)\s*bathroom',
                                r'(\d+(?:\.\d+)?)\s*bath',
                                r'(\d+(?:\.\d+)?)\s*private\s*bath',
                                r'(\d+(?:\.\d+)?)\s*shared\s*bath'
                            ]
                            for pattern in bathroom_patterns:
                                bathroom_match = re.search(pattern, text, re.IGNORECASE)
                                if bathroom_match:
                                    # Convert to integer, rounding up fractional bathrooms
                                    bathroom_count = float(bathroom_match.group(1))
                                    specs['bathrooms'] = int(round(bathroom_count))
                                    break
                        
                        # Extract bed count - enhanced patterns
                        if 'bed' in text and 'bedroom' not in text:
                            bed_patterns = [
                                r'(\d+)\s*bed',
                                r'(\d+)\s*king\s*bed',
                                r'(\d+)\s*queen\s*bed',
                                r'(\d+)\s*double\s*bed'
                            ]
                            for pattern in bed_patterns:
                                bed_match = re.search(pattern, text, re.IGNORECASE)
                                if bed_match:
                                    specs['beds'] = int(bed_match.group(1))
                                    break
                                
                except:
                    continue
            
            # Extract property type from various sources
            property_type = await self._extract_property_type(page)
            if property_type:
                specs['property_type'] = property_type
            
            # Log what we found
            if specs:
                self.logger.info(f"✅ Found property specs: {specs}")
            
            return specs if specs else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting property specs: {e}")
            return None

    async def _extract_property_type(self, page) -> Optional[str]:
        """Extract property type (apartment, house, condo, etc.)."""
        try:
            property_type_selectors = [
                # Primary property type indicators
                "[data-testid='property-type']",
                "h1 + div div:first-child", # Property type often right after title
                "h2 + div div:first-child",
                
                # Text patterns containing property types
                "*:has-text('Entire apartment')",
                "*:has-text('Entire house')",
                "*:has-text('Entire condo')",
                "*:has-text('Entire studio')",
                "*:has-text('Entire villa')",
                "*:has-text('Entire cabin')",
                "*:has-text('Entire townhouse')",
                "*:has-text('Entire loft')",
                "*:has-text('Private room')",
                "*:has-text('Shared room')",
                
                # Property overview sections
                "*:has-text('Property type')",
                "*:contains('Property type')",
                "div:has-text('hosted by') div",
                
                # Generic property info selectors
                "h1 + div span",
                "h1 + p",
                "[class*='property'] [class*='type']",
                "[class*='listing'] [class*='type']"
            ]
            
            property_types_map = {
                'apartment': 'Apartment',
                'house': 'House', 
                'condo': 'Condo',
                'condominium': 'Condo',
                'studio': 'Studio',
                'villa': 'Villa',
                'cabin': 'Cabin',
                'cottage': 'Cottage',
                'townhouse': 'Townhouse',
                'loft': 'Loft',
                'duplex': 'Duplex',
                'bungalow': 'Bungalow',
                'chalet': 'Chalet',
                'mansion': 'Mansion',
                'penthouse': 'Penthouse',
                'room': 'Private Room',
                'shared': 'Shared Room',
                'hotel': 'Hotel Room',
                'guesthouse': 'Guesthouse',
                'bed and breakfast': 'Bed & Breakfast',
                'hostel': 'Hostel'
            }
            
            for selector in property_type_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        text = await element.inner_text()
                        text_lower = text.lower()
                        
                        # Check for property type keywords
                        for keyword, property_type in property_types_map.items():
                            if keyword in text_lower:
                                self.logger.info(f"✅ Found property type: {property_type}")
                                return property_type
                                
                except:
                    continue
            
            # Try extracting from breadcrumb or title context
            try:
                title_elements = page.locator("h1, h2").first
                if await title_elements.is_visible():
                    title_text = await title_elements.inner_text()
                    title_lower = title_text.lower()
                    
                    for keyword, property_type in property_types_map.items():
                        if keyword in title_lower:
                            self.logger.info(f"✅ Found property type from title: {property_type}")
                            return property_type
            except:
                pass
                        
            return None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting property type: {e}")
            return None

    async def _apply_search_filters(self, page, params: Dict[str, Any]):
        """Apply search filters on Airbnb search page."""
        try:
            self.logger.info("🔧 Applying search filters")
            
            # Wait for page to load
            await page.wait_for_timeout(2000)
            
            # Handle common popups first
            await self._handle_popups(page)
            
            # Apply price filters if specified
            min_price = params.get("min_price")
            max_price = params.get("max_price")
            
            if min_price or max_price:
                await self._apply_price_filters(page, min_price, max_price)
            
            # Apply rating filter if specified
            min_rating = params.get("min_rating")
            if min_rating:
                await self._apply_rating_filter(page, min_rating)
            
            # Wait for filters to apply
            await page.wait_for_timeout(3000)
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error applying search filters: {e}")
            # Continue without filters rather than failing completely
    
    async def _apply_price_filters(self, page, min_price: Optional[int], max_price: Optional[int]):
        """Apply price range filters."""
        try:
            # Look for price filter button or menu
            price_filter_selectors = [
                "button:has-text('Price')",
                "[data-testid*='price']",
                "button:has-text('Filters')"
            ]
            
            for selector in price_filter_selectors:
                try:
                    filter_button = page.locator(selector).first
                    if await filter_button.is_visible():
                        await filter_button.click()
                        await page.wait_for_timeout(1000)
                        break
                except:
                    continue
            
            # Try to set min/max price inputs
            if min_price:
                min_price_selectors = [
                    "input[data-testid*='price-min']",
                    "input[placeholder*='min' i]",
                    "input[name*='price_min']"
                ]
                for selector in min_price_selectors:
                    try:
                        min_input = page.locator(selector).first
                        if await min_input.is_visible():
                            await min_input.fill(str(min_price))
                            break
                    except:
                        continue
            
            if max_price:
                max_price_selectors = [
                    "input[data-testid*='price-max']", 
                    "input[placeholder*='max' i]",
                    "input[name*='price_max']"
                ]
                for selector in max_price_selectors:
                    try:
                        max_input = page.locator(selector).first
                        if await max_input.is_visible():
                            await max_input.fill(str(max_price))
                            break
                    except:
                        continue
                        
        except Exception as e:
            self.logger.debug(f"Price filter application failed: {e}")
    
    async def _apply_rating_filter(self, page, min_rating: float):
        """Apply minimum rating filter."""
        try:
            # Look for rating filter options
            rating_selectors = [
                f"button:has-text('{min_rating}+')",
                f"[data-testid*='rating'] button:has-text('{min_rating}')",
                "button:has-text('Guest rating')"
            ]
            
            for selector in rating_selectors:
                try:
                    rating_button = page.locator(selector).first
                    if await rating_button.is_visible():
                        await rating_button.click()
                        await page.wait_for_timeout(1000)
                        break
                except:
                    continue
                    
        except Exception as e:
            self.logger.debug(f"Rating filter application failed: {e}")

    async def _handle_popups(self, page):
        """Handle common Airbnb popups and overlays."""
        try:
            popup_selectors = [
                "button:has-text('Accept')",
                "button:has-text('OK')",
                "button:has-text('Close')",
                "[data-testid='close-dialog']",
                "[aria-label='Close']"
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
            text = re.sub(r'From\s+|per\s+night|\$|€|£|total|night', '', price_text, flags=re.IGNORECASE)
            
            # Extract numeric value
            numbers = re.findall(r'[\d,]+\.?\d*', text)
            if numbers:
                price_str = numbers[0].replace(',', '')
                price = float(price_str)
                
                # Validate price range (10-10000)
                if 10 <= price <= 10000:
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
                # Validate rating range (1-5 for Airbnb)
                if 1 <= rating <= 5:
                    return rating
        except (ValueError, IndexError):
            pass
        
        return None

    def _extract_review_count(self, review_text: str) -> Optional[int]:
        """Extract numeric review count from text."""
        if not review_text:
            return None
        
        try:
            # Look for numbers followed by "review" (case insensitive)
            patterns = [
                r'(\d+)\s*review',
                r'(\d+)\s*Review',
                r'(\d+)\s*REVIEW',
                r'(\d+)\s*\(\s*review',  # Sometimes in parentheses
                r'\(\s*(\d+)\s*\)',      # Just parentheses with number
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, review_text, re.IGNORECASE)
                if matches:
                    count = int(matches[0])
                    # Reasonable review count validation
                    if 0 <= count <= 10000:  # Max 10k reviews seems reasonable
                        return count
                        
        except (ValueError, IndexError):
            pass
        
        return None

    def _extract_property_id_from_url(self, url: str) -> Optional[str]:
        """Generate unique property ID from Airbnb URL."""
        if not url:
            return None
        
        try:
            # Extract property identifier from URL
            parsed = urlparse(url)
            # Airbnb URLs typically have /rooms/{id} format
            path_parts = parsed.path.split('/')
            
            for part in path_parts:
                if part and part.isdigit() and len(part) > 5:
                    return part
                    
            # Fallback: hash the entire URL
            return hashlib.md5(url.encode()).hexdigest()[:8]
            
        except:
            return None

    def _fix_image_url(self, url: str) -> str:
        """Fix and normalize image URL to get original size."""
        if not url:
            return ""
        
        # Ensure HTTPS
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('http:'):
            url = url.replace('http:', 'https:')
            
        # Remove size restrictions to get original/high quality images
        # Common Airbnb image URL patterns that limit quality
        url = re.sub(r'[?&]w=\d+', '', url)  # Remove width restrictions
        url = re.sub(r'[?&]h=\d+', '', url)  # Remove height restrictions
        url = re.sub(r'[?&]s=\d+', '', url)  # Remove size restrictions
        url = re.sub(r'[?&]fit=\w+', '', url)  # Remove fit restrictions
        url = re.sub(r'[?&]crop=\w+', '', url)  # Remove crop restrictions
        url = re.sub(r'[?&]auto=\w+', '', url)  # Remove auto optimizations
        url = re.sub(r'[?&]fm=\w+', '', url)  # Remove format restrictions
        url = re.sub(r'[?&]q=\d+', '', url)  # Remove quality restrictions
        url = re.sub(r'[?&]dpr=[\d.]+', '', url)  # Remove DPR restrictions
        
        # Clean up any leftover URL artifacts
        url = re.sub(r'[?&]$', '', url)  # Remove trailing ? or &
        url = re.sub(r'&+', '&', url)  # Remove multiple &
        url = re.sub(r'\?&', '?', url)  # Fix ?& pattern
        
        return url

    async def _extract_amenities(self, page) -> List[str]:
        """Extract property amenities from detail page."""
        amenities = []
        
        try:
            # Comprehensive amenities extraction - target everything on the page
            amenities_selectors = [
                # Primary amenities sections
                "[data-testid='amenity-section']",
                "[data-section-id='AMENITIES_DEFAULT']",
                "section:has-text('What this place offers')",
                "div:has-text('What this place offers')",
                "h2:has-text('What this place offers') + div",
                "h3:has-text('What this place offers') + div",
                
                # Structured amenities lists
                "ul li[data-testid*='amenity']",
                "div[data-testid*='amenity']",
                "*[class*='amenities'] li",
                "*[class*='amenity-item']",
                
                # General amenities containers
                "section:has-text('amenities') li",
                "div:has-text('amenities') li",
                "ul:has(li:has-text('Wifi')) li",
                "ul:has(li:has-text('Kitchen')) li",
                
                # Fallback: any list with common amenities
                "li:has-text('Wifi')",
                "li:has-text('Kitchen')", 
                "li:has-text('Air conditioning')",
                "li:has-text('Heating')",
                "li:has-text('TV')",
                "li:has-text('Parking')",
                "li:has-text('Pool')",
                "li:has-text('Gym')",
                "li:has-text('Washer')",
                "li:has-text('Dryer')"
            ]
            
            for selector in amenities_selectors:
                try:
                    elements = page.locator(selector)
                    count = await elements.count()
                    
                    for i in range(min(count, 30)):  # Limit to 30 amenities
                        text = await elements.nth(i).inner_text()
                        text = text.strip()
                        
                        # Comprehensive amenities filtering - extract everything valuable
                        if (text and len(text) >= 2 and len(text) <= 100 and 
                            not any(exclude in text.lower() for exclude in [
                                'rare find', 'usually booked', 'show all', 'see all', 'more amenities',
                                'click here', 'tap to', 'book now', 'reserve now', 'per night', 
                                'total cost', 'check availability', 'guests say', 'recent guests',
                                'view all', 'hide', 'close', 'next', 'previous', 'page', 'loading'
                            ]) and
                            # Accept any text that contains common amenity keywords
                            (any(amenity in text.lower() for amenity in [
                                'wifi', 'wi-fi', 'internet', 'kitchen', 'air conditioning', 'heating', 'ac',
                                'tv', 'television', 'cable', 'parking', 'garage', 'pool', 'swimming', 'gym', 'fitness', 
                                'washer', 'dryer', 'laundry', 'balcony', 'patio', 'terrace', 'garden', 'yard',
                                'fireplace', 'hot tub', 'jacuzzi', 'spa', 'sauna', 'bbq', 'grill', 'barbecue',
                                'elevator', 'lift', 'wheelchair', 'accessible', 'disability', 'smoke detector', 
                                'carbon monoxide', 'first aid', 'fire extinguisher', 'workspace', 'desk', 'office',
                                'refrigerator', 'fridge', 'freezer', 'microwave', 'oven', 'stove', 'dishwasher',
                                'hair dryer', 'shampoo', 'body wash', 'towels', 'linens', 'iron', 'ironing board',
                                'coffee', 'tea', 'breakfast', 'kitchen basics', 'cooking', 'dishes', 'utensils',
                                'beach', 'ocean', 'lake', 'pets allowed', 'pet friendly', 'dog', 'cat',
                                'safe', 'security', 'keypad', 'lockbox', 'self check-in', 'doorman', 'concierge',
                                'bathtub', 'shower', 'bathroom', 'toilet', 'bidet', 'bed', 'mattress', 'pillows',
                                'air freshener', 'essentials', 'wifi password', 'netflix', 'streaming', 'books',
                                'games', 'toys', 'crib', 'high chair', 'baby', 'children', 'family friendly',
                                'smoking allowed', 'no smoking', 'quiet', 'soundproof', 'blackout curtains',
                                'mountain view', 'city view', 'garden view', 'water view', 'lake view', 'ocean view'
                            ]) or
                            # Or if it looks like a standalone amenity (short, descriptive)
                            (len(text.split()) <= 4 and not any(char in text for char in ['$', '€', '£', '¥']) and
                             not text.lower().startswith(('rated', 'review', 'guest', 'night', 'stay', 'book'))))):
                            
                            clean_text = text.strip()
                            if clean_text and clean_text not in amenities:
                                amenities.append(clean_text)
                                
                    if len(amenities) > 5:  # If we found good amenities, stop
                        break
                        
                except:
                    continue
            
            # Try "Show all amenities" button with better modal extraction
            if len(amenities) < 5:
                try:
                    # Multiple selectors for show all button
                    show_all_selectors = [
                        "button:has-text('Show all')",
                        "button:has-text('See all')",
                        "*:has-text('Show all amenities')",
                        "*:has-text('View all amenities')"
                    ]
                    
                    for btn_selector in show_all_selectors:
                        try:
                            show_all_btn = page.locator(btn_selector).first
                            if await show_all_btn.is_visible():
                                await show_all_btn.click()
                                await page.wait_for_timeout(3000)
                                
                                # Enhanced modal extraction
                                modal_selectors = [
                                    "[role='dialog'] li",
                                    "[data-testid='modal'] li",
                                    "[role='dialog'] div:has-text('Wifi')",
                                    "[role='dialog'] div:has-text('Kitchen')",
                                    "[role='dialog'] span:has-text('conditioning')"
                                ]
                                
                                for modal_sel in modal_selectors:
                                    try:
                                        modal_amenities = page.locator(modal_sel)
                                        count = await modal_amenities.count()
                                        
                                        for i in range(min(count, 50)):
                                            text = await modal_amenities.nth(i).inner_text()
                                            text = text.strip()
                                            
                                            # Apply comprehensive filtering for modal
                                            if (text and len(text) >= 2 and len(text) <= 100 and 
                                                not any(exclude in text.lower() for exclude in [
                                                    'rare find', 'usually booked', 'show all', 'see all',
                                                    'close', 'back', 'done', 'cancel', 'x'
                                                ]) and text not in amenities):
                                                
                                                # Accept anything that looks like an amenity
                                                if (any(amenity in text.lower() for amenity in [
                                                    'wifi', 'kitchen', 'air', 'heat', 'tv', 'park', 'pool', 
                                                    'gym', 'wash', 'dry', 'balcony', 'garden', 'fire', 'tub',
                                                    'elevator', 'wheel', 'smoke', 'work', 'desk', 'fridge',
                                                    'coffee', 'beach', 'pet', 'safe', 'bath', 'bed'
                                                ]) or len(text.split()) <= 3):
                                                    amenities.append(text)
                                        
                                        if len(amenities) >= 5:
                                            break
                                            
                                    except:
                                        continue
                                        
                                # Close modal
                                try:
                                    close_btn = page.locator("[role='dialog'] button:has-text('Close'), [aria-label='Close']").first
                                    if await close_btn.is_visible():
                                        await close_btn.click()
                                        await page.wait_for_timeout(1000)
                                except:
                                    pass
                                    
                                break
                                
                        except:
                            continue
                            
                except:
                    pass
            
            # Log detailed amenities info
            if amenities:
                self.logger.info(f"✅ Found {len(amenities)} amenities: {amenities[:5]}...")
            else:
                self.logger.warning(f"⚠️ No amenities found")
            
            return amenities[:40]  # Increase limit to 40 amenities
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting amenities: {e}")
            return []
    
    async def _extract_images(self, page) -> List[str]:
        """Extract property images from detail page."""
        images = []
        
        try:
            # Comprehensive image selectors for Airbnb property images
            image_selectors = [
                # Primary gallery areas
                "[data-testid='photo-viewer'] img",
                "[data-testid='listing-gallery'] img",
                "[data-testid='photoViewer'] img",
                
                # Gallery containers
                "[class*='gallery'] img",
                "[class*='photos'] img", 
                "[class*='media'] img",
                "[class*='image'] img",
                
                # Picture elements
                "picture img",
                "figure img",
                
                # Direct image selectors with Airbnb paths
                "img[src*='airbnb.com/im/pictures']",
                "img[src*='pictures']",
                "img[src*='hosting']",
                "img[src*='miso']",
                "img[src*='prohost']",
                
                # Property-specific images (avoid icons)
                "img:not([src*='icons']):not([src*='assets/frontend']):not([width='24']):not([height='24'])",
                
                # All images as fallback (with heavy filtering)
                "img"
            ]
            
            for selector in image_selectors:
                try:
                    img_elements = page.locator(selector)
                    count = await img_elements.count()
                    
                    for i in range(min(count, 25)):  # Increase limit for more images
                        try:
                            src = await img_elements.nth(i).get_attribute("src")
                            if src and src not in images:
                                fixed_url = self._fix_image_url(src)
                                
                                # Enhanced filtering for property images
                                if (fixed_url and 
                                    # Must be from Airbnb or valid image URL
                                    ('airbnb' in fixed_url or fixed_url.startswith('http')) and
                                    # Include property image paths
                                    (any(path in fixed_url for path in ['pictures', 'hosting', 'miso', 'prohost', 'user']) or 
                                     fixed_url.endswith(('.jpg', '.jpeg', '.png', '.webp'))) and
                                    # Exclude UI elements and small images
                                    not any(exclude in fixed_url.lower() for exclude in [
                                        'icons', 'icon-', 'assets/frontend', 'logo', 'badge', 'avatar-default',
                                        'w=24', 'h=24', 'w=32', 'h=32', 'w=16', 'h=16'
                                    ]) and
                                    # Must be reasonable image dimensions (inferred from URL or assume good)
                                    len(fixed_url) > 50):  # Reasonable URL length
                                    images.append(fixed_url)
                        except:
                            continue
                            
                    if len(images) > 8:  # If we found many good images, stop
                        break
                        
                except:
                    continue
            
            # Log what we found with first few URLs for debugging
            if images:
                sample_urls = images[:3]
                self.logger.info(f"✅ Found {len(images)} property images. Sample: {sample_urls}")
            else:
                self.logger.warning(f"⚠️ No property images found")
            
            return images[:15]  # Limit to 15 images
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting images: {e}")
            return []
    
    async def _extract_location_data(self, page) -> Optional[Dict[str, Any]]:
        """Extract location data including coordinates and address."""
        try:
            location_data = {}
            
            # Enhanced coordinates extraction for Airbnb
            try:
                coordinates = await page.evaluate("""
                    () => {
                        // Try multiple Airbnb coordinate sources
                        try {
                            // Method 1: Airbnb bootstrap data
                            if (window.__AIRBNB_BOOTSTRAP__ && window.__AIRBNB_BOOTSTRAP__.reduxData) {
                                const reduxData = window.__AIRBNB_BOOTSTRAP__.reduxData;
                                
                                // Check listing data
                                if (reduxData.entities && reduxData.entities.listings) {
                                    const listings = Object.values(reduxData.entities.listings);
                                    for (const listing of listings) {
                                        if (listing.lat && listing.lng) {
                                            return {
                                                lat: parseFloat(listing.lat),
                                                lng: parseFloat(listing.lng),
                                                source: 'bootstrap_listings'
                                            };
                                        }
                                    }
                                }
                                
                                // Check map data
                                if (reduxData.marketplacePdp && reduxData.marketplacePdp.listingLocationInfo) {
                                    const locInfo = reduxData.marketplacePdp.listingLocationInfo;
                                    if (locInfo.lat && locInfo.lng) {
                                        return {
                                            lat: parseFloat(locInfo.lat),
                                            lng: parseFloat(locInfo.lng),
                                            source: 'bootstrap_location'
                                        };
                                    }
                                }
                            }
                            
                            // Method 2: Check window.__AIRBNB_DATA__
                            if (window.__AIRBNB_DATA__ && window.__AIRBNB_DATA__.listing) {
                                const listing = window.__AIRBNB_DATA__.listing;
                                if (listing.lat && listing.lng) {
                                    return {
                                        lat: parseFloat(listing.lat),
                                        lng: parseFloat(listing.lng),
                                        source: 'window.__AIRBNB_DATA__'
                                    };
                                }
                            }
                            
                            // Method 3: Search JSON scripts for coordinates
                            const scripts = document.querySelectorAll('script[type="application/json"], script:not([src])');
                            for (const script of scripts) {
                                try {
                                    const content = script.textContent || script.innerHTML;
                                    if (content && content.includes('lat') && content.includes('lng')) {
                                        // Try to parse as JSON first
                                        if (content.trim().startsWith('{') || content.trim().startsWith('[')) {
                                            const data = JSON.parse(content);
                                            
                                            // Recursive search for lat/lng in object
                                            const findCoords = (obj, path = '') => {
                                                if (!obj || typeof obj !== 'object') return null;
                                                
                                                if (obj.lat && obj.lng && 
                                                    typeof obj.lat === 'number' && typeof obj.lng === 'number') {
                                                    return {
                                                        lat: obj.lat,
                                                        lng: obj.lng,
                                                        source: 'json_script_' + path
                                                    };
                                                }
                                                
                                                for (const [key, value] of Object.entries(obj)) {
                                                    if (typeof value === 'object') {
                                                        const result = findCoords(value, path + key + '.');
                                                        if (result) return result;
                                                    }
                                                }
                                                return null;
                                            };
                                            
                                            const coords = findCoords(data);
                                            if (coords) return coords;
                                        }
                                        
                                        // Try regex extraction as fallback
                                        const latMatch = content.match(/"lat"\s*:\s*([\d\.\-]+)/);
                                        const lngMatch = content.match(/"lng"\s*:\s*([\d\.\-]+)/);
                                        if (latMatch && lngMatch) {
                                            return {
                                                lat: parseFloat(latMatch[1]),
                                                lng: parseFloat(lngMatch[1]),
                                                source: 'json_regex'
                                            };
                                        }
                                    }
                                } catch(e) { 
                                    // Continue if JSON parsing fails
                                }
                            }
                            
                            // Method 4: Check map iframes and links
                            const mapElements = document.querySelectorAll('iframe[src*="maps"], a[href*="maps"]');
                            for (const element of mapElements) {
                                const src = element.src || element.href;
                                const latMatch = src.match(/lat=([\d\.\-]+)/);
                                const lngMatch = src.match(/lng=([\d\.\-]+)/);
                                if (latMatch && lngMatch) {
                                    return {
                                        lat: parseFloat(latMatch[1]),
                                        lng: parseFloat(lngMatch[1]),
                                        source: 'map_element'
                                    };
                                }
                            }
                            
                        } catch(e) {
                            console.log('Error extracting coordinates:', e);
                        }
                        
                        return null;
                    }
                """)
                
                if coordinates and coordinates.get('lat') and coordinates.get('lng'):
                    location_data['latitude'] = coordinates['lat']
                    location_data['longitude'] = coordinates['lng']
                    
                    # Generate Google Maps URL
                    lat, lng = coordinates['lat'], coordinates['lng']
                    location_data['google_maps_url'] = f"https://www.google.com/maps/search/{lat},{lng}"
                    
                    self.logger.info(f"✅ Found coordinates: {lat}, {lng} from {coordinates['source']}")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Coordinate extraction failed: {e}")
            
            # Extract address information
            address_selectors = [
                "[data-testid='listing-location']",
                "[class*='location'] span",
                "h1 + div span",
                "*:has-text('neighborhood')",
                "*:has-text('district')"
            ]
            
            for selector in address_selectors:
                try:
                    element = page.locator(selector).first
                    if await element.is_visible():
                        address = await element.inner_text()
                        if address and len(address) > 5:
                            # Clean address - remove HTML dump and UI text
                            address = address.strip()
                            # Remove common UI elements and long text dumps
                            if (len(address) < 200 and  # Reasonable address length
                                not any(ui_text in address.lower() for ui_text in [
                                    'show more', 'read more', 'view map', 'get directions',
                                    'nearby attractions', 'transportation', 'reviews',
                                    'amenities', 'house rules', 'cancellation'
                                ])):
                                # Extract clean address lines (first 1-2 lines typically)
                                address_lines = [line.strip() for line in address.split('\n') if line.strip()]
                                clean_address = ', '.join(address_lines[:2])  # Take first 2 lines max
                                if len(clean_address) > 5 and len(clean_address) < 200:
                                    location_data['address'] = clean_address
                                    break
                except:
                    continue
            
            # Extract neighborhood information
            try:
                neighborhood_selectors = [
                    "*:has-text('neighborhood')",
                    "*:has-text('area')",
                    "*:has-text('district')",
                    "[data-testid*='neighborhood']"
                ]
                
                for selector in neighborhood_selectors:
                    try:
                        neighborhood_element = page.locator(selector).first
                        if await neighborhood_element.is_visible():
                            neighborhood = await neighborhood_element.inner_text()
                            if neighborhood:
                                # Clean neighborhood - avoid HTML dumps
                                neighborhood = neighborhood.strip()
                                if (len(neighborhood) < 100 and  # Reasonable length
                                    not any(ui_text in neighborhood.lower() for ui_text in [
                                        'show more', 'read more', 'view map', 'get directions',
                                        'transportation', 'reviews', 'amenities', 'house rules'
                                    ])):
                                    # Extract clean neighborhood name (first meaningful line)
                                    neighborhood_lines = [line.strip() for line in neighborhood.split('\n') if line.strip()]
                                    if neighborhood_lines:
                                        clean_neighborhood = neighborhood_lines[0]
                                        # Remove common prefixes
                                        clean_neighborhood = re.sub(r'^(neighborhood:|area:|district:)\s*', '', clean_neighborhood, flags=re.IGNORECASE)
                                        if len(clean_neighborhood) > 2 and len(clean_neighborhood) < 100:
                                            location_data['neighborhood'] = clean_neighborhood
                                            break
                    except:
                        continue
            except:
                pass
            
            return location_data if location_data else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting location data: {e}")
            return None
    
    async def _extract_description(self, page) -> Optional[str]:
        """Extract comprehensive property description from detail page."""
        try:
            # Enhanced description selectors - comprehensive approach
            description_selectors = [
                # Primary description sections
                "[data-testid='property-description'] span",
                "[data-testid='property-description'] p", 
                "[data-testid='property-description'] div",
                "[data-section-id='DESCRIPTION_DEFAULT'] span",
                "[data-section-id='DESCRIPTION_DEFAULT'] p",
                "[data-section-id='DESCRIPTION_DEFAULT'] div",
                
                # About sections
                "h2:has-text('About this') + div span",
                "h2:has-text('About this') + div p", 
                "h2:has-text('About this') + div div",
                "h3:has-text('About this') + div span",
                "h3:has-text('About this') + div p",
                
                # Description containers
                "[class*='description'] span:not([class*='button']):not([class*='link'])",
                "[class*='description'] p",
                "[class*='description'] div:not([class*='button'])",
                
                # Generic selectors for property descriptions
                "*:has-text('The space') + div p",
                "*:has-text('The space') + div span",
                "*:has-text('About the space') + div p",
                "*:has-text('Property description') + div p",
                "*:has-text('Description') + div p",
                
                # Look for longer text blocks that might be descriptions
                "div p:not([class*='button']):not([class*='link']):not([class*='price'])",
                "section p:not([class*='button']):not([class*='link']):not([class*='price'])"
            ]
            
            descriptions = []
            
            for selector in description_selectors:
                try:
                    elements = page.locator(selector)
                    count = await elements.count()
                    
                    for i in range(min(count, 5)):  # Check up to 5 matches per selector
                        element = elements.nth(i)
                        if await element.is_visible():
                            text = await element.inner_text()
                            text = text.strip()
                            
                            # Filter out non-description text
                            if (text and len(text) > 30 and 
                                not any(exclude in text.lower() for exclude in [
                                    'show more', 'read more', 'see all', 'contact host',
                                    'book now', 'reserve', 'check availability',
                                    'price', '$', '€', '£', 'per night',
                                    'reviews', 'rating', 'superhost',
                                    'cancellation', 'policy', 'rules'
                                ]) and
                                # Should contain descriptive words
                                any(desc_word in text.lower() for desc_word in [
                                    'space', 'room', 'apartment', 'house', 'home',
                                    'location', 'area', 'neighborhood', 'perfect',
                                    'comfortable', 'beautiful', 'enjoy', 'relax',
                                    'stay', 'guest', 'welcome', 'cozy', 'modern',
                                    'feature', 'kitchen', 'bathroom', 'bedroom'
                                ])):
                                descriptions.append(text)
                                
                except:
                    continue
            
            if descriptions:
                # Combine descriptions and deduplicate
                combined_description = ""
                seen_sentences = set()
                
                for desc in descriptions:
                    sentences = desc.split('. ')
                    for sentence in sentences:
                        sentence = sentence.strip()
                        if (sentence and 
                            len(sentence) > 20 and 
                            sentence.lower() not in seen_sentences):
                            seen_sentences.add(sentence.lower())
                            if combined_description:
                                combined_description += ". "
                            combined_description += sentence
                
                if combined_description and len(combined_description) > 50:
                    # Clean and limit description
                    combined_description = combined_description[:3000]  # Increased limit for better completeness
                    self.logger.info(f"✅ Found enhanced description ({len(combined_description)} chars)")
                    return combined_description
            
            return None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting description: {e}")
            return None
    
    async def _extract_basic_reviews(self, page) -> Optional[Dict[str, Any]]:
        """Extract basic review data (2-5 reviews)."""
        try:
            reviews_data = {}
            reviews = []
            
            # Navigate to reviews section
            try:
                reviews_button = page.locator("button:has-text('review'), a:has-text('review')").first
                if await reviews_button.is_visible():
                    await reviews_button.click()
                    await page.wait_for_timeout(3000)
            except:
                pass
            
            # Extract individual reviews
            review_selectors = [
                "[data-testid='review-card']",
                "[class*='review-card']",
                "[class*='review'] div:has-text('★')"
            ]
            
            for selector in review_selectors:
                try:
                    review_elements = page.locator(selector)
                    count = await review_elements.count()
                    
                    for i in range(min(count, 5)):
                        try:
                            review_element = review_elements.nth(i)
                            review_text = await review_element.inner_text()
                            
                            if review_text and len(review_text) > 20:
                                reviews.append({
                                    'text': review_text[:500],  # Limit review length
                                    'extracted_at': datetime.now().isoformat()
                                })
                        except:
                            continue
                            
                    if reviews:
                        break
                        
                except:
                    continue
            
            if reviews:
                reviews_data['reviews'] = reviews
                reviews_data['reviews_count'] = len(reviews)
                reviews_data['reviews_extraction_method'] = "basic_sampling"
                
                self.logger.info(f"✅ Extracted {len(reviews)} basic reviews")
            
            return reviews_data if reviews_data else None
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting basic reviews: {e}")
            return None
    
    async def _extract_comprehensive_reviews(self, page) -> Optional[Dict[str, Any]]:
        """Extract comprehensive review data with enhanced pagination."""
        try:
            reviews_data = {}
            reviews = []
            
            # Navigate to reviews section with multiple strategies
            await self._navigate_to_reviews_section(page)
            
            # Enhanced pagination handling
            max_pages = 5  # Increased for comprehensive extraction
            current_page = 0
            total_attempts = 0
            max_attempts = 10
            
            while current_page < max_pages and total_attempts < max_attempts:
                total_attempts += 1
                
                # Extract reviews from current page
                self.logger.info(f"🔍 Extracting reviews from page {current_page + 1}")
                page_reviews = await self._extract_reviews_from_page(page)
                
                if page_reviews:
                    new_reviews = 0
                    for review in page_reviews:
                        # Avoid duplicates across pages
                        if not any(self._reviews_similar(review.get('text', ''), r.get('text', '')) for r in reviews):
                            reviews.append(review)
                            new_reviews += 1
                    
                    self.logger.info(f"✅ Added {new_reviews} new reviews from page {current_page + 1}")
                    
                    if new_reviews == 0:
                        self.logger.info("No new reviews found, stopping pagination")
                        break
                else:
                    self.logger.info("No reviews found on this page")
                    
                    # Early termination: If we're on the first page and find no reviews,
                    # don't waste time trying pagination - this property likely has no accessible reviews
                    if current_page == 0:
                        self.logger.info("🚫 No reviews found on first page - skipping pagination for this property")
                        break
                
                # Try multiple pagination strategies (only if we found reviews worth paginating)
                if page_reviews:  # Only attempt pagination if we found reviews
                    if not await self._navigate_to_next_page(page):
                        self.logger.info("No more pages available")
                        break
                else:
                    # No reviews found, break pagination loop
                    break
                
                current_page += 1
                
                # Stop if we have enough reviews
                if len(reviews) >= 50:
                    self.logger.info(f"Reached target of {len(reviews)} reviews")
                    break
            
            if reviews:
                # Sort reviews by quality (those with more metadata first)
                reviews.sort(key=lambda r: (
                    bool(r.get('review_date')),
                    bool(r.get('reviewer_location')),
                    bool(r.get('review_rating')),
                    len(r.get('text', ''))
                ), reverse=True)
                
                reviews_data['reviews'] = reviews[:50]  # Keep best 50 reviews
                reviews_data['reviews_count'] = len(reviews[:50])
                reviews_data['total_reviews_found'] = len(reviews)
                reviews_data['reviews_extraction_method'] = "enhanced_comprehensive_pagination"
                reviews_data['pages_processed'] = current_page + 1
                reviews_data['review_quality_score'] = self._calculate_review_quality_score(reviews[:50])
                
                self.logger.info(f"✅ Extracted {len(reviews[:50])} high-quality comprehensive reviews from {current_page + 1} pages")
            
            return reviews_data if reviews_data else None
            
        except Exception as e:
            import traceback
            self.logger.error(f"❌ Error extracting comprehensive reviews: {e}")
            self.logger.error(f"❌ Review extraction traceback: {traceback.format_exc()}")
            return None

    async def _navigate_to_reviews_section(self, page):
        """Navigate to reviews section using multiple strategies with optimized timeouts."""
        navigation_strategies = [
            # Strategy 1: Click reviews button/link
            ("review_button", "button:has-text('review'), a:has-text('review')"),
            
            # Strategy 2: Click review count/rating area
            ("review_count", "span:has-text('review'), div:has-text('review')"),
            
            # Strategy 3: Scroll to reviews section
            ("scroll_to_reviews", "[data-testid*='review'], [class*='review']"),
            
            # Strategy 4: Try specific review section selectors
            ("review_section", "section:has-text('review'), div:has-text('★')")
        ]
        
        for i, (strategy_name, selector) in enumerate(navigation_strategies):
            try:
                self.logger.info(f"Trying navigation strategy {i+1}: {strategy_name}")
                
                if strategy_name == "scroll_to_reviews":
                    # Scroll strategy
                    try:
                        element = page.locator(selector).first
                        await element.wait_for(state="visible", timeout=1000)  # Quick check
                        await element.scroll_into_view_if_needed()
                    except:
                        continue
                else:
                    # Click strategies with timeout
                    try:
                        element = page.locator(selector).first
                        await element.wait_for(state="visible", timeout=1500)  # Reduced from default 30s
                        await element.click()
                    except:
                        continue
                
                # Reduced wait time
                await page.wait_for_timeout(1500)  # Reduced from 2000ms
                
                # Check if reviews are now visible
                reviews_visible = await page.locator("[class*='review'], [data-testid*='review']").count()
                if reviews_visible > 0:
                    self.logger.info(f"✅ Successfully navigated to reviews section with strategy {i+1} ({strategy_name})")
                    return
                    
            except Exception as e:
                self.logger.debug(f"Navigation strategy {i+1} ({strategy_name}) failed: {e}")
                continue
    
    async def _navigate_to_next_page(self, page) -> bool:
        """Try multiple pagination strategies to load more reviews with optimized timeouts."""
        pagination_strategies = [
            # Strategy 1: Click "Next" button
            ("next_button", "button:has-text('Next'), button[aria-label*='next' i]"),
            
            # Strategy 2: Click "Show more" button  
            ("show_more_button", "button:has-text('Show more'), button:has-text('Load more')"),
            
            # Strategy 3: Scroll to load more
            ("scroll", None),
            
            # Strategy 4: Click pagination numbers
            ("page_numbers", "button:has-text('2'), button:has-text('3'), button:has-text('4')")
        ]
        
        for i, (strategy_name, selector) in enumerate(pagination_strategies):
            try:
                self.logger.info(f"Trying pagination strategy {i+1}: {strategy_name}")
                
                # Count current reviews before attempting navigation
                before_count = await page.locator("[class*='review'], [data-testid*='review']").count()
                
                # Execute strategy with timeout optimization
                if strategy_name == "scroll":
                    # Scroll strategy - always works, no timeout needed
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                else:
                    # Button-based strategies - use explicit timeout to avoid 30s default
                    try:
                        # Wait max 2 seconds for element to be visible
                        element = page.locator(selector).first
                        await element.wait_for(state="visible", timeout=2000)
                        await element.click()
                    except Exception as click_error:
                        self.logger.debug(f"Strategy {i+1} element not found or not clickable: {click_error}")
                        continue
                
                # Wait for content to load (reduced from 3000ms to 2000ms)
                await page.wait_for_timeout(2000)
                
                # Check if new content loaded
                after_count = await page.locator("[class*='review'], [data-testid*='review']").count()
                
                if after_count > before_count:
                    self.logger.info(f"✅ Pagination strategy {i+1} ({strategy_name}) loaded more reviews")
                    return True
                else:
                    self.logger.debug(f"Strategy {i+1} ({strategy_name}) didn't load new content")
                    
            except Exception as e:
                self.logger.debug(f"Pagination strategy {i+1} ({strategy_name}) failed: {e}")
                continue
        
        return False

    def _calculate_review_quality_score(self, reviews: List[Dict[str, Any]]) -> float:
        """Calculate quality score for extracted reviews."""
        if not reviews:
            return 0.0
        
        total_score = 0
        for review in reviews:
            score = 0
            
            # Base score for having review text
            if review.get('text') and len(review['text']) > 50:
                score += 3
            elif review.get('text'):
                score += 1
                
            # Additional points for metadata
            if review.get('reviewer_name') and review['reviewer_name'] != 'Anonymous':
                score += 1
            if review.get('reviewer_location'):
                score += 1
            if review.get('review_date'):
                score += 1
            if review.get('review_rating'):
                score += 1
            if review.get('reviewer_avatar'):
                score += 1
            if review.get('reviewer_verified'):
                score += 1
                
            total_score += score
        
        max_possible_score = len(reviews) * 9  # Maximum score per review
        return (total_score / max_possible_score) * 100 if max_possible_score > 0 else 0
    
    async def _extract_reviews_from_page(self, page) -> List[Dict[str, Any]]:
        """Extract comprehensive reviews from current page with full metadata."""
        reviews = []
        
        try:
            # Comprehensive review container selectors
            review_selectors = [
                # Primary Airbnb review containers
                "[data-testid='review-card']",
                "[data-testid='pdp-review-card']", 
                "[data-review-id]",
                
                # Review section containers
                "[class*='review-card']",
                "[class*='review-item']",
                "[class*='review-container']",
                
                # Generic review patterns
                "div:has-text('★') + div",
                "*:has-text('★') [class*='review']",
                "article:has-text('★')",
                
                # Review list items
                "li:has([class*='star'])",
                "div[role='listitem']:has-text('★')",
                
                # Fallback patterns
                "div:has([aria-label*='star']):has-text(' ')",
                "section div:has-text('★'):not([class*='summary'])"
            ]
            
            for selector in review_selectors:
                try:
                    review_elements = page.locator(selector)
                    count = await review_elements.count()
                    
                    self.logger.info(f"🔍 Trying selector '{selector}' - found {count} elements")
                    
                    for i in range(min(count, 25)):  # Increased limit for comprehensive extraction
                        try:
                            review_element = review_elements.nth(i)
                            
                            # Extract comprehensive review data
                            review_data = await self._extract_single_review(review_element)
                            
                            if review_data and review_data.get('text') and len(review_data['text']) > 20:
                                # Avoid duplicates by checking text similarity
                                if not any(self._reviews_similar(review_data['text'], r.get('text', '')) for r in reviews):
                                    reviews.append(review_data)
                                    
                        except Exception as e:
                            self.logger.debug(f"Failed to extract review {i+1}: {e}")
                            continue
                            
                    if len(reviews) >= 10:  # Found enough reviews, no need to try more selectors
                        self.logger.info(f"✅ Found {len(reviews)} reviews with selector: {selector}")
                        break
                        
                except Exception as e:
                    self.logger.debug(f"Selector '{selector}' failed: {e}")
                    continue
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Error extracting reviews from page: {e}")
        
        self.logger.info(f"📄 Extracted {len(reviews)} reviews from current page")
        return reviews

    def _reviews_similar(self, text1: str, text2: str) -> bool:
        """Check if two review texts are similar to avoid duplicates."""
        if not text1 or not text2:
            return False
        
        # Simple similarity check - first 100 characters
        return text1[:100].strip().lower() == text2[:100].strip().lower()

    async def _extract_single_review(self, review_element) -> Optional[Dict[str, Any]]:
        """Extract comprehensive data from a single review element."""
        try:
            review_data = {
                'extracted_at': datetime.now().isoformat(),
                'text': '',
                'reviewer_name': 'Anonymous',
                'reviewer_location': None,
                'reviewer_avatar': None,
                'reviewer_verified': False,
                'review_date': None,
                'review_rating': None,
                'helpful_votes': None,
                'host_response': None,
                'review_language': 'en'
            }
            
            # Get full review container text first
            full_text = await review_element.inner_text()
            
            # Extract main review text - try multiple patterns
            review_text_selectors = [
                "span:not([class*='name']):not([class*='date']):not([class*='star'])",
                "p:not([class*='name']):not([class*='date'])",
                "div:not([class*='name']):not([class*='date']):not([class*='rating'])",
                "div[data-testid='review-text']",
                "[class*='review-text']",
                "[class*='comment']"
            ]
            
            for text_selector in review_text_selectors:
                try:
                    text_element = review_element.locator(text_selector).first
                    if await text_element.is_visible():
                        text = await text_element.inner_text()
                        if text and len(text) > 30 and not any(exclude in text.lower() for exclude in ['show more', 'read more', '★', '·']):
                            review_data['text'] = text.strip()[:1000]  # Increased limit for quality
                            break
                except:
                    continue
            
            # If no specific text found, extract from full text (filtered)
            if not review_data['text'] and full_text:
                # Filter out common UI elements and extract main review text
                lines = full_text.split('\n')
                review_lines = []
                for line in lines:
                    line = line.strip()
                    if (len(line) > 20 and 
                        not any(skip in line.lower() for skip in [
                            'show more', 'read more', 'helpful', 'report', 
                            'years on airbnb', 'months on airbnb', '★', '·',
                            'rating', 'stayed', 'ago'
                        ]) and
                        not line.isdigit() and
                        not all(c in '★·,. ' for c in line)):
                        review_lines.append(line)
                
                if review_lines:
                    review_data['text'] = ' '.join(review_lines[:3])[:1000]  # Take first few meaningful lines
            
            # Extract reviewer name - comprehensive patterns
            name_selectors = [
                "strong:not(:has-text('★')):not(:has-text('·'))",
                "b:not(:has-text('★')):not(:has-text('·'))",
                "[class*='name']:not([class*='property']):not([class*='host'])",
                "h3", "h4", "h5",
                "[data-testid*='reviewer-name']",
                "div:first-child strong",
                "div:first-child b"
            ]
            
            for name_selector in name_selectors:
                try:
                    name_element = review_element.locator(name_selector).first
                    if await name_element.is_visible():
                        name = await name_element.inner_text()
                        name = name.strip()
                        if (name and len(name) < 50 and len(name) > 1 and 
                            not any(skip in name.lower() for skip in ['★', '·', 'rating', 'ago', 'show', 'read']) and
                            not name.isdigit()):
                            review_data['reviewer_name'] = name
                            break
                except:
                    continue
            
            # Extract reviewer location - look for location indicators
            location_patterns = [
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',  # City, State/Country
                r'([A-Z][a-z]+,\s*[A-Z]{2,3})',  # City, State/Country code
            ]
            
            for line in full_text.split('\n'):
                for pattern in location_patterns:
                    import re
                    match = re.search(pattern, line)
                    if match:
                        location = match.group(1).strip()
                        if len(location) > 3 and len(location) < 50:
                            review_data['reviewer_location'] = location
                            break
                if review_data['reviewer_location']:
                    break
            
            # Extract review date - look for date patterns
            date_patterns = [
                r'(\w+\s+\d{4})',  # Month Year
                r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
                r'(\d{1,2}-\d{1,2}-\d{4})',  # MM-DD-YYYY
                r'(\d+\s+(?:days?|weeks?|months?|years?)\s+ago)'  # Relative dates
            ]
            
            for line in full_text.split('\n'):
                for pattern in date_patterns:
                    match = re.search(pattern, line)
                    if match:
                        review_data['review_date'] = match.group(1).strip()
                        break
                if review_data['review_date']:
                    break
            
            # Extract rating if available
            rating_selectors = [
                "[aria-label*='star']",
                "[class*='star']:not([class*='summary'])",
                "span:has-text('★')",
                "[data-testid*='rating']"
            ]
            
            for rating_selector in rating_selectors:
                try:
                    rating_element = review_element.locator(rating_selector).first
                    if await rating_element.is_visible():
                        rating_text = await rating_element.inner_text()
                        # Extract rating number
                        rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                            if 1 <= rating <= 5:
                                review_data['review_rating'] = rating
                                break
                except:
                    continue
            
            # Extract reviewer avatar
            try:
                avatar_element = review_element.locator("img[src*='airbnb'], img[alt*='user'], img[alt*='profile']").first
                if await avatar_element.is_visible():
                    avatar_src = await avatar_element.get_attribute('src')
                    if avatar_src and 'airbnb' in avatar_src.lower():
                        review_data['reviewer_avatar'] = avatar_src
            except:
                pass
            
            # Check if reviewer is verified (look for verification indicators)
            if 'verified' in full_text.lower() or 'identity verified' in full_text.lower():
                review_data['reviewer_verified'] = True
            
            return review_data if review_data['text'] else None
            
        except Exception as e:
            self.logger.debug(f"Error extracting single review: {e}")
            return None
    
