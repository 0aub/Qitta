"""
Page Exploration API - Dynamic DOM analysis and selector testing
============================================================

This module provides real-time page exploration capabilities for debugging
and building robust web scrapers. Instead of hardcoded selectors, it allows
dynamic discovery of page structure and data extraction patterns.

Key Features:
- DOM structure analysis
- Selector testing and validation  
- Data extraction debugging
- Booking.com specific exploration tools
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from playwright.async_api import Browser, Page, TimeoutError as PlaywrightTimeoutError


class PageExplorer:
    """Interactive page exploration for web scraping development."""
    
    def __init__(self, browser: Browser, logger: logging.Logger):
        self.browser = browser
        self.logger = logger
    
    async def analyze_page_structure(self, url: str, wait_timeout: int = 10000) -> Dict[str, Any]:
        """Analyze the basic structure and elements of a webpage."""
        context = await self.browser.new_context()
        page = await context.new_page()
        
        try:
            self.logger.info(f"🔍 Analyzing page structure: {url}")
            
            # Navigate to page
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(wait_timeout)
            
            # Basic page info
            title = await page.title()
            current_url = page.url
            
            # DOM analysis
            structure_info = await page.evaluate("""
                () => {
                    const info = {
                        title: document.title,
                        url: window.location.href,
                        domain: window.location.hostname,
                        elements: {
                            total: document.querySelectorAll('*').length,
                            divs: document.querySelectorAll('div').length,
                            spans: document.querySelectorAll('span').length,
                            links: document.querySelectorAll('a').length,
                            images: document.querySelectorAll('img').length,
                            inputs: document.querySelectorAll('input').length,
                            buttons: document.querySelectorAll('button').length,
                            forms: document.querySelectorAll('form').length
                        },
                        data_attributes: [],
                        classes: [],
                        ids: []
                    };
                    
                    // Collect data attributes
                    const dataAttrs = new Set();
                    document.querySelectorAll('[data-testid], [data-*]').forEach(el => {
                        Array.from(el.attributes).forEach(attr => {
                            if (attr.name.startsWith('data-')) {
                                dataAttrs.add(attr.name);
                            }
                        });
                    });
                    info.data_attributes = Array.from(dataAttrs).slice(0, 50);
                    
                    // Collect common class patterns
                    const classNames = new Set();
                    document.querySelectorAll('[class]').forEach(el => {
                        el.className.split(' ').forEach(cls => {
                            if (cls.trim() && cls.length < 50) {
                                classNames.add(cls.trim());
                            }
                        });
                    });
                    info.classes = Array.from(classNames).slice(0, 100);
                    
                    // Collect IDs
                    const ids = [];
                    document.querySelectorAll('[id]').forEach(el => {
                        if (el.id && el.id.length < 50) {
                            ids.push(el.id);
                        }
                    });
                    info.ids = ids.slice(0, 50);
                    
                    return info;
                }
            """)
            
            return {
                "status": "success",
                "page_info": structure_info,
                "analysis_timestamp": datetime.now().isoformat(),
                "load_time_ms": wait_timeout
            }
            
        except Exception as e:
            self.logger.error(f"Page structure analysis failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "url": url,
                "analysis_timestamp": datetime.now().isoformat()
            }
        finally:
            await context.close()
    
    async def test_selectors(self, url: str, selectors: List[str], 
                           extract_text: bool = True, extract_attributes: bool = False,
                           wait_timeout: int = 10000) -> Dict[str, Any]:
        """Test multiple selectors against a webpage and return what they find."""
        context = await self.browser.new_context()
        page = await context.new_page()
        
        try:
            self.logger.info(f"🧪 Testing {len(selectors)} selectors on: {url}")
            
            # Navigate to page
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(wait_timeout)
            
            selector_results = {}
            
            for i, selector in enumerate(selectors):
                try:
                    # Test selector
                    elements = page.locator(selector)
                    count = await elements.count()
                    
                    result = {
                        "count": count,
                        "selector": selector,
                        "status": "found" if count > 0 else "not_found",
                        "samples": []
                    }
                    
                    # Extract sample data if elements found
                    if count > 0:
                        sample_limit = min(count, 5)  # Get up to 5 samples
                        
                        for j in range(sample_limit):
                            try:
                                element = elements.nth(j)
                                sample = {"index": j}
                                
                                if extract_text:
                                    text = await element.inner_text()
                                    sample["text"] = text.strip() if text else ""
                                
                                if extract_attributes:
                                    # Get all attributes
                                    attrs = await element.evaluate("el => Array.from(el.attributes).map(a => ({name: a.name, value: a.value}))")
                                    sample["attributes"] = attrs
                                
                                # Check visibility
                                sample["visible"] = await element.is_visible()
                                
                                result["samples"].append(sample)
                                
                            except Exception as e:
                                result["samples"].append({
                                    "index": j,
                                    "error": str(e)
                                })
                    
                    selector_results[f"selector_{i+1}"] = result
                    self.logger.debug(f"Selector {i+1}: {selector} -> {count} elements")
                    
                except Exception as e:
                    selector_results[f"selector_{i+1}"] = {
                        "count": 0,
                        "selector": selector,
                        "status": "error",
                        "error": str(e),
                        "samples": []
                    }
            
            return {
                "status": "success",
                "url": url,
                "results": selector_results,
                "total_selectors_tested": len(selectors),
                "successful_selectors": len([r for r in selector_results.values() if r["count"] > 0]),
                "test_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Selector testing failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "url": url,
                "test_timestamp": datetime.now().isoformat()
            }
        finally:
            await context.close()
    
    async def extract_data_debug(self, url: str, extraction_config: Dict[str, Any],
                                wait_timeout: int = 10000) -> Dict[str, Any]:
        """Debug data extraction with detailed logging and validation."""
        context = await self.browser.new_context()
        page = await context.new_page()
        
        try:
            self.logger.info(f"🔍 Debug extraction on: {url}")
            
            # Navigate to page
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(wait_timeout)
            
            extraction_results = {}
            
            for field_name, config in extraction_config.items():
                try:
                    selectors = config.get("selectors", [])
                    extract_type = config.get("extract", "text")  # text, attribute, html
                    attribute_name = config.get("attribute", "")
                    required = config.get("required", False)
                    
                    field_result = {
                        "field": field_name,
                        "required": required,
                        "extract_type": extract_type,
                        "selectors_tested": len(selectors),
                        "extraction_attempts": [],
                        "final_value": None,
                        "status": "not_found"
                    }
                    
                    # Try each selector until we get data
                    for selector in selectors:
                        attempt = {
                            "selector": selector,
                            "element_count": 0,
                            "extracted_value": None,
                            "error": None
                        }
                        
                        try:
                            elements = page.locator(selector)
                            count = await elements.count()
                            attempt["element_count"] = count
                            
                            if count > 0:
                                element = elements.first
                                
                                if extract_type == "text":
                                    value = await element.inner_text()
                                elif extract_type == "attribute" and attribute_name:
                                    value = await element.get_attribute(attribute_name)
                                elif extract_type == "html":
                                    value = await element.inner_html()
                                else:
                                    value = await element.inner_text()
                                
                                if value:
                                    attempt["extracted_value"] = value.strip()
                                    field_result["final_value"] = value.strip()
                                    field_result["status"] = "found"
                                    field_result["successful_selector"] = selector
                                    break
                                    
                        except Exception as e:
                            attempt["error"] = str(e)
                        
                        field_result["extraction_attempts"].append(attempt)
                    
                    extraction_results[field_name] = field_result
                    
                except Exception as e:
                    extraction_results[field_name] = {
                        "field": field_name,
                        "status": "error",
                        "error": str(e)
                    }
            
            return {
                "status": "success",
                "url": url,
                "extraction_results": extraction_results,
                "debug_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Debug extraction failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "url": url,
                "debug_timestamp": datetime.now().isoformat()
            }
        finally:
            await context.close()
    
    async def explore_booking_hotel(self, location: str = "Dubai", 
                                   check_in: str = "2025-12-01", 
                                   check_out: str = "2025-12-03") -> Dict[str, Any]:
        """Booking.com specific exploration to understand current page structure."""
        context = await self.browser.new_context()
        page = await context.new_page()
        
        try:
            self.logger.info(f"🏨 Exploring Booking.com structure for: {location}")
            
            # Navigate to booking.com
            await page.goto("https://www.booking.com", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(3000)
            
            # Perform basic search to get to results page
            try:
                # Fill location
                location_input = page.locator("input[placeholder*='Where are you going?'], input[name='ss']")
                await location_input.fill(location)
                await page.wait_for_timeout(1500)
                
                # Click search
                search_button = page.locator("button:has-text('Search'), button[type='submit']").first
                await search_button.click()
                await page.wait_for_timeout(5000)
                
            except Exception as e:
                self.logger.warning(f"Basic search setup failed: {e}")
            
            current_url = page.url
            
            # Explore key areas of the page
            exploration_results = {}
            
            # 1. Hotel cards exploration
            hotel_cards_selectors = [
                "div[data-testid='property-card']",
                ".sr_property_block", 
                ".bh-property-card",
                "[data-testid*='property']",
                ".property-card"
            ]
            
            exploration_results["hotel_cards"] = await self._explore_element_group(
                page, hotel_cards_selectors, "Hotel Cards", max_samples=3
            )
            
            # 2. Rating selectors exploration
            rating_selectors = [
                "[data-testid*='rating']",
                ".bui-rating",
                "[aria-label*='rating']",
                ".review-score",
                "[data-testid='rating-stars']",
                ".sr_gs_rating_score"
            ]
            
            exploration_results["ratings"] = await self._explore_element_group(
                page, rating_selectors, "Ratings", extract_attributes=True
            )
            
            # 3. Price selectors exploration  
            price_selectors = [
                "[data-testid*='price']",
                ".bui-price-display",
                ".sr_gs_price",
                "[aria-label*='price']",
                ".price",
                "[data-testid='price-and-discounted-price']"
            ]
            
            exploration_results["prices"] = await self._explore_element_group(
                page, price_selectors, "Prices", extract_attributes=True
            )
            
            # 4. Review selectors exploration
            review_selectors = [
                "[data-testid*='review']",
                ".review-item",
                ".bui-review-item", 
                "[data-testid='reviewer-name']",
                ".review-content"
            ]
            
            exploration_results["reviews"] = await self._explore_element_group(
                page, review_selectors, "Reviews"
            )
            
            # 5. Filter selectors exploration
            filter_selectors = [
                "[data-testid*='filter']",
                "input[name*='price']",
                "input[name*='rating']",
                ".filter-item",
                "[aria-label*='filter']"
            ]
            
            exploration_results["filters"] = await self._explore_element_group(
                page, filter_selectors, "Filters", extract_attributes=True
            )
            
            return {
                "status": "success",
                "current_url": current_url,
                "exploration_results": exploration_results,
                "page_title": await page.title(),
                "exploration_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Booking.com exploration failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "exploration_timestamp": datetime.now().isoformat()
            }
        finally:
            await context.close()
    
    async def _explore_element_group(self, page: Page, selectors: List[str], 
                                   group_name: str, max_samples: int = 2,
                                   extract_attributes: bool = False) -> Dict[str, Any]:
        """Helper to explore a group of related selectors."""
        group_results = {
            "group_name": group_name,
            "selectors_tested": len(selectors),
            "working_selectors": 0,
            "total_elements_found": 0,
            "selector_results": []
        }
        
        for selector in selectors:
            try:
                elements = page.locator(selector)
                count = await elements.count()
                
                selector_result = {
                    "selector": selector,
                    "element_count": count,
                    "samples": []
                }
                
                if count > 0:
                    group_results["working_selectors"] += 1
                    group_results["total_elements_found"] += count
                    
                    # Get samples
                    sample_limit = min(count, max_samples)
                    for i in range(sample_limit):
                        try:
                            element = elements.nth(i)
                            sample = {
                                "index": i,
                                "text": (await element.inner_text()).strip(),
                                "visible": await element.is_visible()
                            }
                            
                            if extract_attributes:
                                attrs = await element.evaluate("el => Array.from(el.attributes).map(a => ({name: a.name, value: a.value}))")
                                sample["attributes"] = attrs[:10]  # Limit attributes
                            
                            selector_result["samples"].append(sample)
                            
                        except Exception as e:
                            selector_result["samples"].append({
                                "index": i,
                                "error": str(e)
                            })
                
                group_results["selector_results"].append(selector_result)
                
            except Exception as e:
                group_results["selector_results"].append({
                    "selector": selector,
                    "element_count": 0,
                    "error": str(e),
                    "samples": []
                })
        
        return group_results