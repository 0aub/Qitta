"""
ScrapeSiteTask - Professional website scraper optimized for vector store preparation.

Two-phase approach:
Phase 1: Intelligent HTML collection with JS rendering support
Phase 2: Offline content extraction and chunking (separate task: extract-content)
"""

import asyncio
import logging
import json
import random
import pathlib
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple
import httpx

from .base import _log


class ScrapeSiteTask:
    """
    Professional two-phase website scraper optimized for vector store preparation.
    
    Phase 1: Intelligent HTML collection with JS rendering support
    Phase 2: Offline content extraction and chunking (separate task: extract-content)
    """
    
    # ───── URL management utilities ─────
    @staticmethod
    def _create_readable_filename(url: str) -> str:
        """Create a readable filename from URL."""
        import re
        
        parsed = urllib.parse.urlparse(url)
        
        # Start with domain
        domain = parsed.netloc.replace('www.', '')
        
        # Add path, replacing slashes with underscores
        path = parsed.path.strip('/')
        if path:
            # Clean up the path
            path = re.sub(r'[^\w\-_/.]', '_', path)
            path = path.replace('/', '_')
            filename = f"{domain}_{path}"
        else:
            filename = domain
            
        # Add query params if they look meaningful (not just tracking)
        if parsed.query:
            query_parts = []
            for param in parsed.query.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    # Skip common tracking parameters
                    if key not in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'ref', 'source']:
                        query_parts.append(f"{key}_{value}")
            if query_parts:
                filename += '_' + '_'.join(query_parts[:3])  # Limit to first 3 meaningful params
        
        # Clean up and ensure reasonable length
        filename = re.sub(r'[^\w\-_.]', '_', filename)
        filename = re.sub(r'_+', '_', filename)  # Replace multiple underscores with single
        filename = filename.strip('_')
        
        # Ensure reasonable length (filesystem limits)
        if len(filename) > 200:
            # Keep domain and truncate path
            domain_part = domain.replace('.', '_')
            remaining_length = 200 - len(domain_part) - 1
            if remaining_length > 0:
                truncated_path = filename[len(domain_part)+1:][:remaining_length]
                filename = f"{domain_part}_{truncated_path}"
            else:
                filename = domain_part
        
        return filename or "webpage"
    
    @staticmethod
    def _should_skip_url(url: str, base_domain: str, start_url: str = None) -> bool:
        """Filter out non-content URLs before fetching."""
        # Note: This method references utils functions that would need to be imported
        # For the modular version, we'll implement basic filtering
        
        url_lower = url.lower()
        
        # Basic domain check
        if base_domain not in url_lower:
            return True
            
        # Skip obvious non-content URLs
        skip_patterns = [
            '/search', '/login', '/register', '/cart', '/checkout', '/admin',
            '/api/', '/ajax/', '/.well-known/', '/wp-admin/', '/wp-json/',
            '.xml', '.json', '.rss', '.atom', '.pdf', '.doc', '.docx',
            '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.tar', '.gz',
            '/contact', '/privacy', '/terms', '/sitemap', '/robots.txt',
            '/favicon.ico', '/apple-touch-icon', 'fluidicon.png',
            'join?', 'signup?', 'register?'  # Query parameter based signup links
        ]
        
        # Skip files with non-HTML extensions
        file_extensions_to_skip = [
            '.css', '.js', '.map', '.scss', '.less',
            '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp',
            '.mp4', '.mp3', '.wav', '.avi', '.mov',
            '.woff', '.woff2', '.ttf', '.eot',
            '.zip', '.tar', '.gz', '.rar',
            '.exe', '.dmg', '.deb', '.rpm'
        ]
        
        # Check file extensions
        parsed_url = urllib.parse.urlparse(url_lower)
        path = parsed_url.path.lower()
        if any(path.endswith(ext) for ext in file_extensions_to_skip):
            return True
        
        return any(pattern in url_lower for pattern in skip_patterns)
    
    @staticmethod
    def _extract_urls_from_page(html: str, current_url: str, base_domain: str, start_url: str = None) -> List[str]:
        """Extract valid URLs from a page's HTML."""
        # Simplified URL extraction - full implementation would use utils functions
        import re
        
        # Basic link extraction using regex
        link_pattern = r'href=["\']([^"\']+)["\']'
        raw_links = re.findall(link_pattern, html, re.IGNORECASE)
        
        valid_links = []
        for link in raw_links:
            try:
                # Basic URL normalization
                if link.startswith('//'):
                    link = 'https:' + link
                elif link.startswith('/'):
                    base_url = f"https://{base_domain}"
                    link = base_url + link
                elif not link.startswith('http'):
                    continue  # Skip relative links for simplicity
                
                if not ScrapeSiteTask._should_skip_url(link, base_domain, start_url):
                    valid_links.append(link)
            except Exception:
                continue
                
        return valid_links
    
    # ───── Content fetching with retry logic ─────
    @staticmethod
    async def _fetch_page_html(
        client: httpx.AsyncClient,
        url: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Fetch a single page's HTML with proper error handling.
        Returns: (success, html_content, metadata)
        """
        try:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            
            html = response.text
            content_type = response.headers.get("content-type", "")
            
            # Basic content validation
            if not html or len(html.strip()) < 100:
                return False, None, {"error": "insufficient_content", "url": url}
                
            # Check if it's actually HTML
            if "text/html" not in content_type and not any(
                tag in html.lower()[:1000] for tag in ["<html", "<head", "<body", "<!doctype"]
            ):
                return False, None, {"error": "not_html", "url": url, "content_type": content_type}
            
            metadata = {
                "url": url,
                "status_code": response.status_code,
                "content_type": content_type,
                "content_length": len(html),
                "final_url": str(response.url)
            }
            
            return True, html, metadata
            
        except Exception as e:
            return False, None, {"error": f"fetch_failed_{type(e).__name__}", "url": url}
    
    @staticmethod
    async def _fetch_with_browser(
        browser,
        url: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """
        Fetch page using Playwright for JavaScript-heavy sites.
        Fallback method when HTTP client fails or for dynamic content.
        """
        context = None
        page = None
        
        try:
            # Create browser context with custom headers
            context = await browser.new_context(
                user_agent=headers.get("user-agent", ""),
                extra_http_headers=headers
            )
            
            page = await context.new_page()
            
            # Navigate and wait for content
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)  # Allow dynamic content to load
            
            html = await page.content()
            final_url = page.url
            
            metadata = {
                "url": url,
                "final_url": final_url,
                "content_length": len(html),
                "method": "playwright"
            }
            
            return True, html, metadata
            
        except Exception as e:
            return False, None, {"error": f"browser_failed_{type(e).__name__}", "url": url}
        finally:
            if page:
                await page.close()
            if context:
                await context.close()
    
    # ───── Main scraping logic ─────
    @staticmethod
    async def _scrape_website(
        browser,
        start_url: str,
        max_pages: Optional[int],
        use_browser: bool,
        headers: Dict[str, str],
        out_dir: pathlib.Path,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Execute the main scraping logic."""
        # Basic domain extraction
        parsed = urllib.parse.urlparse(start_url)
        base_domain = parsed.netloc
        
        if not base_domain:
            raise ValueError(f"Invalid start URL: {start_url}")
        
        # Create output directories
        html_dir = out_dir / "raw_html"
        html_dir.mkdir(parents=True, exist_ok=True)
        
        # State tracking
        url_queue = [start_url]
        processed_urls = set()
        successful_pages = []
        failed_pages = []
        total_content_size = 0
        
        # HTTP client for standard requests
        timeout = httpx.Timeout(20.0, connect=10.0)
        
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            while url_queue and (max_pages is None or len(successful_pages) < max_pages):
                current_url = url_queue.pop(0)
                
                if current_url in processed_urls:
                    continue
                    
                processed_urls.add(current_url)
                _log(logger, "info", f"Processing [{len(successful_pages)+1}]: {current_url[:100]}")
                
                # Try HTTP client first, fallback to browser if needed
                success = False
                html = None
                metadata = {}
                
                if not use_browser:
                    success, html, metadata = await ScrapeSiteTask._fetch_page_html(
                        client, current_url, headers, logger
                    )
                
                # Fallback to browser for failed requests or when explicitly requested
                if not success and browser:
                    success, html, metadata = await ScrapeSiteTask._fetch_with_browser(
                        browser, current_url, headers, logger
                    )
                
                if not success:
                    failed_pages.append(metadata)
                    _log(logger, "warning", f"Failed to fetch {current_url}: {metadata.get('error', 'unknown')}")
                    continue
                
                # Save HTML with readable filename
                safe_filename = ScrapeSiteTask._create_readable_filename(current_url) + ".html"
                html_file = html_dir / safe_filename
                
                try:
                    html_file.write_text(html, encoding='utf-8')
                    file_size = len(html.encode('utf-8'))
                except Exception as e:
                    _log(logger, "error", f"Failed to save {current_url}: {e}")
                    failed_pages.append({"url": current_url, "error": f"save_failed_{e}"})
                    continue
                
                # Track success
                page_info = {
                    "url": current_url,
                    "file": safe_filename,
                    "quality_score": 0.8,  # Simplified quality scoring
                    "size": file_size,
                    **metadata
                }
                successful_pages.append(page_info)
                total_content_size += file_size
                
                # Extract new URLs for crawling
                if len(successful_pages) < (max_pages or float('inf')):
                    new_urls = ScrapeSiteTask._extract_urls_from_page(html, current_url, base_domain, start_url)
                    
                    for new_url in new_urls:
                        if new_url not in processed_urls and new_url not in url_queue:
                            url_queue.append(new_url)
                
                # Rate limiting
                await asyncio.sleep(random.uniform(1.0, 2.5))
        
        # Save metadata
        crawl_metadata = {
            "start_url": start_url,
            "base_domain": base_domain,
            "total_pages_found": len(processed_urls),
            "successful_pages": len(successful_pages),
            "failed_pages": len(failed_pages),
            "total_content_size": total_content_size,
            "average_quality_score": sum(p["quality_score"] for p in successful_pages) / len(successful_pages) if successful_pages else 0,
            "pages": successful_pages,
            "failures": failed_pages[:50]  # Limit failure details
        }
        
        # Save metadata files
        (out_dir / "crawl_metadata.json").write_text(json.dumps(crawl_metadata, indent=2))
        (out_dir / "page_urls.json").write_text(json.dumps([p["url"] for p in successful_pages], indent=2))
        
        return {
            "start_url": start_url,
            "base_domain": base_domain,
            "pages_scraped": len(successful_pages),
            "pages_failed": len(failed_pages),
            "total_content_size": total_content_size,
            "html_directory": "raw_html",
            "metadata_file": "crawl_metadata.json",
            "urls_file": "page_urls.json",
            "quality_score_avg": round(crawl_metadata["average_quality_score"], 3)
        }
    
    # ───── Main entry point ─────
    @staticmethod
    async def run(browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Main entry point for the enhanced website scraper.
        
        Phase 1: Collect HTML pages with intelligent filtering and quality scoring.
        Use 'extract-content' task later for Phase 2 content extraction.
        """
        
        # Validate required parameters
        start_url = params.get("url", "").strip()
        if not start_url:
            raise ValueError("Parameter 'url' is required")
        
        # Optional parameters with defaults
        max_pages_str = str(params.get("max_pages", "")).strip()
        max_pages = None if max_pages_str in {"", "0", "-1"} else int(max_pages_str)
        
        use_browser = params.get("use_browser", False)  # Force browser rendering
        user_agent = params.get("user_agent", 
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        # Build request headers
        headers = {
            "user-agent": user_agent,
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "cache-control": "no-cache",
            "upgrade-insecure-requests": "1"
        }
        
        # Setup output directory
        out_dir = pathlib.Path(job_output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        _log(logger, "info", f"Starting website scrape: {start_url}")
        _log(logger, "info", f"Max pages: {max_pages or 'unlimited'}, Browser mode: {use_browser}")
        
        try:
            result = await ScrapeSiteTask._scrape_website(
                browser=browser,
                start_url=start_url,
                max_pages=max_pages,
                use_browser=use_browser,
                headers=headers,
                out_dir=out_dir,
                logger=logger
            )
            
            _log(logger, "info", f"Scrape completed: {result['pages_scraped']} pages, avg quality: {result['quality_score_avg']}")
            return result
            
        except Exception as e:
            _log(logger, "error", f"Scrape failed: {e}")
            raise