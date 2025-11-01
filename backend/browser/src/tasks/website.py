"""
WebsiteTask - RAG-optimized website scraper with comprehensive metadata extraction.

Designed to collect rich, structured website data suitable for RAG (Retrieval-Augmented Generation) systems.
Extracts content, metadata, images, resources, and document structure for downstream processing.
"""

import asyncio
import logging
import json
import random
import pathlib
import urllib.parse
import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import httpx
from bs4 import BeautifulSoup

from .base import _log


class WebsiteTask:
    """
    RAG-optimized website scraper with comprehensive data extraction.

    Features:
    - Page metadata extraction (title, description, author, dates)
    - Document structure analysis (H1-H6 hierarchy)
    - Quality scoring (text/HTML ratio, readability, content density)
    - Image downloading with metadata
    - Resource downloading (PDFs, docs, etc.)
    - Link graph building (internal/external relationships)
    - Per-page structured metadata output
    """

    # ═══════════════════════════════════════════════════════════════════════
    # URL MANAGEMENT UTILITIES
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _create_readable_filename(url: str) -> str:
        """Create a readable filename from URL."""
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
        url_lower = url.lower()

        # Basic domain check
        if base_domain not in url_lower:
            return True

        # Skip obvious non-content URLs
        skip_patterns = [
            '/search', '/login', '/register', '/cart', '/checkout', '/admin',
            '/api/', '/ajax/', '/.well-known/', '/wp-admin/', '/wp-json/',
            '.xml', '.json', '.rss', '.atom',
            '/contact', '/privacy', '/terms', '/sitemap', '/robots.txt',
            '/favicon.ico', '/apple-touch-icon', 'fluidicon.png',
            'join?', 'signup?', 'register?'  # Query parameter based signup links
        ]

        # Skip files with non-HTML extensions (except resources we want to download)
        skip_extensions = [
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
        if any(path.endswith(ext) for ext in skip_extensions):
            return True

        return any(pattern in url_lower for pattern in skip_patterns)

    @staticmethod
    def _is_downloadable_resource(url: str) -> bool:
        """Check if URL points to a downloadable resource (PDF, doc, etc.)."""
        resource_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in resource_extensions)

    # ═══════════════════════════════════════════════════════════════════════
    # METADATA EXTRACTION
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _extract_metadata(soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract comprehensive page metadata for RAG systems."""
        metadata = {
            "url": url,
            "title": None,
            "description": None,
            "author": None,
            "published_date": None,
            "modified_date": None,
            "language": None,
            "canonical_url": None,
            "keywords": []
        }

        # Extract title
        title_tag = soup.find('title')
        if title_tag:
            metadata["title"] = title_tag.get_text(strip=True)

        # Extract meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            # Meta description
            if meta.get('name', '').lower() == 'description':
                metadata["description"] = meta.get('content', '').strip()

            # Meta author
            elif meta.get('name', '').lower() == 'author':
                metadata["author"] = meta.get('content', '').strip()

            # Meta keywords
            elif meta.get('name', '').lower() == 'keywords':
                keywords = meta.get('content', '').strip()
                if keywords:
                    metadata["keywords"] = [k.strip() for k in keywords.split(',')]

            # Published date (article:published_time)
            elif meta.get('property', '').lower() == 'article:published_time':
                metadata["published_date"] = meta.get('content', '').strip()

            # Modified date (article:modified_time)
            elif meta.get('property', '').lower() == 'article:modified_time':
                metadata["modified_date"] = meta.get('content', '').strip()

            # Open Graph description (fallback)
            elif meta.get('property', '').lower() == 'og:description' and not metadata["description"]:
                metadata["description"] = meta.get('content', '').strip()

            # Open Graph title (fallback)
            elif meta.get('property', '').lower() == 'og:title' and not metadata["title"]:
                metadata["title"] = meta.get('content', '').strip()

        # Extract language from html tag
        html_tag = soup.find('html')
        if html_tag and html_tag.get('lang'):
            metadata["language"] = html_tag.get('lang').strip()

        # Extract canonical URL
        canonical = soup.find('link', {'rel': 'canonical'})
        if canonical and canonical.get('href'):
            metadata["canonical_url"] = canonical.get('href').strip()

        return metadata

    # ═══════════════════════════════════════════════════════════════════════
    # DOCUMENT STRUCTURE EXTRACTION
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _extract_document_structure(soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract heading hierarchy (H1-H6) for document structure understanding."""
        structure = {
            "h1": [],
            "h2": [],
            "h3": [],
            "h4": [],
            "h5": [],
            "h6": []
        }

        for level in range(1, 7):
            headers = soup.find_all(f'h{level}')
            structure[f'h{level}'] = [h.get_text(strip=True) for h in headers if h.get_text(strip=True)]

        return structure

    # ═══════════════════════════════════════════════════════════════════════
    # QUALITY SCORING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _calculate_quality_metrics(soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Calculate content quality metrics for RAG filtering."""
        # Extract plain text
        text = soup.get_text(separator=' ', strip=True)
        text_length = len(text)
        html_length = len(html)

        # Calculate text-to-HTML ratio
        text_html_ratio = text_length / html_length if html_length > 0 else 0

        # Word count
        words = re.findall(r'\b\w+\b', text)
        word_count = len(words)

        # Content density (text per KB of HTML)
        content_density = text_length / (html_length / 1024) if html_length > 0 else 0

        # Link density (ratio of link text to total text)
        links = soup.find_all('a')
        link_text_length = sum(len(link.get_text(strip=True)) for link in links)
        link_density = link_text_length / text_length if text_length > 0 else 0

        # Has meaningful content check
        has_meaningful_content = (
            word_count >= 100 and
            text_html_ratio >= 0.1 and
            link_density < 0.5
        )

        # Calculate overall quality score (0-1)
        quality_score = 0.0
        if has_meaningful_content:
            quality_score += 0.4  # Base score for having content
            quality_score += min(0.3, text_html_ratio)  # Up to 0.3 for good text/HTML ratio
            quality_score += min(0.2, word_count / 1000)  # Up to 0.2 for word count
            quality_score += max(0, 0.1 - link_density)  # Up to 0.1 for low link density

        return {
            "text_html_ratio": round(text_html_ratio, 3),
            "word_count": word_count,
            "content_density": round(content_density, 2),
            "link_density": round(link_density, 3),
            "has_meaningful_content": has_meaningful_content,
            "overall_score": round(quality_score, 3)
        }

    # ═══════════════════════════════════════════════════════════════════════
    # IMAGE EXTRACTION & DOWNLOADING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _extract_and_download_images(
        soup: BeautifulSoup,
        current_url: str,
        images_dir: pathlib.Path,
        client: httpx.AsyncClient,
        logger: logging.Logger
    ) -> List[Dict[str, Any]]:
        """Extract images and download them to images/ directory."""
        images_metadata = []
        img_tags = soup.find_all('img')

        for idx, img in enumerate(img_tags[:50]):  # Limit to 50 images per page
            img_src = img.get('src', '').strip()
            if not img_src:
                continue

            # Resolve relative URLs
            img_url = urllib.parse.urljoin(current_url, img_src)

            # Skip data URIs and invalid URLs
            if img_url.startswith('data:') or not img_url.startswith('http'):
                continue

            try:
                # Download image
                response = await client.get(img_url, timeout=10.0)
                response.raise_for_status()

                # Generate filename
                img_ext = pathlib.Path(urllib.parse.urlparse(img_url).path).suffix or '.jpg'
                img_filename = f"img_{idx}_{hashlib.md5(img_url.encode()).hexdigest()[:8]}{img_ext}"
                img_path = images_dir / img_filename

                # Save image
                img_path.write_bytes(response.content)

                # Extract metadata
                img_meta = {
                    "src": img_filename,
                    "original_url": img_url,
                    "alt": img.get('alt', '').strip(),
                    "title": img.get('title', '').strip(),
                    "size_bytes": len(response.content)
                }

                images_metadata.append(img_meta)

            except Exception as e:
                _log(logger, "warning", f"Failed to download image {img_url}: {e}")
                continue

        return images_metadata

    # ═══════════════════════════════════════════════════════════════════════
    # RESOURCE DOWNLOADING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _download_resources(
        soup: BeautifulSoup,
        current_url: str,
        resources_dir: pathlib.Path,
        client: httpx.AsyncClient,
        logger: logging.Logger
    ) -> List[Dict[str, Any]]:
        """Download linked resources (PDFs, docs, etc.)."""
        resources_metadata = []
        links = soup.find_all('a', href=True)

        for link in links:
            href = link.get('href', '').strip()
            if not href:
                continue

            # Resolve relative URLs
            resource_url = urllib.parse.urljoin(current_url, href)

            # Check if it's a downloadable resource
            if not WebsiteTask._is_downloadable_resource(resource_url):
                continue

            try:
                # Download resource
                response = await client.get(resource_url, timeout=30.0)
                response.raise_for_status()

                # Generate filename
                resource_filename = pathlib.Path(urllib.parse.urlparse(resource_url).path).name
                if not resource_filename:
                    resource_filename = f"resource_{hashlib.md5(resource_url.encode()).hexdigest()[:8]}.pdf"

                resource_path = resources_dir / resource_filename

                # Save resource
                resource_path.write_bytes(response.content)

                # Extract metadata
                resource_meta = {
                    "filename": resource_filename,
                    "original_url": resource_url,
                    "type": response.headers.get('content-type', 'application/octet-stream'),
                    "size_bytes": len(response.content),
                    "anchor_text": link.get_text(strip=True)
                }

                resources_metadata.append(resource_meta)
                _log(logger, "info", f"Downloaded resource: {resource_filename} ({len(response.content)} bytes)")

            except Exception as e:
                _log(logger, "warning", f"Failed to download resource {resource_url}: {e}")
                continue

        return resources_metadata

    # ═══════════════════════════════════════════════════════════════════════
    # LINK GRAPH BUILDING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _extract_links(soup: BeautifulSoup, current_url: str, base_domain: str) -> Dict[str, List[Dict[str, str]]]:
        """Extract internal and external links with context."""
        links = {
            "internal": [],
            "external": []
        }

        link_tags = soup.find_all('a', href=True)

        for link in link_tags:
            href = link.get('href', '').strip()
            if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                continue

            # Resolve relative URLs
            absolute_url = urllib.parse.urljoin(current_url, href)

            # Extract link metadata
            link_meta = {
                "url": absolute_url,
                "anchor": link.get_text(strip=True)[:200],  # Limit anchor text length
                "title": link.get('title', '').strip()
            }

            # Classify as internal or external
            if base_domain in absolute_url:
                links["internal"].append(link_meta)
            else:
                links["external"].append(link_meta)

        return links

    # ═══════════════════════════════════════════════════════════════════════
    # URL EXTRACTION FOR CRAWLING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _extract_urls_from_page(soup: BeautifulSoup, current_url: str, base_domain: str, start_url: str = None) -> List[str]:
        """Extract valid URLs from a page for crawling."""
        valid_links = []
        links = soup.find_all('a', href=True)

        for link in links:
            href = link.get('href', '').strip()
            if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:'):
                continue

            try:
                # Resolve relative URLs
                absolute_url = urllib.parse.urljoin(current_url, href)

                # Only crawl internal pages
                if not WebsiteTask._should_skip_url(absolute_url, base_domain, start_url):
                    valid_links.append(absolute_url)
            except Exception:
                continue

        return valid_links

    # ═══════════════════════════════════════════════════════════════════════
    # CONTENT FETCHING
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def _fetch_page_html(
        client: httpx.AsyncClient,
        url: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Fetch a single page's HTML with proper error handling."""
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
        """Fetch page using Playwright for JavaScript-heavy sites."""
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

    # ═══════════════════════════════════════════════════════════════════════
    # MAIN SCRAPING LOGIC
    # ═══════════════════════════════════════════════════════════════════════

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
        """Execute the main scraping logic with full RAG data collection."""
        # Basic domain extraction
        parsed = urllib.parse.urlparse(start_url)
        base_domain = parsed.netloc

        if not base_domain:
            raise ValueError(f"Invalid start URL: {start_url}")

        # Create output directories
        html_dir = out_dir / "raw_html"
        images_dir = out_dir / "images"
        resources_dir = out_dir / "resources"
        metadata_dir = out_dir / "metadata"

        html_dir.mkdir(parents=True, exist_ok=True)
        images_dir.mkdir(parents=True, exist_ok=True)
        resources_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)

        # State tracking
        url_queue = [start_url]
        processed_urls = set()
        successful_pages = []
        failed_pages = []
        total_content_size = 0
        link_graph = {}  # URL -> {internal: [...], external: [...]}

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
                fetch_metadata = {}

                if not use_browser:
                    success, html, fetch_metadata = await WebsiteTask._fetch_page_html(
                        client, current_url, headers, logger
                    )

                # Fallback to browser for failed requests or when explicitly requested
                if not success and browser:
                    success, html, fetch_metadata = await WebsiteTask._fetch_with_browser(
                        browser, current_url, headers, logger
                    )

                if not success:
                    failed_pages.append(fetch_metadata)
                    _log(logger, "warning", f"Failed to fetch {current_url}: {fetch_metadata.get('error', 'unknown')}")
                    continue

                # Parse HTML with BeautifulSoup
                try:
                    soup = BeautifulSoup(html, 'html.parser')
                except Exception as e:
                    _log(logger, "error", f"Failed to parse HTML for {current_url}: {e}")
                    failed_pages.append({"url": current_url, "error": f"parse_failed_{e}"})
                    continue

                # Extract all metadata
                page_metadata = WebsiteTask._extract_metadata(soup, current_url)
                document_structure = WebsiteTask._extract_document_structure(soup)
                quality_metrics = WebsiteTask._calculate_quality_metrics(soup, html)
                links = WebsiteTask._extract_links(soup, current_url, base_domain)

                # Download images
                images_metadata = await WebsiteTask._extract_and_download_images(
                    soup, current_url, images_dir, client, logger
                )

                # Download resources
                resources_metadata = await WebsiteTask._download_resources(
                    soup, current_url, resources_dir, client, logger
                )

                # Save HTML with readable filename
                safe_filename = WebsiteTask._create_readable_filename(current_url)
                html_file = html_dir / f"{safe_filename}.html"

                try:
                    html_file.write_text(html, encoding='utf-8')
                    file_size = len(html.encode('utf-8'))
                except Exception as e:
                    _log(logger, "error", f"Failed to save {current_url}: {e}")
                    failed_pages.append({"url": current_url, "error": f"save_failed_{e}"})
                    continue

                # Save per-page metadata JSON
                page_meta_file = metadata_dir / f"{safe_filename}_meta.json"
                full_page_metadata = {
                    **page_metadata,
                    "headers": document_structure,
                    "images": images_metadata,
                    "resources": resources_metadata,
                    "links": links,
                    "quality_metrics": quality_metrics,
                    "scraped_at": datetime.utcnow().isoformat() + "Z",
                    "html_file": f"raw_html/{safe_filename}.html"
                }

                try:
                    page_meta_file.write_text(json.dumps(full_page_metadata, indent=2), encoding='utf-8')
                except Exception as e:
                    _log(logger, "error", f"Failed to save metadata for {current_url}: {e}")

                # Track success
                page_info = {
                    "url": current_url,
                    "file": f"{safe_filename}.html",
                    "metadata_file": f"{safe_filename}_meta.json",
                    "quality_score": quality_metrics["overall_score"],
                    "size": file_size,
                    "images_count": len(images_metadata),
                    "resources_count": len(resources_metadata),
                    **fetch_metadata
                }
                successful_pages.append(page_info)
                total_content_size += file_size

                # Store link graph
                link_graph[current_url] = links

                # Extract new URLs for crawling
                if len(successful_pages) < (max_pages or float('inf')):
                    new_urls = WebsiteTask._extract_urls_from_page(soup, current_url, base_domain, start_url)

                    for new_url in new_urls:
                        if new_url not in processed_urls and new_url not in url_queue:
                            url_queue.append(new_url)

                # Rate limiting
                await asyncio.sleep(random.uniform(1.0, 2.5))

        # Save link graph
        link_graph_file = out_dir / "link_graph.json"
        try:
            link_graph_file.write_text(json.dumps(link_graph, indent=2), encoding='utf-8')
        except Exception as e:
            _log(logger, "error", f"Failed to save link graph: {e}")

        # Save crawl summary
        crawl_summary = {
            "start_url": start_url,
            "base_domain": base_domain,
            "total_pages_found": len(processed_urls),
            "successful_pages": len(successful_pages),
            "failed_pages": len(failed_pages),
            "total_content_size": total_content_size,
            "total_images": sum(p["images_count"] for p in successful_pages),
            "total_resources": sum(p["resources_count"] for p in successful_pages),
            "average_quality_score": sum(p["quality_score"] for p in successful_pages) / len(successful_pages) if successful_pages else 0,
            "crawled_at": datetime.utcnow().isoformat() + "Z",
            "pages": successful_pages,
            "failures": failed_pages[:50]  # Limit failure details
        }

        # Save summary files
        (out_dir / "crawl_summary.json").write_text(json.dumps(crawl_summary, indent=2), encoding='utf-8')
        (out_dir / "page_urls.json").write_text(json.dumps([p["url"] for p in successful_pages], indent=2), encoding='utf-8')

        return {
            "start_url": start_url,
            "base_domain": base_domain,
            "pages_scraped": len(successful_pages),
            "pages_failed": len(failed_pages),
            "total_content_size": total_content_size,
            "total_images": crawl_summary["total_images"],
            "total_resources": crawl_summary["total_resources"],
            "html_directory": "raw_html",
            "images_directory": "images",
            "resources_directory": "resources",
            "metadata_directory": "metadata",
            "summary_file": "crawl_summary.json",
            "urls_file": "page_urls.json",
            "link_graph_file": "link_graph.json",
            "quality_score_avg": round(crawl_summary["average_quality_score"], 3)
        }

    # ═══════════════════════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    async def run(browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Main entry point for RAG-optimized website scraper.

        Parameters:
        - url: Starting URL to scrape (required)
        - max_pages: Maximum pages to scrape (optional, default: unlimited)
        - use_browser: Force browser rendering for all pages (optional, default: false)
        - user_agent: Custom user agent (optional)

        Output Structure:
        - raw_html/: Original HTML files
        - images/: Downloaded images
        - resources/: Downloaded documents (PDFs, docs, etc.)
        - metadata/: Per-page metadata JSON files
        - crawl_summary.json: Overall crawl statistics
        - page_urls.json: List of all scraped URLs
        - link_graph.json: Page relationship graph
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

        _log(logger, "info", f"Starting RAG-optimized website scrape: {start_url}")
        _log(logger, "info", f"Max pages: {max_pages or 'unlimited'}, Browser mode: {use_browser}")

        try:
            result = await WebsiteTask._scrape_website(
                browser=browser,
                start_url=start_url,
                max_pages=max_pages,
                use_browser=use_browser,
                headers=headers,
                out_dir=out_dir,
                logger=logger
            )

            _log(logger, "info", f"Scrape completed: {result['pages_scraped']} pages, {result['total_images']} images, {result['total_resources']} resources")
            _log(logger, "info", f"Average quality score: {result['quality_score_avg']}")
            return result

        except Exception as e:
            _log(logger, "error", f"Scrape failed: {e}")
            raise
