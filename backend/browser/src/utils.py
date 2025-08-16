from __future__ import annotations
import asyncio
import hashlib
import json
import os
import pathlib
import random
import re
import tempfile
import urllib.parse
from typing import Any, Dict, List, Optional, Set, Tuple
import httpx

# ───────── content sniffing & MIME helpers (from utils_content) ─────────

def classify_payload(head: bytes, content_type: str | None) -> str:
    """Identify likely content type from header bytes and MIME."""
    if head.startswith(b"PK\x03\x04"):
        return "xlsx_zip"
    if head.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
        return "xls_ole"

    lower = head.lower()
    if lower.startswith(b"<!doctype html") or lower.startswith(b"<html") \
       or b"/tspd/" in lower or b"<apm_do_not_touch>" in lower:
        return "html_interstitial"

    ct = (content_type or "").split(";")[0].strip().lower()
    text = head.decode("utf-8", "ignore")
    if ct in {"application/json", "application/ld+json"} or text.lstrip().startswith(("{", "[")):
        return "json_text"
    if ct in {"application/xml", "text/xml"} or text.lstrip().startswith(("<?xml", "<root", "<dataset")):
        return "xml_text"
    if ct in {"text/csv", "application/csv", "text/plain"}:
        return "csv_text" if (("," in text or ";" in text or "\t" in text) and ("\n" in text)) else "text_plain"
    if (("," in text or ";" in text or "\t" in text) and ("\n" in text)):
        return "csv_text"
    return "binary/unknown"


def ext_from_content_type(ct: str | None) -> str:
    """Map MIME type to an extension if specific."""
    ct = (ct or "").split(";")[0].strip().lower()
    mapping = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel": ".xls",
        "text/csv": ".csv",
        "application/csv": ".csv",
        "application/json": ".json",
        "application/xml": ".xml",
        "text/xml": ".xml",
        "text/plain": ".txt",
        "application/zip": ".zip",
    }
    return mapping.get(ct, "")

# ───────── filename helpers (from utils_files) ─────────

def safe_name(s: str) -> str:
    s = (s or "").strip().replace(os.sep, "_")
    s = re.sub(r"[^\w\-.]+", "_", s).strip("._")
    return s[:180] or "file"

# ───────── HTTP headers (from utils_http) ─────────

def build_download_headers(base: dict) -> dict:
    """Build consistent headers for binary downloads."""
    ua = base.get("user-agent") or base.get("User-Agent") or (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    )
    lang = base.get("accept-language") or "en-US,en;q=0.9,ar;q=0.8"
    h = {
        "user-agent": ua,
        "accept": "*/*",
        "accept-language": lang,
        "cache-control": "no-cache",
        "pragma": "no-cache",
    }
    if base.get("referer"):
        h["referer"] = base["referer"]
    return h


# ───────── URL utilities (reusable across tasks) ─────────

def normalize_url(url: str, base_url: Optional[str] = None) -> str:
    """Normalize URL by removing fragments and ensuring proper encoding."""
    if not url:
        return ""
    
    # Add protocol if missing
    if not url.lower().startswith(('http://', 'https://', '//')):
        url = 'https://' + url
    
    # Convert relative to absolute if base_url provided
    if base_url and not url.lower().startswith(('http://', 'https://')):
        url = urllib.parse.urljoin(base_url, url)
    
    # Parse and reconstruct to normalize
    parsed = urllib.parse.urlparse(url)
    normalized = urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, urllib.parse.quote(parsed.path, safe="/"), 
         parsed.params, parsed.query, "")  # Remove fragment
    )
    return normalized


def is_same_domain(url1: str, url2: str) -> bool:
    """Check if two URLs belong to the same domain."""
    try:
        domain1 = urllib.parse.urlparse(url1).netloc.lower()
        domain2 = urllib.parse.urlparse(url2).netloc.lower()
        return domain1 == domain2
    except Exception:
        return False


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""


# ───────── HTTP utilities with retry logic ─────────

async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    timeout: float = 30.0
) -> Tuple[bool, Optional[httpx.Response], Optional[str]]:
    """
    Fetch URL with exponential backoff retry logic.
    Returns: (success, response, error_reason)
    """
    delay = initial_delay
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url, headers=headers or {}, timeout=timeout)
            if response.status_code == 200:
                return True, response, None
            elif response.status_code in {502, 503, 504, 522}:  # Transient errors
                last_error = f"http_{response.status_code}"
            else:
                return False, response, f"http_{response.status_code}"
        except (httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout):
            last_error = "timeout"
        except httpx.RequestError as e:
            last_error = f"request_error_{e.__class__.__name__}"
        except Exception as e:
            last_error = f"exception_{type(e).__name__}"
        
        if attempt < max_retries:
            await asyncio.sleep(delay + random.uniform(0, 0.3))
            delay *= 1.5
    
    return False, None, last_error


# ───────── File operations with atomic saves ─────────

async def save_content_atomic(
    file_path: pathlib.Path,
    content: bytes,
    create_dirs: bool = True
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """
    Save content atomically using .part files.
    Returns: (success, error_reason, metadata)
    """
    try:
        if create_dirs:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temp file first
        temp_path = file_path.with_suffix(file_path.suffix + ".part")
        
        with open(temp_path, "wb") as f:
            f.write(content)
        
        if temp_path.stat().st_size == 0:
            temp_path.unlink(missing_ok=True)
            return False, "empty_file", {}
        
        # Atomic move
        temp_path.replace(file_path)
        
        # Generate metadata
        size = file_path.stat().st_size
        sha256 = hashlib.sha256(content).hexdigest()
        
        return True, None, {"size": size, "sha256": sha256}
        
    except Exception as e:
        return False, str(e), {}


# ───────── Link extraction utilities ─────────

def extract_links_from_html(html: str, base_url: str) -> List[str]:
    """Extract and normalize all links from HTML content."""
    links = []
    
    # Simple regex for href attributes (more robust than current implementation)
    href_pattern = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
    
    for match in href_pattern.finditer(html):
        href = match.group(1).strip()
        if not href or href.startswith(('#', 'javascript:', 'mailto:')):
            continue
            
        try:
            normalized = normalize_url(href, base_url)
            if normalized and normalized.startswith(('http://', 'https://')):
                links.append(normalized)
        except Exception:
            continue
    
    return list(set(links))  # Remove duplicates


# ───────── Content filtering utilities ─────────

def is_content_page(url: str, html: str) -> bool:
    """
    Determine if a page likely contains valuable content for vector stores.
    Filters out navigation pages, search results, login forms, etc.
    """
    url_lower = url.lower()
    
    # Skip obvious non-content URLs
    skip_patterns = [
        '/search', '/login', '/register', '/cart', '/checkout',
        '/admin', '/api/', '.xml', '.json', '.pdf', '.doc',
        '/contact', '/privacy', '/terms', '/sitemap'
    ]
    
    if any(pattern in url_lower for pattern in skip_patterns):
        return False
    
    # Check content indicators
    html_lower = html.lower()
    
    # Special handling for GitHub and code repositories
    if 'github.com' in url_lower:
        # GitHub pages are valuable even with lots of navigation
        # Check for repository content indicators
        github_content_indicators = [
            'readme', 'blob/', 'tree/', 'commits', 'issues', 'wiki',
            'repository', 'code', '.md', '.py', '.js', '.html', '.css'
        ]
        if any(indicator in url_lower or indicator in html_lower for indicator in github_content_indicators):
            return True
        # Even main repo pages are valuable
        if '/tree/' in url_lower or '/blob/' in url_lower or url_lower.count('/') <= 4:
            return True
    
    # Skip if it's mostly navigation/UI (but be more lenient for code sites)
    nav_count = html_lower.count('<nav')
    menu_count = html_lower.count('menu')
    if nav_count > 5 and menu_count > 15:  # More lenient thresholds
        return False
    
    # Check for substantial text content
    import re
    text_content = re.sub(r'<[^>]+>', ' ', html)
    word_count = len(text_content.split())
    
    # Lower threshold for code repositories and technical sites
    min_words = 50 if any(domain in url_lower for domain in ['github.com', 'gitlab.com', 'docs.']) else 100
    
    return word_count > min_words


# ───────── Content quality scoring ─────────

def score_content_quality(html: str) -> float:
    """
    Score content quality from 0.0 to 1.0 for prioritization.
    Higher scores indicate more valuable content for vector stores.
    """
    score = 0.0
    html_lower = html.lower()
    
    # Text density (good indicator of article content)
    import re
    text_content = re.sub(r'<[^>]+>', ' ', html)
    text_length = len(text_content.strip())
    html_length = len(html)
    
    if html_length > 0:
        text_density = text_length / html_length
        score += min(text_density * 2, 0.4)  # Max 0.4 points
    
    # Content structure indicators
    if '<article' in html_lower:
        score += 0.2
    if '<main' in html_lower:
        score += 0.1
    if html_lower.count('<p>') > 3:
        score += 0.2
    if html_lower.count('<h') > 0:  # Headers indicate structure
        score += 0.1
    
    # Special scoring for code repositories (GitHub, GitLab, etc.)
    code_indicators = [
        'readme', 'blob/', 'tree/', '.md', '.py', '.js', '.html', '.css',
        'commits', 'issues', 'wiki', 'repository', 'code', 'class=', 'function'
    ]
    if any(indicator in html_lower for indicator in code_indicators):
        score += 0.2  # Boost for code-related content
    
    # Boost for files and documentation
    if any(ext in html_lower for ext in ['.md', '.rst', '.txt', 'readme', 'license']):
        score += 0.1
    
    return min(score, 1.0)


# ───────── JSON utilities ─────────

def save_json_atomic(file_path: pathlib.Path, data: Any, create_dirs: bool = True) -> bool:
    """Save JSON data atomically with proper encoding."""
    try:
        if create_dirs:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
        temp_path = file_path.with_suffix(file_path.suffix + ".part")
        
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        temp_path.replace(file_path)
        return True
    except Exception:
        return False
