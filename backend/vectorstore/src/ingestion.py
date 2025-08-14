"""Document ingestion for the vector service.

This module defines functions and classes used to ingest content from
web pages and open data portals into the vector store.  It is designed
to be called by the FastAPI application running in ``main.py``.

Ingestion respects politeness by setting a reasonable User‑Agent and
handling HTTP errors gracefully.  For complex, JavaScript‑heavy sites
you may choose to extend this module with a Selenium‑based scraper; the
current implementation is intentionally simple.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Optional, Set

import pandas as pd  # type: ignore
import requests
from bs4 import BeautifulSoup  # type: ignore

# Reuse a single session for all requests to benefit from connection pooling
SESSION = requests.Session()
# Set a polite default User‑Agent
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; AgenticPlatform/1.0; +https://example.com)"
})


def fetch_text_from_url(url: str, verify: bool = True) -> str:
    """Download a URL and return its visible text.

    This helper requests the given URL and parses the response body with
    BeautifulSoup, returning a single concatenated string of the text
    content.  Script and style tags are removed.  Network errors
    propagate as exceptions.
    """
    response = SESSION.get(url, timeout=15, verify=verify)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    text = soup.get_text(separator="\n")
    return text.strip()


def find_csv_links_on_page(url: str) -> list[str]:
    """Find CSV file links on an open data portal page.

    Open data portals often list downloadable resources as anchor tags
    containing ``.csv`` in the href.  This helper returns all such
    absolute URLs.  Relative links are resolved against the base URL.
    """
    response = SESSION.get(url, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.csv' in href.lower():
            links.append(requests.compat.urljoin(url, href))
    return links


def download_tabular_as_text(url: str) -> list[str]:
    """Download a CSV or Excel file and convert each row into a textual record.

    The function infers the file type from the URL extension.  CSV files
    are read via ``pandas.read_csv`` or a fallback CSV reader; Excel
    files are read via ``pandas.read_excel``.  Unsupported formats are
    ignored.  Each row is converted into a comma‑separated string.  If
    loading fails, the exception is propagated to the caller.
    """
    lower = url.lower()
    try:
        if lower.endswith('.csv'):
            df = pd.read_csv(url, encoding='utf-8', on_bad_lines='skip')
        elif lower.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(url, engine='openpyxl' if lower.endswith('.xlsx') else None)
        else:
            raise ValueError(f"Unsupported file format for {url}")
    except Exception:
        # Fallback to raw download and CSV reader
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.content
        # Try to load Excel if file extension hints at it
        if lower.endswith(('.xls', '.xlsx')):
            import tempfile
            tmp = tempfile.NamedTemporaryFile(delete=False)
            tmp.write(content)
            tmp.close()
            df = pd.read_excel(tmp.name)
            os.unlink(tmp.name)
        else:
            text = content.decode('utf-8', errors='ignore')
            reader = csv.reader(io.StringIO(text))
            df = pd.DataFrame(list(reader))
    text_rows = []
    for _, row in df.iterrows():
        values = [str(val) for val in row.tolist() if pd.notna(val)]
        if values:
            text_rows.append(", ".join(values))
    return text_rows


def scrape_open_data(url: str) -> list[str]:
    """Scrape the Saudi open data portal to extract dataset records.

    This helper handles two kinds of URLs:

    * Publisher URLs of the form ``.../publishers/<id>``.  These pages
      list multiple datasets across paginated pages.  Each dataset link
      is followed to its resources page, where downloadable files are
      discovered.
    * Dataset view URLs of the form ``.../datasets/view/<uuid>``.  These
      pages are forwarded to their ``/resources`` subpage to locate
      downloadable files.

    Supported resource formats are CSV and Excel (xls/xlsx).  Each
    downloaded table is converted into a list of comma‑joined row
    strings.  If no resources are found the HTML text of the page is
    returned.
    """
    docs: list[str] = []
    visited: Set[str] = set()

    def process_dataset(dataset_url: str) -> None:
        """Download all resources for a single dataset."""
        resources_url = dataset_url.rstrip('/') + "/resources"
        try:
            resp = SESSION.get(resources_url, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            logging.error(f"Failed to fetch resources page {resources_url}: {exc}")
            return
        soup = BeautifulSoup(resp.text, "html.parser")
        # Find links to downloadable resources
        for a in soup.find_all('a', href=True):
            href = a['href']
            lower = href.lower()
            if any(lower.endswith(ext) for ext in ['.csv', '.xls', '.xlsx']):
                file_url = requests.compat.urljoin(resources_url, href)
                try:
                    docs.extend(download_tabular_as_text(file_url))
                except Exception as exc:
                    logging.error(f"Failed to download resource {file_url}: {exc}")
        # If no downloadable files were processed, fall back to dataset page text
        if not docs:
            try:
                docs.append(fetch_text_from_url(dataset_url))
            except Exception as exc:
                logging.error(f"Failed to fetch dataset page {dataset_url}: {exc}")

    # Publisher pages list multiple datasets with pagination
    if '/publishers/' in url:
        page = 1
        while True:
            page_url = url if page == 1 else f"{url}?page={page}"
            try:
                resp = SESSION.get(page_url, timeout=20)
                resp.raise_for_status()
            except Exception as exc:
                logging.error(f"Failed to fetch publisher page {page_url}: {exc}")
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            dataset_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '/datasets/view/' in href:
                    dataset_links.append(requests.compat.urljoin(page_url, href))
            # Break if no dataset links found
            if not dataset_links:
                break
            for d_url in dataset_links:
                if d_url not in visited:
                    visited.add(d_url)
                    process_dataset(d_url)
            page += 1
        return docs
    # Dataset view pages
    if '/datasets/view/' in url:
        process_dataset(url)
        return docs
    # Otherwise treat as generic open data page (fallback to previous behaviour)
    return scrape_generic_site(url)


def scrape_generic_site(url: str) -> list[str]:
    """Scrape a generic website and return its text in chunks.

    A single web page may contain a large amount of text.  To improve
    search quality we split the text into paragraphs separated by
    blank lines.  Each paragraph becomes a document in the vector
    store.  Empty paragraphs are ignored.
    """
    try:
        text = fetch_text_from_url(url)
    except Exception as exc:
        logging.error(f"Failed to fetch {url}: {exc}")
        return []
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    return paragraphs


@dataclass
class IngestionResult:
    """Return value for ingestion operations."""
    count: int
    errors: list[str]


def ingest_sources(
    sources: Iterable[str],
    case: str,
    vector_store: Any,
    logger: logging.Logger,
    verify_ssl: bool = True,
) -> IngestionResult:
    """Ingest a collection of sources into the given vector store.

    Parameters
    ----------
    sources : Iterable[str]
        URLs or identifiers.  If a source belongs to the Saudi open data
        portal (contains ``open.data.gov.sa``) the ingestion routine
        attempts to locate CSV resources and download them.  Otherwise
        the source is treated as a generic website and its text is
        extracted.
    case : str
        A string identifying the current case.  Passed to the logger to
        separate per‑case logs.
    vector_store : object
        An object implementing ``add_documents(texts, metadatas)``.
    logger : logging.Logger
        The logger configured for this ingestion.  All errors are
        appended to the return value and logged.
    verify_ssl : bool, optional
        Whether to verify SSL certificates when making HTTP requests.
        Default is ``True``; set to ``False`` to ignore certificate
        errors.

    Returns
    -------
    IngestionResult
        The number of documents added and a list of error messages.
    """
    errors: list[str] = []
    docs: list[str] = []
    metadatas: list[Dict[str, Any]] = []
    for src in sources:
        src = src.strip()
        if not src:
            continue
        try:
            if 'open.data.gov.sa' in src:
                texts = scrape_open_data(src)
            else:
                texts = scrape_generic_site(src)
        except Exception as exc:
            err_msg = f"Error scraping {src}: {exc}"
            logger.error(err_msg)
            errors.append(err_msg)
            continue
        for t in texts:
            docs.append(t)
            metadatas.append({
                "source": src,
                "case": case
            })
    if docs:
        vector_store.add_documents(docs, metadatas)
    return IngestionResult(count=len(docs), errors=errors)