"""
SaudiOpenDataTask - Saudi Open Data portal scraping and dataset collection.

This task handles downloading datasets from the Saudi government's open data portal.
"""

import asyncio
import logging
import json
import random
import pathlib
import hashlib
import os
import tempfile
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple
import httpx

from .base import _log


class SaudiOpenDataTask:
    """Saudi Open Data portal scraper for automated dataset collection."""
    
    BASE = "https://open.data.gov.sa"
    
    @staticmethod
    def _create_safe_filename(name: str) -> str:
        """Create a safe filename from a resource name."""
        if not name:
            return "file"
        
        # Basic filename sanitization
        safe_chars = []
        for char in name:
            if char.isalnum() or char in (' ', '_', '-', '.'):
                safe_chars.append(char)
            else:
                safe_chars.append('_')
        
        result = ''.join(safe_chars).strip()
        return result if result else "file"
    
    @staticmethod
    async def _fetch_datasets_for_publisher(client: httpx.AsyncClient, publisher_id: str, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Fetch list of datasets for a specific publisher."""
        try:
            url = f"https://open.data.gov.sa/data/api/organizations?version=-1&organization={publisher_id}"
            headers = {"accept": "application/json"}
            
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            datasets = data.get("datasets", [])
            
            result = []
            for dataset in datasets:
                result.append({
                    "id": dataset.get("id"),
                    "title": dataset.get("titleEn") or dataset.get("titleAr") or "Unknown",
                    "url": f"https://open.data.gov.sa/en/datasets/view/{dataset.get('id')}/resources"
                })
            
            _log(logger, "info", f"Found {len(result)} datasets for publisher {publisher_id}")
            return result
            
        except Exception as e:
            _log(logger, "error", f"Failed to fetch datasets for publisher {publisher_id}: {e}")
            return []
    
    @staticmethod
    async def _fetch_dataset_resources(client: httpx.AsyncClient, dataset_id: str, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Fetch resources (downloadable files) for a specific dataset."""
        try:
            # Try the primary API endpoint
            url = f"https://open.data.gov.sa/data/api/datasets/resources?version=-1&dataset={dataset_id}"
            headers = {
                "accept": "application/json",
                "x-requested-with": "XMLHttpRequest",
                "x-security-request": "required",
                "referer": f"https://open.data.gov.sa/en/datasets/view/{dataset_id}/resources"
            }
            
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and "resources" in data:
                    return data["resources"]
            
            # Fallback to alternative endpoint
            url2 = f"https://open.data.gov.sa/api/datasets/{dataset_id}"
            response2 = await client.get(url2, headers={"accept": "application/json"})
            if response2.status_code == 200:
                data2 = response2.json()
                if isinstance(data2, dict) and "resources" in data2:
                    return data2["resources"]
            
            _log(logger, "warning", f"No resources found for dataset {dataset_id}")
            return []
            
        except Exception as e:
            _log(logger, "error", f"Failed to fetch resources for dataset {dataset_id}: {e}")
            return []
    
    @staticmethod
    async def _download_resource(
        client: httpx.AsyncClient,
        resource: Dict[str, Any],
        dataset_id: str,
        output_dir: pathlib.Path,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Download a single resource file."""
        try:
            # Get download URL
            resource_id = resource.get("resourceID") or resource.get("id")
            download_url = resource.get("downloadUrl") or resource.get("url")
            
            if not download_url:
                return {"status": "error", "reason": "no_download_url"}
            
            # Normalize URL
            if not download_url.startswith("http"):
                download_url = f"https://open.data.gov.sa/data/{download_url.lstrip('/')}"
            
            # Prepare headers
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "referer": f"https://open.data.gov.sa/en/datasets/view/{dataset_id}/resources"
            }
            
            # Download the file
            response = await client.get(download_url, headers=headers)
            response.raise_for_status()
            
            # Determine filename
            filename = "download"
            if "content-disposition" in response.headers:
                cd = response.headers["content-disposition"]
                if "filename=" in cd:
                    filename = cd.split("filename=")[1].strip('"; ')
            
            if not filename or filename == "download":
                # Use resource name or URL path
                name = resource.get("name") or resource.get("titleEn") or resource.get("titleAr")
                if name:
                    filename = SaudiOpenDataTask._create_safe_filename(name)
                else:
                    path = urllib.parse.urlparse(download_url).path
                    filename = os.path.basename(path) or f"resource_{resource_id}"
            
            # Add extension if missing
            if not "." in filename:
                content_type = response.headers.get("content-type", "")
                if "json" in content_type:
                    filename += ".json"
                elif "csv" in content_type:
                    filename += ".csv"
                elif "xml" in content_type:
                    filename += ".xml"
                elif "excel" in content_type or "spreadsheet" in content_type:
                    filename += ".xlsx"
            
            # Save file
            file_path = output_dir / filename
            content = response.content
            
            # Basic content validation
            if len(content) == 0:
                return {"status": "error", "reason": "empty_file"}
            
            # Check for HTML error pages
            if content.startswith(b"<!DOCTYPE html") or content.startswith(b"<html"):
                return {"status": "error", "reason": "html_error_page"}
            
            file_path.write_bytes(content)
            file_size = len(content)
            file_hash = hashlib.sha256(content).hexdigest()
            
            return {
                "status": "success",
                "filename": filename,
                "size": file_size,
                "sha256": file_hash,
                "url": download_url,
                "resource_name": resource.get("name") or resource.get("titleEn") or resource.get("titleAr")
            }
            
        except Exception as e:
            return {
                "status": "error", 
                "reason": f"download_failed: {type(e).__name__}",
                "error": str(e)
            }
    
    @staticmethod
    async def _process_dataset(
        client: httpx.AsyncClient,
        dataset_info: Dict[str, Any],
        output_dir: pathlib.Path,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Process a single dataset - fetch resources and download files."""
        dataset_id = dataset_info["id"]
        dataset_title = dataset_info["title"]
        
        _log(logger, "info", f"Processing dataset: {dataset_title} ({dataset_id})")
        
        # Create dataset directory
        dataset_dir = output_dir / dataset_id
        downloads_dir = dataset_dir / "downloads"
        downloads_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Fetch resources
            resources = await SaudiOpenDataTask._fetch_dataset_resources(client, dataset_id, logger)
            
            if not resources:
                return {
                    "dataset_id": dataset_id,
                    "dataset_title": dataset_title,
                    "status": "error",
                    "reason": "no_resources",
                    "total_resources": 0,
                    "downloaded": 0,
                    "failed": 0
                }
            
            # Save resources metadata
            (dataset_dir / "resources.json").write_text(
                json.dumps(resources, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            
            # Download each resource
            download_results = []
            success_count = 0
            
            for resource in resources:
                result = await SaudiOpenDataTask._download_resource(
                    client, resource, dataset_id, downloads_dir, logger
                )
                download_results.append(result)
                
                if result["status"] == "success":
                    success_count += 1
                    _log(logger, "info", f"✅ Downloaded: {result['filename']}")
                else:
                    _log(logger, "warning", f"❌ Failed: {result.get('reason', 'unknown')}")
                
                # Rate limiting
                await asyncio.sleep(random.uniform(0.5, 1.0))
            
            # Save download results
            (dataset_dir / "downloads.json").write_text(
                json.dumps(download_results, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            
            return {
                "dataset_id": dataset_id,
                "dataset_title": dataset_title,
                "dataset_url": f"https://open.data.gov.sa/en/datasets/view/{dataset_id}/resources",
                "status": "success",
                "total_resources": len(resources),
                "downloaded": success_count,
                "failed": len(resources) - success_count,
                "resources_json": "resources.json",
                "downloads_json": "downloads.json"
            }
            
        except Exception as e:
            _log(logger, "error", f"Dataset processing failed for {dataset_id}: {e}")
            return {
                "dataset_id": dataset_id,
                "dataset_title": dataset_title,
                "status": "error",
                "reason": str(e),
                "total_resources": 0,
                "downloaded": 0,
                "failed": 0
            }
    
    @staticmethod
    async def run(browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Main entry point for Saudi Open Data scraping.
        
        Supports:
        - Single dataset download: {"dataset_id": "123"}
        - Publisher datasets: {"publisher_id": "456"}
        - With optional filtering: {"dataset_limit": 10, "dataset_range": [0, 5]}
        """
        
        output_dir = pathlib.Path(job_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timeout = httpx.Timeout(60.0, connect=30.0)
        
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            
            # Single dataset mode
            if "dataset_id" in params:
                dataset_id = params["dataset_id"]
                dataset_info = {"id": dataset_id, "title": f"Dataset {dataset_id}"}
                
                result = await SaudiOpenDataTask._process_dataset(client, dataset_info, output_dir, logger)
                return result
            
            # Publisher mode
            elif "publisher_id" in params:
                publisher_id = params["publisher_id"]
                
                _log(logger, "info", f"Fetching datasets for publisher: {publisher_id}")
                
                # Get all datasets for publisher
                datasets = await SaudiOpenDataTask._fetch_datasets_for_publisher(client, publisher_id, logger)
                
                if not datasets:
                    return {
                        "publisher_id": publisher_id,
                        "status": "error",
                        "reason": "no_datasets_found",
                        "total_datasets": 0,
                        "datasets_succeeded": 0,
                        "datasets_failed": 0
                    }
                
                # Apply filtering
                if "dataset_range" in params:
                    start, end = params["dataset_range"]
                    datasets = datasets[int(start):int(end)+1]
                
                if "dataset_limit" in params:
                    limit = int(params["dataset_limit"])
                    datasets = datasets[:limit]
                
                _log(logger, "info", f"Processing {len(datasets)} datasets")
                
                # Process each dataset
                results = []
                total_files_ok = 0
                total_files_failed = 0
                datasets_succeeded = 0
                datasets_failed = 0
                
                for i, dataset_info in enumerate(datasets, 1):
                    _log(logger, "info", f"[{i}/{len(datasets)}] {dataset_info['title']}")
                    
                    result = await SaudiOpenDataTask._process_dataset(client, dataset_info, output_dir, logger)
                    results.append(result)
                    
                    if result["status"] == "success":
                        total_files_ok += result["downloaded"]
                        total_files_failed += result["failed"]
                        if result["downloaded"] > 0:
                            datasets_succeeded += 1
                        else:
                            datasets_failed += 1
                    else:
                        datasets_failed += 1
                    
                    # Rate limiting between datasets
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                
                # Save summary
                (output_dir / "publisher_results.json").write_text(
                    json.dumps(results, indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )
                
                return {
                    "publisher_id": publisher_id,
                    "status": "success",
                    "total_datasets": len(datasets),
                    "datasets_succeeded": datasets_succeeded,
                    "datasets_failed": datasets_failed,
                    "total_files_ok": total_files_ok,
                    "total_files_failed": total_files_failed,
                    "details_file": "publisher_results.json"
                }
            
            else:
                raise ValueError("Either 'dataset_id' or 'publisher_id' parameter is required")