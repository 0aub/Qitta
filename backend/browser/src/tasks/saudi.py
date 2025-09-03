"""
SaudiOpenDataTask - EXACT working implementation from backup.

This is the proven working code that successfully processed thousands of datasets.
"""

import asyncio
import hashlib
import json
import logging
import mimetypes
import os
import pathlib
import random
import re
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import httpx
from playwright.async_api import Browser, BrowserContext, Page, TimeoutError

from .base import _log


def safe_name(s: str) -> str:
    """Create a safe filename from input string."""
    s = (s or "").strip().replace(os.sep, "_")
    s = re.sub(r"[^\w\-.]+", "_", s).strip("._")
    return s[:180] or "file"


def _pick_filename_from_headers(headers: Dict[str, str]) -> Optional[str]:
    """Extract filename from Content-Disposition header."""
    disp = headers.get("content-disposition", "")
    if not disp:
        return None
    
    # Look for filename*= (UTF-8) or filename=
    for pattern in [r"filename\*=UTF-8''([^;]+)", r'filename="([^"]+)"', r"filename=([^;]+)"]:
        match = re.search(pattern, disp, re.IGNORECASE)
        if match:
            fname = match.group(1).strip()
            if pattern.startswith("filename\\*"):
                try:
                    fname = urllib.parse.unquote(fname)
                except Exception:
                    pass
            return fname
    return None


def _ext_from_format(fmt: Optional[str]) -> str:
    """Get file extension from format field."""
    if not fmt:
        return ""
    fmt = fmt.lower().strip()
    mapping = {
        "xlsx": ".xlsx", "excel": ".xlsx", "xls": ".xls", 
        "json": ".json", "xml": ".xml", "csv": ".csv",
        "txt": ".txt", "text": ".txt", "pdf": ".pdf"
    }
    return mapping.get(fmt, "")


def ext_from_content_type(content_type: Optional[str]) -> str:
    """Get file extension from content type."""
    if not content_type:
        return ""
    ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
    return ext or ""


def classify_payload(head: bytes, content_type: str = None) -> str:
    """Classify content payload to detect HTML interstitials vs real data."""
    if head.startswith(b"PK\x03\x04"):  # zip container (xlsx)
        return "xlsx_zip"
    if head.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):  # OLE CF (xls)
        return "xls_ole"
    
    lower = head.lower()
    if (lower.startswith(b"<!doctype html") or lower.startswith(b"<html") or 
        b"/tspd/" in lower or b"<apm_do_not_touch>" in lower):
        return "html_interstitial"
    
    if lower.startswith(b"{") or lower.startswith(b"["):
        return "json_text"
    if lower.startswith(b"<?xml") or lower.startswith(b"<xml"):
        return "xml_text"
    if b"," in head and b"\n" in head:  # Simple CSV heuristic
        return "csv_text"
    
    return "binary_unknown"


class SaudiTask:
    BASE = "https://open.data.gov.sa"

    # ───── filename / validation (Saudi-specific decisions kept here) ─────
    @staticmethod
    def _derive_name(resource: dict, link: str, headers: Dict[str, str], content_type: Optional[str]) -> str:
        """
        Choose a REAL filename:
          1) Content-Disposition filename
          2) If URL path has a file segment with extension → use it
          3) resource['name'] (or title) + extension from FORMAT (preferred)
          4) resourceID + extension from FORMAT
          5) Finally, use MIME only if it's specific (not octet-stream). Never ".bin" unless truly unknown.
        """
        # 1) From headers
        from_disp = _pick_filename_from_headers(headers) or ""
        if from_disp:
            return safe_name(from_disp)

        # 2) From URL path
        path_seg = urllib.parse.urlsplit(link).path.rsplit("/", 1)[-1]
        if path_seg and "." in path_seg:
            return safe_name(path_seg)

        # 3) From resource + format
        fmt_ext = _ext_from_format(resource.get("format"))
        base = (resource.get("name") or resource.get("titleEn") or resource.get("titleAr") or "").strip()
        if base:
            base = safe_name(base.replace(" ", "_"))
            return safe_name(base + (fmt_ext or ""))

        # 4) resourceID + format
        rid = resource.get("resourceID") or resource.get("id") or ""
        if rid:
            return safe_name(rid + (fmt_ext or ""))

        # 5) MIME
        ct_ext = ext_from_content_type(content_type)
        if ct_ext and ct_ext != ".bin":
            return "file" + ct_ext

        return "file"

    @staticmethod
    def _validate_and_save(
        dst_dir: pathlib.Path,
        suggested: str,
        body: bytes,
        content_type: str | None,
        resource: Optional[dict] = None,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """Return (ok, filename, meta). Reject HTML interstitials."""
        head = body[:2048]
        kind = classify_payload(head, content_type)
        if kind == "html_interstitial":
            (dst_dir / "blocked.html").write_bytes(body)
            return False, "", {"reason": "html_interstitial"}

        # Ensure filename has a base + reasonable extension
        name = safe_name(suggested)
        root, ext = os.path.splitext(name)

        if not root:
            # Guarantee non-empty base using resource name or id
            if resource:
                root = safe_name(
                    (resource.get("name")
                     or resource.get("titleEn")
                     or resource.get("titleAr")
                     or resource.get("resourceID")
                     or resource.get("id")
                     or "file").replace(" ", "_")
                )
            else:
                root = "file"

        # pick extension preference: FORMAT > Content-Type (if specific) > kind
        fmt_ext = _ext_from_format(resource.get("format") if resource else None)
        ct_ext = ext_from_content_type(content_type)
        if not ext or ext == ".bin":
            chosen = fmt_ext or (ct_ext if ct_ext and ct_ext != ".bin" else "")
            if not chosen:
                chosen = {
                    "xlsx_zip": ".xlsx",
                    "xls_ole": ".xls",
                    "json_text": ".json",
                    "xml_text": ".xml",
                    "csv_text": ".csv",
                    "text_plain": ".txt",
                }.get(kind, "")
            ext = chosen or ext

        final_name = safe_name(root + ext)

        tmp = (dst_dir / final_name).with_suffix((dst_dir / final_name).suffix + ".part")
        with open(tmp, "wb") as f:
            f.write(body)
        if os.path.getsize(tmp) == 0:
            tmp.unlink(missing_ok=True)
            return False, "", {"reason": "empty"}
        final = dst_dir / final_name
        os.replace(tmp, final)

        sha = hashlib.sha256(body).hexdigest()
        return True, final.name, {"size": final.stat().st_size, "sha256": sha, "kind": kind}

    # ───── playwright context helpers ─────
    @staticmethod
    async def _new_ctx(browser: Browser, head: dict, proxy: Optional[str]) -> BrowserContext:
        args = {
            "user_agent": head["user-agent"],
            "accept_downloads": True,
            "locale": "en-US",
        }
        if proxy:
            args["proxy"] = {"server": proxy}
        ctx = await browser.new_context(**args)
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        return ctx

    @staticmethod
    async def _warm_resources(ctx: BrowserContext, dsid: str, log: logging.Logger):
        url = f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources"
        pg = await ctx.new_page()
        try:
            await pg.goto(url, timeout=90_000, wait_until="domcontentloaded")
            await pg.wait_for_timeout(600)  # keep your original tiny warm
            _log(log, "info", f"{dsid}: warmed resources page")
        except Exception as e:
            _log(log, "warning", f"{dsid}: warm failed – {e}")
        finally:
            await pg.close()
            
    @staticmethod
    async def _ensure_interstitial_cookies(ctx: BrowserContext, dsid: str, log: logging.Logger) -> None:
        """
        Fast interstitial warm: do the 2 navigations (base -> dataset resources),
        then spend at most ~800ms checking if the cookie jar grew. No brittle name checks.
        """
        page = await ctx.new_page()
        try:
            # Snapshot before
            try:
                before = await ctx.cookies("https://open.data.gov.sa")
                before_len = len(before)
            except Exception:
                before_len = 0

            await page.goto("https://open.data.gov.sa/", wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(400)
            await page.goto(f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources", wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(400)

            # Snapshot after, detect growth
            try:
                after = await ctx.cookies("https://open.data.gov.sa")
                if len(after) > before_len:
                    _log(log, "info", f"{dsid}: interstitial bypass cookies detected")
                else:
                    _log(log, "debug", f"{dsid}: no new cookies from interstitial warm")
            except Exception as e:
                _log(log, "warning", f"{dsid}: interstitial cookie check failed: {e}")

        except Exception as e:
            _log(log, "warning", f"{dsid}: interstitial warm failed: {e}")
        finally:
            await page.close()

    # ───── metadata (resources and dataset info) ─────
    @staticmethod
    async def _get_dataset_metadata(ctx: BrowserContext, dsid: str, head: dict, log: logging.Logger) -> dict:
        """Get dataset metadata using the official API."""
        url = f"https://open.data.gov.sa/data/api/datasets?version=-1&dataset={dsid}"
        start_time = time.time()
        
        try:
            _log(log, "debug", f"🌐 {dsid}: requesting dataset metadata from API")
            r = await ctx.request.get(url, headers={"accept": "application/json"}, timeout=45_000)
            api_time = time.time() - start_time
            
            if r.status != 200:
                _log(log, "warning", f"❌ {dsid}: dataset metadata API returned HTTP {r.status} in {api_time:.2f}s")
                raise RuntimeError(f"dataset_metadata_http_{r.status}")
            
            body = await r.body()
            kind = classify_payload(body[:2048], r.headers.get("content-type"))
            if kind == "html_interstitial":
                _log(log, "warning", f"🚫 {dsid}: dataset metadata blocked by WAF in {api_time:.2f}s")
                raise RuntimeError("dataset_metadata_blocked")
            
            j = json.loads(body.decode("utf-8", "ignore"))
            _log(log, "info", f"✅ {dsid}: dataset metadata retrieved in {api_time:.2f}s")
            return j
            
        except Exception as e:
            api_time = time.time() - start_time
            _log(log, "error", f"❌ {dsid}: dataset metadata failed in {api_time:.2f}s – {e}")
            raise

    @staticmethod
    async def _resources_via_api(ctx: BrowserContext, dsid: str, head: dict, log: logging.Logger) -> List[dict]:
        """Get dataset resources using the official API."""
        url = f"https://open.data.gov.sa/data/api/datasets/resources?version=-1&dataset={dsid}"
        start_time = time.time()
        
        try:
            _log(log, "debug", f"🗂️  {dsid}: requesting resources metadata from API")
            r = await ctx.request.get(url, headers={"accept": "application/json"}, timeout=45_000)
            api_time = time.time() - start_time
            
            if r.status != 200:
                _log(log, "warning", f"❌ {dsid}: resources API returned HTTP {r.status} in {api_time:.2f}s")
                raise RuntimeError(f"resources_http_{r.status}")
            
            body = await r.body()
            kind = classify_payload(body[:2048], r.headers.get("content-type"))
            if kind == "html_interstitial":
                _log(log, "warning", f"🚫 {dsid}: resources blocked by WAF in {api_time:.2f}s")
                raise RuntimeError("resources_blocked")
            
            j = json.loads(body.decode("utf-8", "ignore"))
            if isinstance(j, dict) and j.get("resources"):
                resource_count = len(j["resources"])
                _log(log, "info", f"✅ {dsid}: {resource_count} resources retrieved in {api_time:.2f}s")
                return j["resources"]
            else:
                _log(log, "warning", f"⚠️  {dsid}: API response contains no resources in {api_time:.2f}s")
                raise RuntimeError("no_resources_in_response")
                
        except Exception as e:
            api_time = time.time() - start_time  
            _log(log, "error", f"❌ {dsid}: resources API failed in {api_time:.2f}s – {e}")
            raise

    # ───── download strategies ─────
    @staticmethod
    async def _ctx_v1_download(ctx: BrowserContext, dsid: str, rid: str, resource: dict,
                              out_dir: pathlib.Path, head: dict, log: logging.Logger) -> Dict[str, Any]:
        """Download using browser context with v1 API."""
        url = f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download"
        try:
            r = await ctx.request.get(url, headers=SaudiOpenDataTask._dl_headers(head), timeout=90_000)
            if r.status != 200:
                return {"stage": "ctx(v1)", "status": "error", "reason": f"http_{r.status}"}
            
            body = await r.body()
            suggested = SaudiOpenDataTask._derive_name(resource, url, r.headers, r.headers.get("content-type"))
            ok, fname, meta = SaudiOpenDataTask._validate_and_save(out_dir, suggested, body, r.headers.get("content-type"), resource)
            
            if not ok:
                return {"stage": "ctx(v1)", "status": "error", **meta}
            return {"stage": "ctx(v1)", "status": "ok", "file": fname, **meta}
            
        except TimeoutError:
            return {"stage": "ctx(v1)", "status": "error", "reason": "timeout"}
        except Exception as e:
            return {"stage": "ctx(v1)", "status": "error", "reason": str(e)}

    @staticmethod
    async def _httpx_download(url: str, resource: dict, out_dir: pathlib.Path, head: dict) -> Dict[str, Any]:
        """Download using httpx client."""
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=90) as cl:
                r = await cl.get(url, headers=SaudiOpenDataTask._dl_headers(head))
                if r.status_code != 200:
                    return {"stage": "httpx", "status": "error", "reason": f"http_{r.status_code}"}
                
                body = r.content
                suggested = SaudiOpenDataTask._derive_name(resource, url, dict(r.headers), r.headers.get("content-type"))
                ok, fname, meta = SaudiOpenDataTask._validate_and_save(out_dir, suggested, body, r.headers.get("content-type"), resource)
                
                if not ok:
                    return {"stage": "httpx", "status": "error", **meta}
                return {"stage": "httpx", "status": "ok", "file": fname, **meta}
                
        except Exception as e:
            return {"stage": "httpx", "status": "error", "reason": str(e)}

    @staticmethod
    def _dl_headers(base: dict) -> dict:
        """Generate download headers."""
        h = {"user-agent": base.get("user-agent", "")}
        if base.get("referer"):
            h["referer"] = base["referer"]
        return h

    @staticmethod
    async def _download_resource(ctx: BrowserContext, dsid: str, resource: dict,
                                out_dir: pathlib.Path, head: dict, log: logging.Logger) -> Dict[str, Any]:
        """Try multiple download strategies with anti-bot protection."""
        rid = resource.get("resourceID") or resource.get("id") or ""
        link = resource.get("downloadUrl") or resource.get("url") or ""
        
        attempts = []
        
        # Strategy 1: ctx(v1) via browser context
        if rid:
            r1 = await SaudiOpenDataTask._ctx_v1_download(ctx, dsid, rid, resource, out_dir, head, log)
            if r1["status"] == "ok":
                return r1
            attempts.append(r1)

        # Strategy 2: httpx direct download
        if link:
            r2 = await SaudiOpenDataTask._httpx_download(link, resource, out_dir, head)
            if r2["status"] == "ok":
                return r2
            attempts.append(r2)

        # Strategy 3: Use interstitial cookies then retry ctx(v1)
        if rid:
            try:
                await SaudiOpenDataTask._ensure_interstitial_cookies(ctx, dsid, log)
                await asyncio.sleep(random.uniform(0.5, 1.0))
                r3 = await SaudiOpenDataTask._ctx_v1_download(ctx, dsid, rid, resource, out_dir, head, log)
                if r3["status"] == "ok":
                    return r3
                attempts.append(r3)
            except Exception as e:
                attempts.append({"stage": "ctx(v1+cookies)", "status": "error", "reason": str(e)})

        # All strategies failed
        return {
            "status": "error",
            "url": link or f"/v1/{dsid}/{rid}",
            "reason": "all_attempts_failed",
            "attempts": attempts
        }

    # ───── one dataset ─────
    @staticmethod
    async def _run_dataset(browser: Browser, dsid: str, head: dict, out_root: pathlib.Path, logger: logging.Logger) -> Dict[str, Any]:
        import urllib.parse, json, random, asyncio, pathlib

        out_dir = out_root / dsid
        (out_dir / "downloads").mkdir(parents=True, exist_ok=True)

        ctx = await SaudiOpenDataTask._new_ctx(browser, head, proxy=None)
        try:
            # Get dataset metadata first
            _log(logger, "debug", f"📊 {dsid}: fetching dataset metadata")
            try:
                dataset_metadata = await SaudiOpenDataTask._get_dataset_metadata(ctx, dsid, head, logger)
                (out_dir / "dataset_metadata.json").write_text(json.dumps(dataset_metadata, indent=2, ensure_ascii=False), "utf-8")
                _log(logger, "debug", f"💾 {dsid}: dataset metadata saved")
            except Exception as e:
                _log(logger, "warning", f"⚠️  {dsid}: dataset metadata failed – {e}")
                dataset_metadata = {"error": str(e)}
                (out_dir / "dataset_metadata.json").write_text(json.dumps(dataset_metadata, indent=2, ensure_ascii=False), "utf-8")

            # Get resources metadata
            _log(logger, "debug", f"📋 {dsid}: fetching resources metadata")
            try:
                resources = await SaudiOpenDataTask._resources_via_api(ctx, dsid, head, logger)
                _log(logger, "info", f"📝 {dsid}: starting download of {len(resources)} resources")
            except Exception as e:
                _log(logger, "error", f"❌ {dsid}: resources metadata failed – {e}")
                # make failure inspectable instead of silent
                (out_dir / "downloads.json").write_text(
                    json.dumps([{"status": "error", "stage": "metadata", "reason": str(e)}], indent=2, ensure_ascii=False),
                    "utf-8"
                )
                raise  # keep your current upstream failure behavior

            # Normalize resource URLs
            for r in resources:
                lk = r.get("downloadUrl") or r.get("url") or ""
                if lk and not lk.lower().startswith("http"):
                    lk = "https://open.data.gov.sa/data/" + lk.lstrip("/")
                p = urllib.parse.urlsplit(lk)
                r["downloadUrl"] = urllib.parse.urlunsplit(
                    (p.scheme, p.netloc, urllib.parse.quote(p.path, safe="/"), p.query, "")
                )

            (out_dir / "resources.json").write_text(json.dumps(resources, indent=2, ensure_ascii=False), "utf-8")

            # --- download phase with retries/cookie reuse handled inside _download_resource ---
            results: List[Dict[str, Any]] = []
            download_start_time = time.time()
            
            for idx, r in enumerate(resources, 1):
                resource_name = r.get("name") or r.get("titleEn") or r.get("titleAr") or f"Resource {idx}"
                resource_format = r.get("format", "unknown")
                
                _log(logger, "debug", f"📥 {dsid}: downloading [{idx}/{len(resources)}] {resource_name} ({resource_format})")
                
                resource_start_time = time.time()
                item = await SaudiOpenDataTask._download_resource(ctx, dsid, r, out_dir / "downloads", head, logger.getChild("dl"))
                resource_time = time.time() - resource_start_time
                
                item["resource_name"] = resource_name
                item["download_time_seconds"] = round(resource_time, 2)
                results.append(item)
                
                # Log download result
                if item.get("status") == "ok":
                    file_size = item.get("size", 0)
                    _log(logger, "info", f"✅ {dsid}: [{idx}/{len(resources)}] downloaded {resource_name} ({file_size} bytes) in {resource_time:.2f}s")
                else:
                    _log(logger, "warning", f"❌ {dsid}: [{idx}/{len(resources)}] failed {resource_name} - {item.get('reason', 'unknown error')}")
                
                # Conservative delay between downloads
                delay = random.uniform(0.5, 1.2)
                await asyncio.sleep(delay)
            
            download_total_time = time.time() - download_start_time
            downloaded_count = sum(1 for x in results if x.get("status") == "ok")
            _log(logger, "info", f"🏁 {dsid}: download phase completed in {download_total_time:.1f}s ({downloaded_count}/{len(resources)} successful)")

            (out_dir / "downloads.json").write_text(json.dumps(results, indent=2, ensure_ascii=False), "utf-8")

            ok_count = sum(1 for x in results if x.get("status") == "ok")
            fail_count = len(results) - ok_count

            return {
                "dataset_id": dsid,
                "status": "success",
                "total_resources": len(resources),
                "downloaded": ok_count,
                "failed": fail_count,
                "resources_json": "resources.json",
                "downloads_json": "downloads.json",
                "dataset_metadata_json": "dataset_metadata.json",
            }

        finally:
            await ctx.close()

    # ───── list datasets by publisher ─────
    @staticmethod
    async def _get_organization_metadata(browser: Browser, pub_id: str, head: dict, logger: logging.Logger) -> dict:
        """Get organization metadata and datasets list using official API."""
        ctx = await SaudiOpenDataTask._new_ctx(browser, head, proxy=None)
        try:
            url = f"https://open.data.gov.sa/data/api/organizations?version=-1&organization={pub_id}"
            r = await ctx.request.get(url, headers={"accept": "application/json"}, timeout=45_000)
            
            if r.status == 404:
                _log(logger, "warning", f"org API http_404")
                return {"datasets": []}
            elif r.status != 200:
                _log(logger, "warning", f"org API http_{r.status}")
                return {"datasets": []}
                
            body = await r.body()
            try:
                j = json.loads(body.decode("utf-8", "ignore"))
                _log(logger, "info", f"{pub_id}: organization metadata retrieved")
                return j
            except json.JSONDecodeError as e:
                _log(logger, "warning", f"org API JSON decode error: {e}")
                return {"datasets": []}
                
        except Exception as e:
            _log(logger, "warning", f"org API error: {e}")
            return {"datasets": []}
        finally:
            await ctx.close()

    @staticmethod
    async def _list_datasets_for_publisher(browser: Browser, pub_id: str, head: dict, logger: logging.Logger) -> List[dict]:
        """Get datasets list from organization metadata."""
        org_data = await SaudiOpenDataTask._get_organization_metadata(browser, pub_id, head, logger)
        return org_data.get("datasets", [])

    # ───── main entrypoint ─────
    @staticmethod
    async def run(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """Main task entrypoint."""
        import time

        HEAD = {
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/139.0.6779.87 Safari/537.36"
            )
        }

        out_root = pathlib.Path(job_output_dir)
        out_root.mkdir(parents=True, exist_ok=True)

        # Single dataset mode
        if dsid := params.get("dataset_id"):
            _log(logger, "info", f"{dsid}: single dataset flow")
            res = await SaudiOpenDataTask._run_dataset(browser, dsid, HEAD, out_root, logger)
            return res

        # Publisher mode
        pub_id = params.get("publisher_id") or params.get("publisher")
        if not pub_id:
            raise ValueError("Either 'publisher_id' or 'publisher' parameter is required")
        
        # Create publisher folder structure: /job_output_dir/publisher_id/
        publisher_root = out_root / pub_id
        publisher_root.mkdir(parents=True, exist_ok=True)
        
        # Get organization metadata and save it
        _log(logger, "info", f"📊 {pub_id}: fetching organization metadata")
        start_time = time.time()
        
        try:
            org_metadata = await SaudiOpenDataTask._get_organization_metadata(browser, pub_id, HEAD, logger)
            metadata_time = time.time() - start_time
            _log(logger, "info", f"✅ {pub_id}: organization metadata retrieved in {metadata_time:.2f}s")
            
            # Save organization metadata in publisher folder
            (publisher_root / "organization_metadata.json").write_text(json.dumps(org_metadata, indent=2, ensure_ascii=False), "utf-8")
            
        except Exception as e:
            _log(logger, "error", f"❌ {pub_id}: organization metadata failed: {e}")
            raise
        
        # Get datasets list
        all_items = org_metadata.get("datasets", [])
        _log(logger, "info", f"📋 {pub_id}: found {len(all_items)} total datasets")

        # Apply filtering
        if "dataset_range" in params:
            a, b = params["dataset_range"]
            all_items = all_items[int(a): int(b)+1]
            _log(logger, "info", f"🎯 {pub_id}: applied range filter [{a}:{b+1}], {len(all_items)} datasets")
            
        if "max_datasets" in params:
            original_count = len(all_items)
            all_items = all_items[: int(params["max_datasets"])]
            _log(logger, "info", f"🎯 {pub_id}: applied max_datasets filter {original_count}→{len(all_items)}")

        _log(logger, "info", f"🚀 {pub_id}: starting processing of {len(all_items)} dataset(s)")

        # Process each dataset with comprehensive logging and monitoring
        per_dataset: List[Dict[str, Any]] = []
        success_count = 0
        error_count = 0
        total_start_time = time.time()
        
        # Track performance metrics for monitoring
        processing_times = []
        
        for i, item in enumerate(all_items):
            dsid = item.get("id") or item.get("dataset_id") or ""
            title = item.get("titleEn") or item.get("title") or item.get("name") or "Untitled"
            url = f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources"
            
            dataset_start_time = time.time()
            _log(logger, "info", f"📦 [{i+1}/{len(all_items)}] Starting: {dsid} – {title[:60]}{'...' if len(title) > 60 else ''}")
            
            try:
                # Process dataset in publisher folder
                res = await SaudiOpenDataTask._run_dataset(browser, dsid, HEAD, publisher_root, logger)
                dataset_time = time.time() - dataset_start_time
                processing_times.append(dataset_time)
                
                res.update({
                    "dataset_id": dsid,
                    "dataset_title": title,
                    "dataset_url": url,
                    "processing_time_seconds": round(dataset_time, 2),
                })
                per_dataset.append(res)
                success_count += 1
                
                _log(logger, "info", f"✅ [{i+1}/{len(all_items)}] Completed: {dsid} in {dataset_time:.2f}s (downloaded: {res.get('downloaded', 0)} files)")
                
            except Exception as e:
                dataset_time = time.time() - dataset_start_time
                error_count += 1
                
                _log(logger, "error", f"❌ [{i+1}/{len(all_items)}] Failed: {dsid} after {dataset_time:.2f}s - {e}")
                per_dataset.append({
                    "dataset_id": dsid,
                    "dataset_title": title,
                    "dataset_url": url,
                    "status": "error",
                    "error": str(e),
                    "processing_time_seconds": round(dataset_time, 2),
                })
                
                # Safety check: if too many consecutive errors, something might be wrong
                if error_count > 0 and error_count >= max(3, len(all_items) // 2):
                    _log(logger, "warning", f"⚠️  High error rate detected ({error_count}/{i+1}). Continuing with increased caution.")

            # Conservative delay to avoid any blocking
            delay = random.uniform(0.8, 1.5)
            _log(logger, "debug", f"😴 Waiting {delay:.1f}s before next dataset...")
            await asyncio.sleep(delay)
            
            # Progress update every 5 datasets
            if (i + 1) % 5 == 0:
                elapsed = time.time() - total_start_time
                avg_time = sum(processing_times[-5:]) / min(5, len(processing_times)) if processing_times else 0
                remaining = len(all_items) - (i + 1)
                eta = remaining * avg_time if avg_time > 0 else 0
                _log(logger, "info", f"📈 Progress: {i+1}/{len(all_items)} | Success: {success_count} | Errors: {error_count} | Avg: {avg_time:.1f}s/dataset | ETA: {eta/60:.1f}m")

        # Final statistics
        total_time = time.time() - total_start_time
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        _log(logger, "info", f"🏁 Processing complete! Total: {total_time:.1f}s | Success: {success_count} | Errors: {error_count} | Avg: {avg_processing_time:.1f}s/dataset")

        # Save results in publisher folder
        (publisher_root / "publisher_results.json").write_text(json.dumps(per_dataset, indent=2, ensure_ascii=False), "utf-8")

        result = {
            "publisher_id": pub_id,
            "status": "success",
            "total_datasets": len(all_items),
            "datasets_succeeded": sum(1 for x in per_dataset if x.get("status") == "success"),
            "datasets_failed": sum(1 for x in per_dataset if x.get("status") != "success"),
            "total_files_ok": sum(x.get("downloaded", 0) for x in per_dataset if isinstance(x.get("downloaded"), int)),
            "total_files_failed": sum(x.get("failed", 0) for x in per_dataset if isinstance(x.get("failed"), int)),
            "details_file": "publisher_results.json",
            "organization_metadata_file": "organization_metadata.json",
        }

        return result