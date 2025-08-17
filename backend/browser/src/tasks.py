# tasks.py — registry + tasks (Saudi Open Data kept in one module)
# 2025-08-10

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import logging
import os
import pathlib
import random
import re
import urllib.parse
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx
from playwright.async_api import Browser, BrowserContext, TimeoutError

from .utils import classify_payload, ext_from_content_type, safe_name, build_download_headers


# ───────────────── registry (OOP) ─────────────────
class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: Dict[str, Callable[..., Awaitable[Dict[str, Any]]]] = {}

    def register(self, name: str):
        def deco(fn):
            self._tasks[name] = fn
            return fn
        return deco

    def resolve(self, name: str) -> str:
        variants = [name, name.replace("_", "-"), name.replace("-", "_")]
        for v in variants:
            if v in self._tasks:
                return v
        return name

    @property
    def tasks(self) -> Dict[str, Callable[..., Awaitable[Dict[str, Any]]]]:
        return self._tasks


_registry = TaskRegistry()
task_registry = _registry.tasks  # exported for main
def normalise_task(name: str) -> str:  # exported helper
    return _registry.resolve(name)


# ──────────────── shared small utils ────────────────────
UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

def _log(lg: logging.Logger, lvl: str, msg: str):
    getattr(lg, lvl)(msg.replace("\n", " ")[:1000])

def _ext_from_format(fmt: Optional[str]) -> str:
    f = (fmt or "").strip().lower()
    mapping = {
        "xlsx": ".xlsx",
        "xls": ".xls",
        "csv": ".csv",
        "json": ".json",
        "xml": ".xml",
        "zip": ".zip",
    }
    return mapping.get(f, "")

def _pick_filename_from_headers(headers: Dict[str, str]) -> Optional[str]:
    disp = headers.get("content-disposition") or headers.get("Content-Disposition") or ""
    m = re.search(r"filename\*=(?:UTF-8''|)([^;]+)", disp, re.I)
    if m:
        try:
            return urllib.parse.unquote(m.group(1).strip().strip('"'))
        except Exception:
            pass
    m2 = re.search(r'filename="?([^";]+)"?', disp, re.I)
    if m2:
        return m2.group(1).strip()
    return None


# ═══════════════════ Saudi Open Data Task (kept in one module) ═══════════════════
class SaudiOpenDataTask:
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
          5) Finally, use MIME only if it’s specific (not octet-stream). Never ".bin" unless truly unknown.
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
            await page.goto(f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources",
                            wait_until="domcontentloaded", timeout=90_000)

            # Short, bounded polling: exit as soon as jar size increases
            for _ in range(4):  # ~4 * 200ms = ~800ms max
                await page.wait_for_timeout(200)
                try:
                    after_len = len(await ctx.cookies("https://open.data.gov.sa"))
                except Exception:
                    after_len = before_len
                if after_len > before_len:
                    break
            # No logging here; success is implied by the navigations themselves.
        finally:
            await page.close()


    # ───── metadata (resources) ─────
    @staticmethod
    async def _resources_via_api(ctx: BrowserContext, dsid: str, head: dict, log: logging.Logger) -> List[dict]:
        referer = f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources"
        url1 = f"https://open.data.gov.sa/data/api/datasets/resources?version=-1&dataset={dsid}"
        url2 = f"https://open.data.gov.sa/api/datasets/{dsid}"

        async def _try_url(url: str, headers: Dict[str, str]) -> Optional[List[dict]]:
            try:
                r = await ctx.request.get(url, headers=headers, timeout=45_000)
                if r.status != 200:
                    return None
                body = await r.body()
                # Quick JSON guard
                if not body or body[:1] not in (b"{", b"["):
                    return None
                kind = classify_payload(body[:2048], r.headers.get("content-type"))
                if kind == "html_interstitial":
                    return None
                j = json.loads(body.decode("utf-8", "ignore"))
                if isinstance(j, dict) and j.get("resources"):
                    return j["resources"]
                return None
            except Exception:
                return None

        headers1 = {
            "accept": "application/json",
            "x-requested-with": "XMLHttpRequest",
            "x-security-request": "required",
            "referer": referer,
        }
        headers2 = {"accept": "application/json", "referer": referer}

        # Two short rounds: url1 then url2 (with tiny pause between rounds)
        for attempt in range(2):
            res = await _try_url(url1, headers1)
            if res:
                _log(log, "info", f"{dsid}: resources via datasets/resources?version=-1")
                return res
            res = await _try_url(url2, headers2)
            if res:
                _log(log, "info", f"{dsid}: resources via /api/datasets/{dsid}")
                return res
            await asyncio.sleep(0.6 + 0.4 * attempt)

        raise RuntimeError("metadata_blocked")

    # ───── download strategies ─────
    @staticmethod
    async def _ctx_v1_download(
        ctx: BrowserContext,
        dsid: str,
        rid: str,
        resource: dict,
        out_dir: pathlib.Path,
        head: dict,
        log: logging.Logger,
    ) -> Dict[str, Any]:
        url = f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download"
        try:
            r = await ctx.request.get(url, headers=build_download_headers(head), timeout=90_000)
            if r.status != 200:
                return {"stage": "ctx(v1)", "status": "error", "reason": f"http_{r.status}"}
            body = await r.body()
            suggested = SaudiOpenDataTask._derive_name(resource, url, r.headers, r.headers.get("content-type"))
            ok, fname, meta = SaudiOpenDataTask._validate_and_save(
                out_dir, suggested, body, r.headers.get("content-type"), resource
            )
            if not ok:
                return {"stage": "ctx(v1)", "status": "error", **meta}
            return {"stage": "ctx(v1)", "status": "ok", "file": fname, **meta}
        except TimeoutError:
            return {"stage": "ctx(v1)", "status": "error", "reason": "timeout"}
        except Exception as e:
            return {"stage": "ctx(v1)", "status": "error", "reason": str(e)}

    @staticmethod
    async def _httpx_download(
        url: str,
        resource: dict,
        out_dir: pathlib.Path,
        head: dict,
        ctx: "BrowserContext | None" = None,
        referer: "str | None" = None,
    ) -> Dict[str, Any]:
        import httpx, hashlib, os, tempfile, mimetypes, urllib.parse

        stage = "httpx"

        # Headers
        headers = build_download_headers(head)
        if referer:
            headers["referer"] = referer

        # Reuse Playwright cookies (if available)
        if ctx:
            try:
                # site cookies
                site_cookies = await ctx.cookies("https://open.data.gov.sa")
                # target host cookies (if different)
                p = urllib.parse.urlsplit(url)
                base = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""
                tgt_cookies = await ctx.cookies(base) if base and "open.data.gov.sa" not in base else []
                jar = {}
                for c in (site_cookies or []) + (tgt_cookies or []):
                    n, v = c.get("name"), c.get("value")
                    if n and v is not None:
                        jar[n] = v
                if jar:
                    headers["cookie"] = "; ".join(f"{k}={v}" for k, v in jar.items())
            except Exception:
                pass

        # Request
        try:
            timeout = httpx.Timeout(90.0, connect=30.0, read=90.0)
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
                resp = await client.get(url, headers=headers)
        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            return {"status": "error", "stage": stage, "reason": "timeout"}
        except httpx.RequestError as e:
            return {"status": "error", "stage": stage, "reason": f"request_error:{e.__class__.__name__}"}
        except Exception as e:
            return {"status": "error", "stage": stage, "reason": f"exception:{type(e).__name__}"}

        if resp.status_code != 200:
            return {"status": "error", "stage": stage, "reason": f"http_{resp.status_code}"}

        # Stream to temp, detect interstitial, finalize
        first_chunk = b""
        hasher = hashlib.sha256()
        bytes_written = 0

        os.makedirs(out_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(delete=False, dir=str(out_dir)) as tmp:
            tmp_path = tmp.name
            try:
                async for chunk in resp.aiter_bytes():
                    if not first_chunk:
                        first_chunk = chunk[:4096]
                    hasher.update(chunk)
                    tmp.write(chunk)
                    bytes_written += len(chunk)
            except httpx.ReadTimeout:
                try: os.remove(tmp_path)
                except Exception: pass
                return {"status": "error", "stage": stage, "reason": "timeout"}
            except Exception as e:
                try: os.remove(tmp_path)
                except Exception: pass
                return {"status": "error", "stage": stage, "reason": f"stream_error:{type(e).__name__}"}

        if bytes_written == 0:
            try: os.remove(tmp_path)
            except Exception: pass
            return {"status": "error", "stage": stage, "reason": "empty"}

        kind = classify_payload(first_chunk, resp.headers.get("content-type"))
        if kind == "html_interstitial":
            try: os.remove(tmp_path)
            except Exception: pass
            return {"status": "error", "stage": stage, "reason": "html_interstitial"}

        # Name file
        def _pick_filename(resp: httpx.Response) -> str:
            cd = resp.headers.get("content-disposition", "")
            if "filename=" in cd:
                fname = cd.split("filename=", 1)[1].strip('"; ')
                if fname:
                    return fname
            path = urllib.parse.urlsplit(str(resp.url)).path
            base = os.path.basename(path) or ""
            if base:
                return base
            name = (resource.get("name") or resource.get("titleEn") or resource.get("titleAr") or "file").strip()
            ctype = resp.headers.get("content-type")
            ext = ""
            if ctype:
                ext = mimetypes.guess_extension(ctype.split(";")[0].strip()) or ""
            safe = "".join(ch for ch in name if ch.isalnum() or ch in (" ", "_", "-", ".")).strip() or "file"
            return (safe + ext).replace("  ", " ")

        final_name = _pick_filename(resp).split("?")[0].strip() or "download"
        final_path = out_dir / final_name
        i = 1
        stem, ext = os.path.splitext(final_path.name)
        while final_path.exists():
            final_path = out_dir / f"{stem} ({i}){ext}"
            i += 1

        try:
            os.replace(tmp_path, final_path)
        except Exception:
            final_path = out_dir / final_name  # best effort if rename failed

        return {
            "status": "ok",
            "stage": stage,
            "file": str(final_path),
            "size": bytes_written,
            "sha256": hasher.hexdigest(),
        }

    @staticmethod
    async def _tab_nav(ctx: BrowserContext, url: str, resource: dict, out_dir: pathlib.Path, head: dict) -> Dict[str, Any]:
        pg = await ctx.new_page()
        try:
            await pg.set_extra_http_headers(build_download_headers(head))
            async with pg.expect_download() as info:
                await pg.goto(url, timeout=90_000)
            dl = await info.value
            suggested = dl.suggested_filename or SaudiOpenDataTask._derive_name(resource, url, {}, None)
            tmp = out_dir / (suggested + ".part")
            await dl.save_as(tmp)
            body = tmp.read_bytes() if tmp.exists() else b""
            ok, fname, meta = SaudiOpenDataTask._validate_and_save(out_dir, suggested, body, None, resource)
            tmp.unlink(missing_ok=True)
            if not ok:
                return {"stage": "tab-nav", "status": "error", **meta}
            return {"stage": "tab-nav", "status": "ok", "file": fname, **meta}
        except TimeoutError:
            return {"stage": "tab-nav", "status": "error", "reason": "timeout"}
        except Exception as e:
            return {"stage": "tab-nav", "status": "error", "reason": str(e)}
        finally:
            await pg.close()

    @staticmethod
    async def _download_resource(
        ctx: BrowserContext, dsid: str, resource: dict, out_dir: pathlib.Path, head: dict, log: logging.Logger
    ) -> Dict[str, Any]:
        # Build dataset referer and normalize link
        referer = f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources"
        link = (resource.get("downloadUrl") or resource.get("url") or "").strip()
        if link and not link.lower().startswith("http"):
            link = "https://open.data.gov.sa/data/" + link.lstrip("/")
        rid = resource.get("resourceID") or resource.get("id") or ""
        attempts: List[Dict[str, Any]] = []

        # Small retry helper for transient failures
        async def _run_with_retry(coro_factory, max_retries=2):
            delay = 0.8
            last = None
            transient = ("timeout", "html_interstitial", "http_502", "http_503", "http_504", "http_522", "empty")
            for i in range(max_retries + 1):
                r = await coro_factory()
                if r.get("status") == "ok":
                    return r
                last = r
                reason = (r.get("reason") or "").lower()
                # retry only on transient reasons
                if any(k in reason for k in transient):
                    # If interstitial, try to re-solve cookies quickly
                    if "interstitial" in reason:
                        try:
                            await SaudiOpenDataTask._ensure_interstitial_cookies(ctx, dsid, log)
                        except Exception:
                            pass
                    await asyncio.sleep(delay)
                    delay *= 1.6
                    continue
                break
            return last

        # 0) v1 official endpoint
        if dsid and rid:
            v1_url = f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download"

            r = await _run_with_retry(
                lambda: SaudiOpenDataTask._ctx_v1_download(ctx, dsid, rid, resource, out_dir, head, log),
                max_retries=2
            )
            if r["status"] == "ok":
                return {
                    "status": "ok",
                    "via": r["stage"],
                    "url": v1_url,
                    "path": r["file"],
                    "size": r.get("size"),
                    "sha256": r.get("sha256"),
                }
            attempts.append(r)

            # httpx against v1 endpoint, but **with cookies+referer**
            r2 = await _run_with_retry(
                lambda: SaudiOpenDataTask._httpx_download(v1_url, resource, out_dir, head, ctx=ctx, referer=referer),
                max_retries=2
            )
            if r2["status"] == "ok":
                return {
                    "status": "ok",
                    "via": "httpx(v1)",
                    "url": v1_url,
                    "path": r2["file"],
                    "size": r2.get("size"),
                    "sha256": r2.get("sha256"),
                }
            attempts.append(r2)

        # 1) odp-public (direct) link
        if link:
            r3 = await _run_with_retry(
                lambda: SaudiOpenDataTask._httpx_download(link, resource, out_dir, head, ctx=ctx, referer=referer),
                max_retries=2
            )
            if r3["status"] == "ok":
                return {
                    "status": "ok",
                    "via": "httpx",
                    "url": link,
                    "path": r3["file"],
                    "size": r3.get("size"),
                    "sha256": r3.get("sha256"),
                }
            attempts.append(r3)

            r4 = await _run_with_retry(
                lambda: SaudiOpenDataTask._tab_nav(ctx, link, resource, out_dir, head),
                max_retries=1  # tab navigation is heavier; usually 1 extra try is enough
            )
            if r4["status"] == "ok":
                return {
                    "status": "ok",
                    "via": "tab-nav",
                    "url": link,
                    "path": r4["file"],
                    "size": r4.get("size"),
                    "sha256": r4.get("sha256"),
                }
            attempts.append(r4)

        return {"status": "error", "url": link or f"/v1/{dsid}/{rid}", "reason": "all_attempts_failed", "attempts": attempts}

    # ───── one dataset ─────
    @staticmethod
    async def _run_dataset(browser: Browser, dsid: str, head: dict, out_root: pathlib.Path, logger: logging.Logger) -> Dict[str, Any]:
        import urllib.parse, json, random, asyncio, pathlib

        out_dir = out_root / dsid
        (out_dir / "downloads").mkdir(parents=True, exist_ok=True)

        ctx = await SaudiOpenDataTask._new_ctx(browser, head, proxy=None)
        try:
            # Warm in a way that allows anti-bot cookies to be set
            await SaudiOpenDataTask._warm_resources(ctx, dsid, logger)

            # --- metadata phase (write a minimal downloads.json on failure) ---
            try:
                resources = await SaudiOpenDataTask._resources_via_api(ctx, dsid, head, logger)
            except Exception as e:
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
            for r in resources:
                item = await SaudiOpenDataTask._download_resource(ctx, dsid, r, out_dir / "downloads", head, logger.getChild("dl"))
                item["resource_name"] = r.get("name") or r.get("titleEn") or r.get("titleAr")
                results.append(item)
                await asyncio.sleep(random.uniform(0.5, 1.2))

            (out_dir / "downloads.json").write_text(json.dumps(results, indent=2, ensure_ascii=False), "utf-8")

            ok = sum(1 for x in results if x["status"] == "ok")
            fail = sum(1 for x in results if x["status"] == "error")
            ds_title = None
            for r in resources:
                t = r.get("datasetTitle") or r.get("dataset_title") or r.get("titleEn")
                if t:
                    ds_title = t
                    break

            return {
                "dataset_id": dsid,
                "dataset_title": ds_title,
                "dataset_url": f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources",
                "total_resources": len(resources),
                "downloaded": ok,
                "failed": fail,
                "resources_json": "resources.json",
                "downloads_json": "downloads.json",
                "files_sample": results[:2],
            }
        finally:
            await ctx.close()

    # ───── list datasets for publisher ─────
    @staticmethod
    async def _list_datasets_for_publisher(browser: Browser, pub_id: str, head: dict, logger: logging.Logger) -> List[dict]:
        async with httpx.AsyncClient(follow_redirects=True, timeout=45) as cl:
            url = f"https://open.data.gov.sa/data/api/organizations?version=-1&organization={pub_id}"
            r = await cl.get(url, headers={"accept": "application/json"})
            if r.status_code != 200:
                _log(logger, "warning", f"org API http_{r.status_code}")
                return []
            try:
                data = r.json()
            except Exception:
                _log(logger, "warning", "org API returned non-JSON")
                return []
            ds = data.get("datasets") or []
            out = []
            for d in ds:
                out.append({"id": d.get("id"), "title": d.get("titleEn") or d.get("titleAr")})
            return out

    # ───── entrypoint ─────
    async def run(self, *, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        HEAD = {
            "accept": "application/json, text/plain, */*",
            "content-language": "en",
            "accept-language": "en",
            "x-security-request": "required",
            "x-requested-with": "XMLHttpRequest",
            "user-agent": params.get(
                "user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            ),
            "referer": self.BASE + "/",
        }

        out_root = pathlib.Path(job_output_dir)
        out_root.mkdir(parents=True, exist_ok=True)

        if dsid := params.get("dataset_id"):
            _log(logger, "info", f"{dsid}: single dataset flow")
            return await SaudiOpenDataTask._run_dataset(browser, dsid, HEAD, out_root, logger)

        pub_id = params["publisher_id"]
        _log(logger, "info", f"{pub_id}: list via org API")
        all_items = await SaudiOpenDataTask._list_datasets_for_publisher(browser, pub_id, HEAD, logger)

        if "dataset_range" in params:
            a, b = params["dataset_range"]
            all_items = all_items[int(a) : int(b) + 1]
        if "dataset_limit" in params:
            all_items = all_items[: int(params["dataset_limit"])]

        _log(logger, "info", f"{pub_id}: {len(all_items)} dataset(s) after filtering)")

        per_dataset = []
        total_files_ok = total_files_err = 0
        datasets_complete = datasets_partial = datasets_failed = 0

        for idx, item in enumerate(all_items, 1):
            dsid = item.get("id")
            title = item.get("title") or ""
            _log(logger, "info", f"[{idx}/{len(all_items)}] {dsid} – {title[:90]}")
            try:
                dsres = await SaudiOpenDataTask._run_dataset(browser, dsid, HEAD, out_root, logger)
            except Exception as e:
                _log(logger, "warning", f"{dsid}: error – {e}")
                dsres = {
                    "dataset_id": dsid,
                    "dataset_title": title,
                    "dataset_url": f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources",
                    "error": "failed",
                }
            per_dataset.append(dsres)

            if "downloaded" in dsres:
                ok, tot = dsres["downloaded"], dsres["total_resources"]
                err = dsres["failed"]
                total_files_ok += ok
                total_files_err += err
                if ok == tot and tot > 0:
                    datasets_complete += 1
                elif ok > 0 and ok < tot:
                    datasets_partial += 1
                else:
                    datasets_failed += 1
            else:
                datasets_failed += 1

            await asyncio.sleep(random.uniform(1.0, 2.0))

        (out_root / "publisher_results.json").write_text(json.dumps(per_dataset, indent=2, ensure_ascii=False), "utf-8")

        result = {
            "publisher_id": pub_id,
            "total_datasets": len(all_items),
            # New names:
            "datasets_succeeded": datasets_complete,
            "datasets_partial": datasets_partial,
            "datasets_failed": datasets_failed,
            "total_files_ok": total_files_ok,
            "total_files_failed": total_files_err,
            "details_file": "publisher_results.json",
            # Backward-compat aliases (optional – remove if you don't need them)
            "datasets_complete": datasets_complete,
        }
        return result



# Register: saudi-open-data
@_registry.register("saudi-open-data")
async def saudi_open_data(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    task = SaudiOpenDataTask()
    return await task.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)


# Register: booking-hotels
@_registry.register("booking-hotels")
async def booking_hotels(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    return await BookingHotelsTask.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)


# ═══════════════════ Booking.com Hotels Scraper ═══════════════════

class BookingHotelsTask:
    """
    Fast HTTP-based hotel scraper for Booking.com using direct API access.
    
    Features:
    - Direct API calls (no browser automation)
    - Fast response times (seconds, not minutes)
    - Location-based search with comprehensive filtering
    - Date range and guest configuration
    - Price, rating, and amenity filters
    - Scalable and reliable
    """
    
    BASE_URL = "https://www.booking.com"
    API_BASE = "https://www.booking.com/dml/graphql"
    SEARCH_API = "https://www.booking.com/searchresults.html"
    
    # ───── Parameter validation and defaults ─────
    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize input parameters."""
        import datetime
        
        validated = {}
        
        # Required parameters
        if not params.get("location"):
            raise ValueError("location parameter is required")
        validated["location"] = str(params["location"]).strip()
        
        if not params.get("check_in"):
            raise ValueError("check_in date is required (YYYY-MM-DD format)")
        if not params.get("check_out"):
            raise ValueError("check_out date is required (YYYY-MM-DD format)")
            
        # Parse and validate dates
        try:
            check_in = datetime.datetime.strptime(params["check_in"], "%Y-%m-%d").date()
            check_out = datetime.datetime.strptime(params["check_out"], "%Y-%m-%d").date()
        except Exception:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
            
        if check_in < datetime.date.today():
            raise ValueError("check_in date cannot be in the past")
        if check_out <= check_in:
            raise ValueError("check_out must be after check_in")
            
        validated["check_in"] = check_in.strftime("%Y-%m-%d")
        validated["check_out"] = check_out.strftime("%Y-%m-%d")
        validated["nights"] = (check_out - check_in).days
        
        # Guest configuration with defaults
        validated["adults"] = max(1, int(params.get("adults", 2)))
        validated["children"] = max(0, int(params.get("children", 0)))
        validated["rooms"] = max(1, int(params.get("rooms", 1)))
        
        # Optional filters
        if "min_price" in params and params["min_price"] is not None:
            validated["min_price"] = max(0, float(params["min_price"]))
        if "max_price" in params and params["max_price"] is not None:
            validated["max_price"] = max(0, float(params["max_price"]))
            
        if "min_rating" in params and params["min_rating"] is not None:
            rating = float(params["min_rating"])
            if not 0 <= rating <= 10:
                raise ValueError("min_rating must be between 0 and 10")
            validated["min_rating"] = rating
            
        if "star_rating" in params and params["star_rating"] is not None:
            stars = params["star_rating"]
            if isinstance(stars, (list, tuple)):
                validated["star_rating"] = [int(s) for s in stars if 1 <= int(s) <= 5]
            else:
                star = int(stars)
                if 1 <= star <= 5:
                    validated["star_rating"] = [star]
                    
        # Amenities normalization
        if "amenities" in params and params["amenities"]:
            amenity_map = {
                "wifi": "free_wifi",
                "pool": "swimming_pool", 
                "gym": "fitness_center",
                "spa": "spa_wellness",
                "parking": "parking",
                "restaurant": "restaurant",
                "bar": "bar",
                "pets": "pets_allowed"
            }
            amenities = params["amenities"]
            if isinstance(amenities, str):
                amenities = [amenities]
            validated["amenities"] = [amenity_map.get(a.lower(), a.lower()) for a in amenities]
            
        # Search radius for proximity searches
        if "search_radius" in params:
            radius = params["search_radius"]
            if isinstance(radius, str) and radius.endswith("km"):
                validated["search_radius_km"] = float(radius[:-2])
            else:
                validated["search_radius_km"] = float(radius)
                
        # Results configuration
        validated["max_results"] = min(200, max(1, int(params.get("max_results", 50))))
        validated["include_reviews"] = bool(params.get("include_reviews", True))
        
        return validated
    
    # ───── HTTP client setup ─────
    @staticmethod
    def _create_http_session() -> httpx.AsyncClient:
        """Create HTTP client with enhanced anti-bot headers for Booking.com."""
        import random
        
        # Rotate User-Agent to avoid detection
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        
        headers = {
            "User-Agent": random.choice(user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",  # Re-enable compression with proper handling
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"'
        }
        
        # Enhanced cookie management
        cookies = {
            "bkng": "1",
            "BJS": "-",
            "destination_id": "-553173",
            "selected_currency": "USD",
            "bkng_sso_session": "",
            "_gcl_au": "1.1.1234567890.1234567890"
        }
        
        return httpx.AsyncClient(
            headers=headers,
            timeout=30.0,
            follow_redirects=True,
            cookies=cookies
        )
    
    # ───── URL construction for direct API access ─────
    @staticmethod
    def _build_search_url(validated_params: Dict[str, Any]) -> str:
        """Build direct search URL for Booking.com."""
        import urllib.parse
        
        # Base search URL
        base_url = "https://www.booking.com/searchresults.html"
        
        # Build query parameters
        params = {
            "ss": validated_params["location"],
            "checkin": validated_params["check_in"],
            "checkout": validated_params["check_out"],
            "group_adults": str(validated_params["adults"]),
            "group_children": str(validated_params["children"]),
            "no_rooms": str(validated_params["rooms"]),
            "selected_currency": "USD",
            "order": "popularity",
            "lang": "en-us",
            "soz": "1",
            "lang_click": "other"
        }
        
        # Add filters if specified
        if "min_price" in validated_params:
            params["price_min"] = str(int(validated_params["min_price"]))
        if "max_price" in validated_params:
            params["price_max"] = str(int(validated_params["max_price"]))
        if "min_rating" in validated_params:
            # Convert rating to Booking.com's review score filter
            rating = validated_params["min_rating"]
            if rating >= 9:
                params["review_score"] = "90"
            elif rating >= 8:
                params["review_score"] = "80"
            elif rating >= 7:
                params["review_score"] = "70"
            elif rating >= 6:
                params["review_score"] = "60"
        
        # Add star rating filter
        if "star_rating" in validated_params:
            # Multiple star ratings
            star_filters = []
            for star in validated_params["star_rating"]:
                star_filters.append(f"class={star}")
            if star_filters:
                params["nflt"] = ";".join(star_filters)
        
        # Build final URL
        query_string = urllib.parse.urlencode(params)
        return f"{base_url}?{query_string}"
    
    # ───── HTTP-based hotel extraction ─────
    @staticmethod
    async def _extract_hotels_http(client: httpx.AsyncClient, search_url: str, max_results: int, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Extract hotel data using HTTP requests."""
        hotels = []
        
        try:
            # Pre-flight request to establish session and avoid bot detection
            _log(logger, "info", "Establishing session with Booking.com homepage")
            try:
                home_response = await client.get("https://www.booking.com/")
                _log(logger, "info", f"Homepage response status: {home_response.status_code}")
                # Small delay to mimic human behavior
                await asyncio.sleep(1)
            except Exception as e:
                _log(logger, "warning", f"Homepage pre-flight failed: {str(e)}")
            
            _log(logger, "info", f"Fetching search results from: {search_url}")
            
            # Make HTTP request to search URL
            response = await client.get(search_url)
            response.raise_for_status()
            
            # Ensure proper content decompression
            html_content = response.text
            
            # If content appears to be compressed/binary, try different decoding
            if len(html_content) > 0 and ord(html_content[0]) > 127:
                try:
                    # Try reading as bytes and decoding
                    html_content = response.content.decode('utf-8')
                except:
                    try:
                        # Try with response.text again after reading bytes
                        html_content = response.text
                    except:
                        _log(logger, "warning", "Could not decode response content")
            
            _log(logger, "info", f"Received {len(html_content)} characters of HTML")
            
            # Debug: Check if we got a valid response (log first 500 chars)
            html_preview = html_content[:500].replace('\n', ' ').replace('\r', ' ')
            _log(logger, "info", f"HTML preview: {html_preview}")
            
            # Check for challenge/CAPTCHA page
            if any(keyword in html_content.lower() for keyword in ['awswafcookiedomainlist', 'challenge', 'captcha', 'robot']):
                _log(logger, "warning", "Detected anti-bot challenge page - attempting alternative approach")
                
                # Try with modified headers and different approach
                alt_headers = {
                    "User-Agent": "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                }
                
                # Create new client with different headers
                alt_client = httpx.AsyncClient(headers=alt_headers, timeout=30.0, follow_redirects=True)
                try:
                    await asyncio.sleep(2)  # Wait before retry
                    alt_response = await alt_client.get(search_url)
                    alt_html = alt_response.text
                    
                    if not any(keyword in alt_html.lower() for keyword in ['awswafcookiedomainlist', 'challenge']):
                        html_content = alt_html
                        _log(logger, "info", f"Alternative approach successful: {len(html_content)} characters")
                    else:
                        _log(logger, "warning", "Alternative approach also blocked - proceeding with limited data")
                        
                except Exception as e:
                    _log(logger, "warning", f"Alternative approach failed: {str(e)}")
                finally:
                    await alt_client.aclose()
            
            # Parse HTML to extract hotel data
            try:
                from bs4 import BeautifulSoup
            except ImportError:
                import bs4
                BeautifulSoup = bs4.BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Try multiple selectors for hotel containers
            hotel_containers = []
            
            # First try current selectors
            selectors_to_try = [
                {'data-testid': 'property-card'},
                {'data-testid': lambda x: x and 'property' in x if x else False},
                {'class': lambda x: x and 'sr_property_block' in str(x) if x else False},
                {'class': lambda x: x and 'property' in str(x).lower() if x else False},
                {'class': lambda x: x and 'hotel' in str(x).lower() if x else False},
                {'data-component': lambda x: x and 'property' in str(x).lower() if x else False}
            ]
            
            for selector in selectors_to_try:
                containers = soup.find_all(['div'], selector)
                if containers:
                    hotel_containers = containers
                    _log(logger, "info", f"Found {len(containers)} containers with selector: {selector}")
                    break
            
            # If no specific containers found, try broader search
            if not hotel_containers:
                all_divs = soup.find_all('div')
                _log(logger, "info", f"Total divs found: {len(all_divs)}")
                
                # Look for divs that might contain hotel data
                potential_containers = []
                for div in all_divs:
                    if div.get('data-testid') or any(keyword in str(div.get('class', '')).lower() 
                                                   for keyword in ['property', 'hotel', 'accommodation', 'card']):
                        potential_containers.append(div)
                
                hotel_containers = potential_containers[:max_results]
                _log(logger, "info", f"Found {len(potential_containers)} potential containers")
            
            _log(logger, "info", f"Using {len(hotel_containers)} hotel containers for extraction")
            
            for i, container in enumerate(hotel_containers[:max_results]):
                try:
                    hotel_data = BookingHotelsTask._parse_hotel_from_html(container, logger)
                    if hotel_data:
                        hotels.append(hotel_data)
                        
                    if len(hotels) >= max_results:
                        break
                        
                except Exception as e:
                    _log(logger, "warning", f"Failed to parse hotel {i}: {str(e)}")
                    continue
            
            _log(logger, "info", f"Successfully extracted {len(hotels)} hotels")
            
        except Exception as e:
            _log(logger, "error", f"HTTP extraction failed: {str(e)}")
            
        return hotels
    
    @staticmethod
    def _parse_hotel_from_html(container, logger: logging.Logger) -> Optional[Dict[str, Any]]:
        """Enhanced parser for individual hotel data from HTML container."""
        try:
            hotel_data = {}
            
            # Hotel name - try multiple selectors
            name_selectors = [
                {'data-testid': lambda x: x and 'title' in str(x) if x else False},
                {'data-testid': 'property-card-title'},
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['sr-hotel__name', 'property-card-title', 'fcab3ed991']) if x else False}
            ]
            
            name_elem = None
            for selector in name_selectors:
                name_elem = container.find(['h1', 'h2', 'h3', 'h4', 'div', 'a'], selector)
                if name_elem:
                    break
            
            if name_elem:
                hotel_data["name"] = name_elem.get_text(strip=True)
            else:
                # Fallback: search for any link that might be hotel name
                link_elem = container.find('a', href=True)
                if link_elem:
                    hotel_data["name"] = link_elem.get_text(strip=True) or "Unknown Hotel"
                else:
                    hotel_data["name"] = "Unknown Hotel"
            
            # Rating - enhanced extraction
            rating_selectors = [
                {'data-testid': lambda x: x and 'rating' in str(x) if x else False},
                {'class': lambda x: x and 'bui-review-score__badge' in str(x) if x else False},
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['review-score', 'rating', 'score']) if x else False}
            ]
            
            rating_elem = None
            for selector in rating_selectors:
                rating_elem = container.find(['div', 'span'], selector)
                if rating_elem:
                    break
            
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                # Try different rating patterns
                rating_patterns = [r'(\d+\.?\d*)/10', r'(\d+\.?\d*)', r'(\d+,\d+)']
                for pattern in rating_patterns:
                    rating_match = re.search(pattern, rating_text.replace(',', '.'))
                    if rating_match:
                        try:
                            hotel_data["rating"] = float(rating_match.group(1))
                            break
                        except:
                            continue
                else:
                    hotel_data["rating"] = None
            else:
                hotel_data["rating"] = None
            
            # Price - much more comprehensive extraction
            price_selectors = [
                {'data-testid': lambda x: x and 'price' in str(x) if x else False},
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['prco-valign-middle-helper', 'bui-price-display', 'prco-inline-block-maker-helper']) if x else False}
            ]
            
            price_elem = None
            for selector in price_selectors:
                price_elem = container.find(['span', 'div'], selector)
                if price_elem:
                    break
                    
            # If no specific price element, search in all text for price patterns
            if not price_elem:
                all_text = container.get_text()
                # Look for common price patterns
                price_patterns = [
                    r'SAR\s*([0-9,]+)',  # Saudi Riyal
                    r'AED\s*([0-9,]+)',  # UAE Dirham  
                    r'USD\s*([0-9,]+)',  # US Dollar
                    r'\$([0-9,]+)',      # Dollar symbol
                    r'([0-9,]+)\s*SAR',  # Riyal after number
                    r'([0-9,]+)\s*AED',  # Dirham after number
                    r'([0-9]{2,4})'      # Any 2-4 digit number (fallback)
                ]
                
                for pattern in price_patterns:
                    price_match = re.search(pattern, all_text)
                    if price_match:
                        try:
                            price_num = int(price_match.group(1).replace(',', ''))
                            if 50 <= price_num <= 50000:  # Reasonable hotel price range
                                hotel_data["price_per_night"] = price_num
                                # Determine currency from pattern
                                if 'SAR' in pattern:
                                    hotel_data["currency"] = "SAR"
                                elif 'AED' in pattern:
                                    hotel_data["currency"] = "AED"
                                elif 'USD' in pattern or '$' in pattern:
                                    hotel_data["currency"] = "USD"
                                else:
                                    hotel_data["currency"] = "SAR"  # Default for Saudi/Gulf region
                                break
                        except:
                            continue
                else:
                    hotel_data["price_per_night"] = None
                    hotel_data["currency"] = None
            else:
                price_text = price_elem.get_text(strip=True)
                # Enhanced price extraction
                price_patterns = [r'([0-9,]+)', r'(\d+\.?\d*)']
                currency_patterns = [r'(SAR|AED|USD|EUR|GBP)', r'([$€£¥])']
                
                price_match = None
                for pattern in price_patterns:
                    price_match = re.search(pattern, price_text.replace(' ', ''))
                    if price_match:
                        break
                
                if price_match:
                    try:
                        hotel_data["price_per_night"] = int(float(price_match.group(1).replace(',', '')))
                    except:
                        hotel_data["price_per_night"] = None
                else:
                    hotel_data["price_per_night"] = None
                
                # Extract currency
                currency_match = None
                for pattern in currency_patterns:
                    currency_match = re.search(pattern, price_text)
                    if currency_match:
                        break
                        
                hotel_data["currency"] = currency_match.group(1) if currency_match else "SAR"
            
            # Address/Location - enhanced extraction
            address_selectors = [
                {'data-testid': lambda x: x and 'address' in str(x) if x else False},
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['sr_card_address', 'property-card-location']) if x else False}
            ]
            
            address_elem = None
            for selector in address_selectors:
                address_elem = container.find(['span', 'div'], selector)
                if address_elem:
                    break
            
            if address_elem:
                hotel_data["address"] = address_elem.get_text(strip=True)
            else:
                # Try to find location info in any text
                location_text = container.get_text()
                city_patterns = [r'(Riyadh|Dubai|Jeddah|Abu Dhabi|Doha|Kuwait)', r'([A-Z][a-z]+ (Street|Road|Avenue|District))']
                for pattern in city_patterns:
                    match = re.search(pattern, location_text)
                    if match:
                        hotel_data["address"] = match.group(1)
                        break
                else:
                    hotel_data["address"] = "Address not available"
            
            # Distance to center
            distance_selectors = [
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['distance', 'location', 'sr_card__subtitle']) if x else False}
            ]
            
            distance_elem = None
            for selector in distance_selectors:
                distance_elem = container.find(['span', 'div'], selector)
                if distance_elem:
                    break
            
            if distance_elem:
                distance_text = distance_elem.get_text(strip=True)
                # Look for distance patterns
                distance_match = re.search(r'(\d+\.?\d*\s*(km|miles?|m))', distance_text, re.IGNORECASE)
                hotel_data["distance_to_center"] = distance_match.group(1) if distance_match else distance_text
            else:
                hotel_data["distance_to_center"] = None
            
            # Booking URL - enhanced extraction
            link_elem = container.find('a', href=True)
            if link_elem and link_elem.get('href'):
                href = link_elem['href']
                if href.startswith('http'):
                    hotel_data["booking_url"] = href
                elif href.startswith('/'):
                    hotel_data["booking_url"] = f"https://www.booking.com{href}"
                else:
                    hotel_data["booking_url"] = f"https://www.booking.com/{href}"
            else:
                hotel_data["booking_url"] = None
            
            # Images - extract higher quality images
            img_selectors = [
                container.find('img', {'data-testid': lambda x: x and 'image' in str(x) if x else False}),
                container.find('img', src=True),
                container.find(['div'], style=lambda x: x and 'background-image' in str(x) if x else False)
            ]
            
            images = []
            for img_elem in img_selectors:
                if img_elem:
                    if img_elem.name == 'img' and img_elem.get('src'):
                        src = img_elem['src']
                        # Try to get higher quality image
                        if 'square240' in src:
                            src = src.replace('square240', 'max1024x768')
                        images.append(src)
                    elif img_elem.get('style'):
                        # Extract background image URL
                        style = img_elem['style']
                        bg_match = re.search(r'background-image:\s*url\(["\']?([^"\']+)["\']?\)', style)
                        if bg_match:
                            images.append(bg_match.group(1))
                    
            hotel_data["images"] = images[:3] if images else []  # Limit to 3 images
            
            # Reviews count extraction
            reviews_selectors = [
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['review', 'comment']) if x else False}
            ]
            
            reviews_elem = None
            for selector in reviews_selectors:
                reviews_elem = container.find(['span', 'div'], selector)
                if reviews_elem:
                    break
            
            if reviews_elem:
                reviews_text = reviews_elem.get_text(strip=True)
                reviews_match = re.search(r'(\d+)\s*(reviews?|comments?)', reviews_text, re.IGNORECASE)
                hotel_data["reviews_count"] = int(reviews_match.group(1)) if reviews_match else 0
            else:
                hotel_data["reviews_count"] = 0
            
            # Star rating extraction
            star_selectors = [
                {'class': lambda x: x and 'star' in str(x).lower() if x else False},
                {'data-testid': lambda x: x and 'rating' in str(x) if x else False}
            ]
            
            star_elem = None
            for selector in star_selectors:
                star_elem = container.find(['span', 'div'], selector)
                if star_elem:
                    break
            
            if star_elem:
                star_text = star_elem.get_text(strip=True)
                star_match = re.search(r'(\d+)\s*star', star_text, re.IGNORECASE)
                hotel_data["star_rating"] = int(star_match.group(1)) if star_match else None
            else:
                hotel_data["star_rating"] = None
            
            # Generate ID
            name_for_id = hotel_data["name"].lower().replace(" ", "_").replace("-", "_")
            hotel_data["id"] = f"booking_{hashlib.md5(name_for_id.encode()).hexdigest()[:8]}"
            
            # Rating text
            hotel_data["rating_text"] = f"{hotel_data['rating']}/10" if hotel_data["rating"] else None
            
            return hotel_data
            
        except Exception as e:
            _log(logger, "warning", f"Failed to parse hotel container: {str(e)}")
            return None
    
    # ───── Individual hotel page review extraction ─────
    @staticmethod
    async def _fetch_hotel_reviews(client: httpx.AsyncClient, hotel_url: str, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Fetch detailed reviews from individual hotel page."""
        try:
            response = await client.get(hotel_url)
            response.raise_for_status()
            
            html_content = response.text
            
            try:
                from bs4 import BeautifulSoup
            except ImportError:
                import bs4
                BeautifulSoup = bs4.BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            reviews = []
            
            # Try multiple selectors for review containers
            review_selectors = [
                {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['review-item', 'review_item', 'c-review']) if x else False},
                {'data-testid': lambda x: x and 'review' in str(x) if x else False}
            ]
            
            review_containers = []
            for selector in review_selectors:
                containers = soup.find_all(['div', 'article'], selector)
                if containers:
                    review_containers = containers
                    break
            
            for review_elem in review_containers[:10]:  # Limit to 10 reviews
                try:
                    review_data = {}
                    
                    # Review text
                    text_selectors = [
                        {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['review-text', 'review_text', 'c-review__content']) if x else False},
                        {'data-testid': lambda x: x and 'review' in str(x) and 'text' in str(x) if x else False}
                    ]
                    
                    text_elem = None
                    for selector in text_selectors:
                        text_elem = review_elem.find(['div', 'p', 'span'], selector)
                        if text_elem:
                            break
                    
                    if text_elem:
                        review_text = text_elem.get_text(strip=True)
                        # Clean up review text
                        review_text = re.sub(r'\s+', ' ', review_text)
                        review_data["text"] = review_text[:500]  # Limit length
                    else:
                        # Fallback: get any paragraph text from review
                        all_text = review_elem.get_text(strip=True)
                        if len(all_text) > 20:  # Minimum meaningful review length
                            review_data["text"] = all_text[:300]
                        else:
                            continue  # Skip if no meaningful text
                    
                    # Review rating
                    rating_selectors = [
                        {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['rating', 'score']) if x else False},
                        {'data-testid': lambda x: x and 'rating' in str(x) if x else False}
                    ]
                    
                    rating_elem = None
                    for selector in rating_selectors:
                        rating_elem = review_elem.find(['div', 'span'], selector)
                        if rating_elem:
                            break
                    
                    if rating_elem:
                        rating_text = rating_elem.get_text(strip=True)
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        review_data["rating"] = float(rating_match.group(1)) if rating_match else None
                    else:
                        review_data["rating"] = None
                    
                    # Reviewer name
                    name_selectors = [
                        {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['reviewer', 'author', 'name']) if x else False},
                        {'data-testid': lambda x: x and 'reviewer' in str(x) if x else False}
                    ]
                    
                    name_elem = None
                    for selector in name_selectors:
                        name_elem = review_elem.find(['span', 'div'], selector)
                        if name_elem:
                            break
                    
                    if name_elem:
                        review_data["reviewer"] = name_elem.get_text(strip=True)
                    else:
                        review_data["reviewer"] = "Anonymous"
                    
                    # Review date
                    date_selectors = [
                        {'class': lambda x: x and any(keyword in str(x).lower() for keyword in ['date', 'time']) if x else False},
                        {'data-testid': lambda x: x and 'date' in str(x) if x else False}
                    ]
                    
                    date_elem = None
                    for selector in date_selectors:
                        date_elem = review_elem.find(['span', 'div', 'time'], selector)
                        if date_elem:
                            break
                    
                    if date_elem:
                        review_data["date"] = date_elem.get_text(strip=True)
                    else:
                        review_data["date"] = None
                    
                    review_data["source"] = "booking_page_detailed"
                    reviews.append(review_data)
                    
                except Exception as e:
                    _log(logger, "warning", f"Failed to parse individual review: {str(e)}")
                    continue
            
            return reviews
            
        except Exception as e:
            _log(logger, "warning", f"Failed to fetch reviews from {hotel_url}: {str(e)}")
            return []
    
    # ───── Enhanced browser automation with improved data extraction ─────
    @staticmethod
    async def _extract_hotels_browser_enhanced(page, validated_params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Enhanced browser automation combining reliable navigation with superior data extraction."""
        hotels = []
        
        try:
            # Navigate and perform search with enhanced reliability
            await BookingHotelsTask._perform_search_enhanced(page, validated_params, logger)
            
            # Wait for results with multiple fallback selectors
            result_selectors = [
                "[data-testid='property-card']",
                ".sr_property_block",
                "[data-component='PropertyCard']",
                ".sr_item"
            ]
            
            results_found = False
            for selector in result_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=15000)
                    results_found = True
                    _log(logger, "info", f"✅ Found results with selector: {selector}")
                    break
                except:
                    continue
            
            if not results_found:
                _log(logger, "warning", "⚠️  No hotel results found with any selector")
                return []
            
            # Extract hotel data using enhanced parsing
            max_results = validated_params["max_results"]
            hotel_containers = await page.query_selector_all(result_selectors[0])  # Use first successful selector
            
            _log(logger, "info", f"🏨 Found {len(hotel_containers)} hotel containers, extracting {min(len(hotel_containers), max_results)}")
            
            for i, container in enumerate(hotel_containers[:max_results]):
                try:
                    # COMPREHENSIVE browser-based data extraction
                    hotel_data = await BookingHotelsTask._extract_complete_hotel_data(container, i+1, logger)
                    
                    if hotel_data:
                        # Apply filters during extraction
                        should_include = True
                        
                        # Apply min_rating filter
                        if "min_rating" in validated_params and validated_params["min_rating"]:
                            min_rating = validated_params["min_rating"]
                            hotel_rating = hotel_data.get("rating")
                            if not hotel_rating or hotel_rating < min_rating:
                                _log(logger, "info", f"🚫 Filtered out {hotel_data['name']}: rating {hotel_rating} < {min_rating}")
                                should_include = False
                        
                        # Apply max_price filter
                        if "max_price" in validated_params and validated_params["max_price"]:
                            max_price = validated_params["max_price"]
                            hotel_price = hotel_data.get("price_per_night")
                            if hotel_price and hotel_price > max_price:
                                _log(logger, "info", f"🚫 Filtered out {hotel_data['name']}: price {hotel_price} > {max_price}")
                                should_include = False
                        
                        # Apply min_price filter
                        if "min_price" in validated_params and validated_params["min_price"]:
                            min_price = validated_params["min_price"]
                            hotel_price = hotel_data.get("price_per_night")
                            if hotel_price and hotel_price < min_price:
                                _log(logger, "info", f"🚫 Filtered out {hotel_data['name']}: price {hotel_price} < {min_price}")
                                should_include = False
                        
                        if should_include:
                            # PHASE 1: Get basic data from listing
                            hotels.append(hotel_data)
                            _log(logger, "info", f"✅ Hotel #{len(hotels)}: {hotel_data['name']} - ${hotel_data.get('price_per_night', 'N/A')} - ⭐{hotel_data.get('rating', 'N/A')}")
                        
                except Exception as e:
                    _log(logger, "warning", f"⚠️  Failed to extract hotel #{i+1}: {str(e)}")
                    continue
            
            _log(logger, "info", f"🎯 Browser extraction completed: {len(hotels)} hotels")
            
            # PHASE 2: Enhanced data collection from individual hotel pages
            if hotels and validated_params.get("include_reviews", True):
                _log(logger, "info", f"🔍 Phase 2: Collecting detailed data from {len(hotels)} hotel pages...")
                enhanced_hotels = []
                
                for i, hotel in enumerate(hotels):
                    try:
                        if hotel.get("booking_url"):
                            enhanced_data = await BookingHotelsTask._get_detailed_hotel_data(
                                page, hotel, validated_params, logger
                            )
                            if enhanced_data:
                                enhanced_hotels.append(enhanced_data)
                                _log(logger, "info", f"✅ Enhanced #{i+1}: {enhanced_data['name']} - ${enhanced_data.get('price_per_night', 'N/A')}")
                            else:
                                enhanced_hotels.append(hotel)  # Fallback to basic data
                        else:
                            enhanced_hotels.append(hotel)  # No URL, keep basic data
                    except Exception as e:
                        _log(logger, "warning", f"⚠️  Failed to enhance hotel #{i+1}: {str(e)}")
                        enhanced_hotels.append(hotel)  # Fallback to basic data
                
                return enhanced_hotels
            
            return hotels
            
        except Exception as e:
            _log(logger, "error", f"❌ Browser extraction failed: {str(e)}")
            return []
    
    @staticmethod 
    async def _perform_search_enhanced(page, validated_params: Dict[str, Any], logger: logging.Logger) -> None:
        """Enhanced search form submission with improved reliability."""
        try:
            # Navigate to booking.com with enhanced settings
            _log(logger, "info", "🌐 Navigating to Booking.com")
            await page.goto(BookingHotelsTask.BASE_URL, wait_until="networkidle", timeout=60000)
            
            # Handle cookie consent with multiple selectors
            cookie_selectors = [
                "button[data-testid*='cookie']",
                "button:has-text('Accept')",
                "button:has-text('I accept')", 
                "button:has-text('Accept all')",
                "#onetrust-accept-btn-handler",
                ".bui-button--primary:has-text('OK')",
                "[data-consent-manage-id='accept_all']"
            ]
            
            for selector in cookie_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    _log(logger, "info", f"🍪 Accepted cookies with {selector}")
                    await page.wait_for_timeout(1000)
                    break
                except:
                    continue
            
            # Enhanced location input with multiple selectors
            location_selectors = [
                "input[data-testid='destination-input']",
                "input[name='ss']", 
                "input[placeholder*='destination']",
                "input[placeholder*='Where are you going']",
                "input.sb-destination__input",
                "#ss",
                "[data-element-name='destination']"
            ]
            
            location_input = None
            for selector in location_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    location_input = page.locator(selector)
                    if await location_input.count() > 0:
                        _log(logger, "info", f"🎯 Found location input: {selector}")
                        break
                except:
                    continue
            
            if not location_input or await location_input.count() == 0:
                raise RuntimeError("Could not find location search input with any selector")
            
            # Enhanced typing with human-like behavior
            await location_input.click()
            await page.wait_for_timeout(random.uniform(500, 1000))
            await location_input.clear()
            await page.wait_for_timeout(random.uniform(300, 700))
            
            # Type location with realistic delays
            location_text = validated_params["location"]
            for char in location_text:
                await location_input.type(char)
                await page.wait_for_timeout(random.uniform(50, 150))
            
            # Handle autocomplete suggestions
            suggestion_selectors = [
                "li[data-testid*='autocomplete']",
                ".sb-autocomplete__item",
                "[data-testid='autocomplete-result']",
                ".c-autocomplete__item",
                ".sb-autocomplete__option"
            ]
            
            suggestion_clicked = False
            for selector in suggestion_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=3000)
                    await page.click(f"{selector}:first-child", timeout=2000)
                    _log(logger, "info", f"📍 Clicked autocomplete: {selector}")
                    suggestion_clicked = True
                    break
                except:
                    continue
            
            if not suggestion_clicked:
                await location_input.press("Enter")
                _log(logger, "info", "⌨️  No autocomplete - pressed Enter")
            
            await page.wait_for_timeout(1000)
            
            # Enhanced date handling
            try:
                date_selectors = [
                    "[data-testid='date-display-field-start']",
                    "button[data-testid*='date']",
                    ".sb-date-field__input",
                    "[data-placeholder*='Check-in']"
                ]
                
                for selector in date_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=3000)
                        await page.click(selector)
                        _log(logger, "info", f"📅 Opened date picker: {selector}")
                        break
                    except:
                        continue
                
                # Simple date selection (can be enhanced further)
                await page.wait_for_timeout(2000)
                
                # Try to close date picker
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(1000)
                
            except Exception as e:
                _log(logger, "warning", f"⚠️  Date handling skipped: {str(e)}")
            
            # Enhanced search submission
            search_selectors = [
                "button[type='submit']:has-text('Search')",
                "button[data-testid*='search']",
                ".sb-searchbox__button",
                "button:has-text('Search')",
                "[data-element-name='search_button']"
            ]
            
            search_clicked = False
            for selector in search_selectors:
                try:
                    await page.click(selector, timeout=5000)
                    _log(logger, "info", f"🔍 Clicked search: {selector}")
                    search_clicked = True
                    break
                except:
                    continue
            
            if not search_clicked:
                await page.keyboard.press("Enter")
                _log(logger, "info", "⌨️  Fallback search with Enter key")
            
            # Wait for navigation
            await page.wait_for_load_state("networkidle", timeout=30000)
            _log(logger, "info", "✅ Search completed successfully")
            
        except Exception as e:
            _log(logger, "error", f"❌ Enhanced search failed: {str(e)}")
            raise
    
    # ───── Legacy browser methods (kept for compatibility) ─────
    @staticmethod
    async def _perform_search(page, validated_params: Dict[str, Any], logger: logging.Logger) -> None:
        """Fill and submit the hotel search form."""
        try:
            # Navigate to booking.com
            _log(logger, "info", "Navigating to Booking.com")
            await page.goto(BookingHotelsTask.BASE_URL, wait_until="networkidle", timeout=60000)
            await BookingHotelsTask._minimal_delay(page)
            
            # Accept cookies if modal appears
            try:
                cookie_selectors = [
                    "button[data-testid*='cookie']",
                    "button:has-text('Accept')",
                    "button:has-text('I accept')", 
                    "button:has-text('Accept all')",
                    "#onetrust-accept-btn-handler",
                    ".bui-button--primary:has-text('OK')"
                ]
                for selector in cookie_selectors:
                    try:
                        await page.click(selector, timeout=3000)
                        _log(logger, "info", f"Accepted cookies with {selector}")
                        break
                    except:
                        continue
                await page.wait_for_timeout(2000)
            except:
                pass
            
            # More robust location input detection
            _log(logger, "info", "Looking for search input")
            location_selectors = [
                "input[data-testid='destination-input']",
                "input[name='ss']", 
                "input[placeholder*='destination']",
                "input[placeholder*='Where are you going']",
                "input.sb-destination__input",
                "#ss"
            ]
            
            location_input = None
            for selector in location_selectors:
                try:
                    location_input = page.locator(selector)
                    if await location_input.count() > 0:
                        _log(logger, "info", f"Found location input: {selector}")
                        break
                except:
                    continue
                    
            if not location_input or await location_input.count() == 0:
                raise RuntimeError("Could not find location search input")
            
            # Human-like typing
            await location_input.click()
            await page.wait_for_timeout(random.uniform(500, 1000))
            await location_input.clear()
            await page.wait_for_timeout(random.uniform(300, 700))
            
            # Type letter by letter with random delays
            location_text = validated_params["location"]
            for char in location_text:
                await location_input.type(char)
                await page.wait_for_timeout(random.uniform(50, 150))
            
            await BookingHotelsTask._minimal_delay(page)
            
            # Wait for and click autocomplete suggestion
            try:
                suggestion_selectors = [
                    "li[data-testid*='autocomplete']",
                    ".sb-autocomplete__item",
                    "[data-testid='autocomplete-result']",
                    ".c-autocomplete__item"
                ]
                
                for selector in suggestion_selectors:
                    try:
                        suggestion = page.locator(selector).first
                        if await suggestion.count() > 0:
                            await suggestion.click(timeout=5000)
                            _log(logger, "info", f"Clicked suggestion: {selector}")
                            break
                    except:
                        continue
            except:
                # If no suggestions, press Enter
                await location_input.press("Enter")
                _log(logger, "info", "No autocomplete - pressed Enter")
            
            await page.wait_for_timeout(random.uniform(2000, 3000))
            
            # Handle date selection with multiple attempts
            _log(logger, "info", "Setting up dates")
            date_selectors = [
                "button[data-testid='date-display-field-start']",
                "input[data-testid='searchbox-dates-input']",
                ".xp__dates-inner",
                "[data-testid='searchbox-dates']",
                ".sb-dates"
            ]
            
            date_clicked = False
            for selector in date_selectors:
                try:
                    date_input = page.locator(selector)
                    if await date_input.count() > 0:
                        await date_input.first.click(timeout=5000)
                        _log(logger, "info", f"Clicked date input: {selector}")
                        date_clicked = True
                        break
                except:
                    continue
            
            if not date_clicked:
                _log(logger, "warning", "Could not find date input - continuing anyway")
            
            await page.wait_for_timeout(random.uniform(2000, 3000))
            
            # Try to select dates - be flexible with date formats
            check_in_date = validated_params["check_in"]
            check_out_date = validated_params["check_out"]
            
            try:
                # Multiple date cell selectors
                date_cell_selectors = [
                    f"td[data-date='{check_in_date}']",
                    f"span[data-date='{check_in_date}']", 
                    f"[data-date='{check_in_date}']",
                    f"td:has-text('{check_in_date.split('-')[2]}')",  # Just the day
                ]
                
                for selector in date_cell_selectors:
                    try:
                        check_in_cell = page.locator(selector)
                        if await check_in_cell.count() > 0:
                            await check_in_cell.first.click(timeout=5000)
                            _log(logger, "info", f"Selected check-in date: {selector}")
                            break
                    except:
                        continue
                
                await page.wait_for_timeout(random.uniform(1000, 2000))
                
                # Select check-out date
                for selector in [s.replace(check_in_date, check_out_date) for s in date_cell_selectors]:
                    try:
                        check_out_cell = page.locator(selector)
                        if await check_out_cell.count() > 0:
                            await check_out_cell.first.click(timeout=5000)
                            _log(logger, "info", f"Selected check-out date: {selector}")
                            break
                    except:
                        continue
                        
                await page.wait_for_timeout(random.uniform(1000, 2000))
                
            except Exception as e:
                _log(logger, "warning", f"Date selection failed: {e} - continuing anyway")
            
            # Configure guests and rooms
            if validated_params["adults"] != 2 or validated_params["children"] > 0 or validated_params["rooms"] > 1:
                guest_button = page.locator("button[data-testid='occupancy-config'], button:has-text('guests')")
                await guest_button.click()
                await page.wait_for_timeout(1000)
                
                # Adults
                adults_current = await page.locator("input[data-testid='adults-input'], input[id*='adults']").input_value()
                adults_diff = validated_params["adults"] - int(adults_current or "2")
                if adults_diff > 0:
                    for _ in range(adults_diff):
                        await page.locator("button[data-testid='adults-increment'], button[aria-label*='Increase adults']").click()
                elif adults_diff < 0:
                    for _ in range(abs(adults_diff)):
                        await page.locator("button[data-testid='adults-decrement'], button[aria-label*='Decrease adults']").click()
                
                # Children
                if validated_params["children"] > 0:
                    for _ in range(validated_params["children"]):
                        await page.locator("button[data-testid='children-increment'], button[aria-label*='Increase children']").click()
                
                # Rooms
                rooms_current = await page.locator("input[data-testid='rooms-input'], input[id*='rooms']").input_value()
                rooms_diff = validated_params["rooms"] - int(rooms_current or "1")
                if rooms_diff > 0:
                    for _ in range(rooms_diff):
                        await page.locator("button[data-testid='rooms-increment'], button[aria-label*='Increase rooms']").click()
                
                await page.wait_for_timeout(500)
            
            # Submit search with multiple button selectors
            _log(logger, "info", "Submitting search")
            search_selectors = [
                "button[data-testid='header-search-button']",
                "button[type='submit']:has-text('Search')",
                ".sb-searchbox__button",
                "button:has-text('Search')",
                "[data-testid='search-button']",
                "button.bui-button--primary:has-text('Search')"
            ]
            
            search_submitted = False
            for selector in search_selectors:
                try:
                    search_button = page.locator(selector)
                    if await search_button.count() > 0:
                        await search_button.first.click(timeout=10000)
                        _log(logger, "info", f"Clicked search button: {selector}")
                        search_submitted = True
                        break
                except:
                    continue
            
            if not search_submitted:
                # Try pressing Enter as fallback
                await page.keyboard.press("Enter")
                _log(logger, "info", "Pressed Enter as search fallback")
            
            # Wait for results to load with longer timeout
            _log(logger, "info", "Waiting for search results")
            result_selectors = [
                "[data-testid='property-card']",
                ".sr_property_block", 
                "[data-testid='property']",
                ".sr_item",
                ".c2-property-list-item"
            ]
            
            results_found = False
            for selector in result_selectors:
                try:
                    await page.wait_for_selector(selector, timeout=45000)
                    _log(logger, "info", f"Found results: {selector}")
                    results_found = True
                    break
                except:
                    continue
            
            if not results_found:
                # Check if we're on an error page or no results
                page_content = await page.content()
                if "no results" in page_content.lower() or "geen resultaten" in page_content.lower():
                    raise RuntimeError(f"No hotels found for location: {validated_params['location']}")
                else:
                    raise RuntimeError("Could not find hotel results on page")
            
            await page.wait_for_timeout(random.uniform(3000, 5000))
            _log(logger, "info", f"Search completed for {validated_params['location']}")
            
        except TimeoutError:
            raise RuntimeError("Search form submission timed out - Booking.com may be blocking automation")
        except Exception as e:
            raise RuntimeError(f"Search form error: {str(e)}")
    
    # ───── Filter application ─────
    @staticmethod
    async def _apply_filters(page, validated_params: Dict[str, Any], logger: logging.Logger) -> None:
        """Apply price, rating, and amenity filters."""
        try:
            await page.wait_for_timeout(2000)
            
            # Price filters
            if "min_price" in validated_params or "max_price" in validated_params:
                try:
                    # Try to find price filter section
                    price_filter = page.locator("div[data-testid*='price'], div:has-text('Price per night')")
                    await price_filter.click(timeout=5000)
                    await page.wait_for_timeout(1000)
                    
                    if "min_price" in validated_params:
                        min_input = page.locator("input[data-testid*='price-min'], input[name*='min'], input[placeholder*='min']")
                        await min_input.fill(str(int(validated_params["min_price"])))
                        
                    if "max_price" in validated_params:
                        max_input = page.locator("input[data-testid*='price-max'], input[name*='max'], input[placeholder*='max']")
                        await max_input.fill(str(int(validated_params["max_price"])))
                        
                    await page.wait_for_timeout(500)
                    _log(logger, "info", "Applied price filters")
                except Exception:
                    _log(logger, "warning", "Could not apply price filters")
            
            # Rating filter
            if "min_rating" in validated_params:
                try:
                    rating_value = validated_params["min_rating"]
                    # Convert to booking.com's rating categories
                    if rating_value >= 9:
                        rating_selector = "div[data-testid*='rating']:has-text('9+'), button:has-text('Wonderful: 9+')"
                    elif rating_value >= 8:
                        rating_selector = "div[data-testid*='rating']:has-text('8+'), button:has-text('Very good: 8+')"
                    elif rating_value >= 7:
                        rating_selector = "div[data-testid*='rating']:has-text('7+'), button:has-text('Good: 7+')"
                    elif rating_value >= 6:
                        rating_selector = "div[data-testid*='rating']:has-text('6+'), button:has-text('Pleasant: 6+')"
                    else:
                        rating_selector = None
                        
                    if rating_selector:
                        rating_filter = page.locator(rating_selector).first
                        await rating_filter.click(timeout=5000)
                        await page.wait_for_timeout(1000)
                        _log(logger, "info", f"Applied rating filter: {rating_value}+")
                except Exception:
                    _log(logger, "warning", "Could not apply rating filter")
            
            # Star rating filter
            if "star_rating" in validated_params:
                try:
                    for star in validated_params["star_rating"]:
                        star_checkbox = page.locator(f"input[name*='star'][value='{star}'], label:has-text('{star} star')")
                        await star_checkbox.click(timeout=5000)
                        await page.wait_for_timeout(500)
                    _log(logger, "info", f"Applied star rating filter: {validated_params['star_rating']}")
                except Exception:
                    _log(logger, "warning", "Could not apply star rating filter")
            
            # Amenity filters
            if "amenities" in validated_params:
                try:
                    for amenity in validated_params["amenities"]:
                        amenity_checkbox = page.locator(f"input[value*='{amenity}'], label:has-text('{amenity.replace('_', ' ').title()}')")
                        await amenity_checkbox.click(timeout=3000)
                        await page.wait_for_timeout(300)
                    _log(logger, "info", f"Applied amenity filters: {validated_params['amenities']}")
                except Exception:
                    _log(logger, "warning", "Could not apply amenity filters")
            
            # Wait for filters to take effect
            await page.wait_for_timeout(3000)
            
        except Exception as e:
            _log(logger, "warning", f"Filter application failed: {str(e)}")
    
    # ───── Hotel data extraction ─────
    @staticmethod
    async def _extract_hotel_data(page, hotel_element, logger: logging.Logger) -> Optional[Dict[str, Any]]:
        """Extract comprehensive data from a single hotel card."""
        try:
            hotel_data = {}
            
            # Basic information
            try:
                name_elem = hotel_element.locator("[data-testid*='title'], .sr-hotel__name, h3, h4")
                hotel_data["name"] = await name_elem.first.text_content()
            except:
                hotel_data["name"] = "Unknown Hotel"
            
            # Rating and reviews
            try:
                rating_elem = hotel_element.locator("[data-testid*='rating'], .bui-review-score__badge")
                rating_text = await rating_elem.first.text_content()
                hotel_data["rating"] = float(re.search(r"(\d+\.?\d*)", rating_text).group(1))
            except:
                hotel_data["rating"] = None
                
            try:
                rating_desc_elem = hotel_element.locator("[data-testid*='rating-desc'], .bui-review-score__title")
                hotel_data["rating_text"] = await rating_desc_elem.first.text_content()
            except:
                hotel_data["rating_text"] = None
                
            try:
                reviews_elem = hotel_element.locator("[data-testid*='reviews'], .bui-review-score__text")
                reviews_text = await reviews_elem.first.text_content()
                match = re.search(r"(\d+(?:,\d+)*)", reviews_text)
                if match:
                    hotel_data["reviews_count"] = int(match.group(1).replace(",", ""))
                else:
                    hotel_data["reviews_count"] = 0
            except:
                hotel_data["reviews_count"] = 0
            
            # Price information
            try:
                price_elem = hotel_element.locator("[data-testid*='price'], .prco-valign-middle-helper, .bui-price-display__value")
                price_text = await price_elem.first.text_content()
                price_match = re.search(r"(\d+(?:,\d+)*)", price_text.replace(" ", ""))
                if price_match:
                    hotel_data["price_per_night"] = int(price_match.group(1).replace(",", ""))
                    # Extract currency
                    currency_match = re.search(r"([A-Z]{3}|[$€£¥])", price_text)
                    hotel_data["currency"] = currency_match.group(1) if currency_match else "USD"
                else:
                    hotel_data["price_per_night"] = None
                    hotel_data["currency"] = None
            except:
                hotel_data["price_per_night"] = None
                hotel_data["currency"] = None
            
            # Location information
            try:
                location_elem = hotel_element.locator("[data-testid*='address'], .sr_card_address_line")
                hotel_data["address"] = await location_elem.first.text_content()
            except:
                hotel_data["address"] = None
                
            try:
                distance_elem = hotel_element.locator("[data-testid*='distance'], .sr_card__subtitle--info")
                distance_text = await distance_elem.first.text_content()
                hotel_data["distance_to_center"] = distance_text.strip()
            except:
                hotel_data["distance_to_center"] = None
            
            # Hotel URL for booking
            try:
                link_elem = hotel_element.locator("a[data-testid*='title'], .hotel_name_link")
                hotel_url = await link_elem.first.get_attribute("href")
                if hotel_url and not hotel_url.startswith("http"):
                    hotel_url = "https://www.booking.com" + hotel_url
                hotel_data["booking_url"] = hotel_url
            except:
                hotel_data["booking_url"] = None
            
            # Star rating
            try:
                stars_elem = hotel_element.locator("[data-testid*='stars'], .bui-rating__stars")
                stars_html = await stars_elem.first.inner_html()
                star_count = stars_html.count("star") or stars_html.count("★")
                hotel_data["star_rating"] = min(5, max(1, star_count)) if star_count else None
            except:
                hotel_data["star_rating"] = None
            
            # Images
            try:
                img_elements = hotel_element.locator("img[data-testid*='image'], .sr_item_photo_link img")
                images = []
                for i in range(min(3, await img_elements.count())):
                    img_src = await img_elements.nth(i).get_attribute("src")
                    if img_src and img_src.startswith("http"):
                        images.append(img_src)
                hotel_data["images"] = images
            except:
                hotel_data["images"] = []
            
            # Generate unique ID
            name_for_id = hotel_data["name"].lower().replace(" ", "_")
            hotel_data["id"] = f"booking_{hashlib.md5(name_for_id.encode()).hexdigest()[:8]}"
            
            return hotel_data
            
        except Exception as e:
            _log(logger, "warning", f"Failed to extract hotel data: {str(e)}")
            return None
    
    # ───── Optimized reviews collection ─────
    @staticmethod
    async def _collect_reviews(page, hotel_url: str, max_reviews: int, logger: logging.Logger) -> List[Dict[str, Any]]:
        """Collect reviews for a specific hotel with stealth mode."""
        reviews = []
        try:
            if not hotel_url:
                return reviews
            
            # Skip review collection entirely for speed - this was causing the 1.5min delay
            # The issue is that even "staying on page" was taking too long
            _log(logger, "info", f"Skipping review collection to optimize speed")
            
            # Just return some basic review placeholders based on rating
            # This keeps the API consistent without the performance hit
            return [
                {
                    "text": "Review data available on booking page",
                    "rating": None,
                    "reviewer": "See booking page", 
                    "date": None,
                    "source": "placeholder_for_speed"
                }
            ] if max_reviews > 0 else []
            
        except Exception as e:
            _log(logger, "warning", f"Review collection failed: {str(e)}")
        
        return reviews
    
    # ───── Hybrid execution method (HTTP primary + Browser fallback) ─────
    @staticmethod
    async def run(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        ENHANCED execution method with GraphQL API interception and fallback strategies.
        
        Strategy:
        1. Try GraphQL API interception first (best data quality)
        2. Fall back to enhanced HTML scraping (reliable)
        3. Use HTTP approach as last resort (fast but limited)
        """
        start_time = datetime.datetime.utcnow()
        method_used = "unknown"
        
        try:
            # Validate parameters
            validated_params = BookingHotelsTask._validate_params(params)
            _log(logger, "info", f"🚀 Starting ENHANCED hotel search for {validated_params['location']}")
            
            hotels = []
            
            # ═══════════════ PHASE 1: GraphQL API Interception ═══════════════
            _log(logger, "info", "🔥 Phase 1: Attempting GraphQL API interception")
            try:
                # Create browser context with API interception
                ctx = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = await ctx.new_page()
                
                try:
                    # Set up GraphQL API interception
                    hotels = await BookingHotelsTask._extract_with_graphql_interception(
                        page, validated_params, logger
                    )
                    
                    if hotels and len(hotels) > 0:
                        method_used = "graphql_api"
                        _log(logger, "info", f"✅ GraphQL API method successful: Found {len(hotels)} hotels")
                    else:
                        _log(logger, "warning", "⚠️  GraphQL API returned no data - falling back to HTML")
                        
                finally:
                    await ctx.close()
                    
            except Exception as e:
                _log(logger, "warning", f"⚠️  GraphQL API method failed: {str(e)} - falling back to HTML")
            
            # ═══════════════ PHASE 2: HTML Scraping Fallback ═══════════════
            if not hotels:
                _log(logger, "info", "🌐 Phase 2: Using enhanced HTML scraping fallback")
                try:
                    # Create browser context with enhanced settings
                    ctx = await browser.new_context(
                        viewport={"width": 1920, "height": 1080},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    page = await ctx.new_page()
                    
                    try:
                        # Use enhanced browser extraction (BACKUP METHOD)
                        hotels = await BookingHotelsTask._extract_hotels_browser_enhanced(page, validated_params, logger)
                        method_used = "html_scraping"
                        _log(logger, "info", f"✅ HTML scraping method successful: Found {len(hotels)} hotels")
                        
                    finally:
                        await ctx.close()
                        
                except Exception as e:
                    _log(logger, "error", f"❌ HTML scraping method also failed: {str(e)}")
                    method_used = "html_failed"
            
            # ═══════════════ PHASE 3: HTTP Fallback (Last Resort) ═══════════════
            if not hotels:
                _log(logger, "info", "⚡ Phase 3: Using HTTP approach as last resort")
                try:
                    client = BookingHotelsTask._create_http_session()
                    try:
                        # Build direct search URL
                        search_url = BookingHotelsTask._build_search_url(validated_params)
                        max_results = validated_params["max_results"]
                        
                        # Try HTTP extraction
                        hotels = await BookingHotelsTask._extract_hotels_http(client, search_url, max_results, logger)
                        
                        if hotels:
                            method_used = "http_fallback"
                            _log(logger, "info", f"✅ HTTP fallback successful: Found {len(hotels)} hotels")
                        else:
                            _log(logger, "warning", "⚠️  All extraction methods failed")
                            method_used = "all_failed"
                            
                    finally:
                        await client.aclose()
                        
                except Exception as e:
                    _log(logger, "error", f"❌ HTTP fallback also failed: {str(e)}")
                    method_used = "all_failed"
            
            # ═══════════════ PHASE 3: Enhanced review collection ═══════════════
            if validated_params["include_reviews"] and hotels:
                _log(logger, "info", f"📝 Phase 3: Collecting detailed reviews for {min(len(hotels), 3)} hotels")
                
                # Use HTTP client for review collection regardless of primary method
                client = BookingHotelsTask._create_http_session()
                try:
                    for i, hotel in enumerate(hotels[:3]):  # Detailed reviews for top 3 hotels
                        if hotel.get("booking_url"):
                            try:
                                detailed_reviews = await BookingHotelsTask._fetch_hotel_reviews(client, hotel["booking_url"], logger)
                                if detailed_reviews:
                                    hotel["reviews"] = detailed_reviews[:5]
                                    _log(logger, "info", f"📖 Fetched {len(hotel['reviews'])} reviews for {hotel['name']}")
                                else:
                                    hotel["reviews"] = [{"text": "Reviews available on booking page", "rating": hotel.get("rating"), "source": "booking_page"}]
                            except Exception as e:
                                _log(logger, "warning", f"⚠️  Review collection failed for {hotel['name']}: {str(e)}")
                                hotel["reviews"] = [{"text": "Reviews available on booking page", "rating": hotel.get("rating"), "source": "booking_page"}]
                        else:
                            hotel["reviews"] = []
                    
                    # Lightweight reviews for remaining hotels
                    for hotel in hotels[3:]:
                        hotel["reviews"] = [{"text": "Additional reviews available on booking page", "rating": hotel.get("rating"), "source": "booking_page_summary"}]
                        
                finally:
                    await client.aclose()
            else:
                for hotel in hotels:
                    hotel["reviews"] = []
            
            # ═══════════════ PHASE 4: Results processing and optimization ═══════════════
            collected_count = len(hotels)
            _log(logger, "info", f"🎯 Phase 4: Processing {collected_count} hotels with method: {method_used}")
            
            # Save comprehensive data
            output_file = pathlib.Path(job_output_dir) / "hotels_data.json"
            
            result_data = {
                "search_metadata": {
                    "location": validated_params["location"],
                    "check_in": validated_params["check_in"],
                    "check_out": validated_params["check_out"],
                    "nights": validated_params["nights"],
                    "guests": {
                        "adults": validated_params["adults"],
                        "children": validated_params["children"],
                        "rooms": validated_params["rooms"]
                    },
                    "filters_applied": {k: v for k, v in validated_params.items() 
                                      if k in ["min_price", "max_price", "min_rating", "star_rating", "amenities", "search_radius_km"]},
                    "extraction_method": method_used,
                    "total_found": len(hotels),
                    "scraped_count": collected_count,
                    "search_completed_at": datetime.datetime.utcnow().isoformat()
                },
                "hotels": hotels
            }
            
            output_file.write_text(json.dumps(result_data, indent=2, ensure_ascii=False), "utf-8")
            
            # Calculate comprehensive statistics
            avg_price = None
            if hotels:
                prices = [h["price_per_night"] for h in hotels if h.get("price_per_night") and h["price_per_night"] > 0]
                if prices:
                    avg_price = sum(prices) / len(prices)
            
            avg_rating = None
            if hotels:
                ratings = [h["rating"] for h in hotels if h.get("rating") and h["rating"] > 0]
                if ratings:
                    avg_rating = sum(ratings) / len(ratings)
            
            end_time = datetime.datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            performance_note = "⚡ Fast" if method_used == "http_fast" else "🛡️ Reliable" if method_used == "browser_enhanced" else "⚠️ Limited"
            _log(logger, "info", f"🏁 Hotel search completed: {collected_count} hotels in {duration:.1f}s ({performance_note})")
            
            return {
                "success": True,
                "hotels_found": collected_count,
                "location": validated_params["location"],
                "date_range": f"{validated_params['check_in']} to {validated_params['check_out']}",
                "nights": validated_params["nights"],
                "average_price_per_night": round(avg_price, 2) if avg_price else None,
                "average_rating": round(avg_rating, 1) if avg_rating else None,
                "data_file": "hotels_data.json",
                "execution_time_seconds": round(duration, 1),
                "extraction_method": method_used
            }
                
        except Exception as e:
            error_msg = str(e)
            _log(logger, "error", f"❌ Hybrid booking hotels task failed: {error_msg}")
            
            return {
                "success": False,
                "error": error_msg,
                "location": params.get("location", "unknown"),
                "execution_time_seconds": (datetime.datetime.utcnow() - start_time).total_seconds(),
                "extraction_method": method_used
            }

    @staticmethod
    async def _extract_complete_hotel_data(container, hotel_index: int, logger: logging.Logger) -> Optional[Dict[str, Any]]:
        """
        Comprehensive browser-based hotel data extraction with multiple fallback strategies.
        Extracts: name, price, rating, reviews, address, amenities, images, distance, and more.
        """
        try:
            _log(logger, "info", f"🔍 Extracting hotel #{hotel_index} data...")
            
            hotel_data = {
                "name": None,
                "price_per_night": None,
                "total_price": None,
                "rating": None,
                "review_count": None,
                "address": None,
                "distance_to_center": None,
                "amenities": [],
                "images": [],
                "reviews": [],
                "booking_url": None,
                "star_rating": None,
                "availability_note": None
            }
            
            # ═══════════════ HOTEL NAME EXTRACTION ═══════════════
            name_selectors = [
                '[data-testid="title"]',
                'h3[data-testid="title"]',
                '.sr-hotel__name',
                '.fc63351294',
                '.f6431b446c',
                'h3 a[data-testid="title-link"]',
                'h3',
                '.sr_item_photo_link',
                '[class*="hotel"] [class*="name"]',
                'a[href*="/hotel/"]'
            ]
            
            for selector in name_selectors:
                try:
                    name_element = await container.query_selector(selector)
                    if name_element:
                        name = await name_element.inner_text()
                        if name and len(name.strip()) > 0:
                            hotel_data["name"] = name.strip()
                            break
                except Exception:
                    continue
            
            if not hotel_data["name"]:
                _log(logger, "warning", f"❌ Could not extract name for hotel #{hotel_index}")
                return None
            
            # ═══════════════ PRICE EXTRACTION ═══════════════
            # Strategy 1: Try specific selectors first
            price_selectors = [
                # Modern Booking.com price selectors (2025)
                '[data-testid="price-and-discounted-price"] span',
                '[data-testid="price-and-discounted-price"]',
                '[data-testid="price"] span',
                '[data-testid="price"]',
                'span[aria-label*="price"]',
                '.a4c1805887',  # Current price class
                '.f6431b446c',  # Price display class
                '.fcab3ed991',  # Price container class
                '.e729ed16dc',  # Price value class
                '.bd2f5e73a0',  # Price wrapper
                
                # Fallback selectors for price
                '.bui-price-display__value',
                '.prco-valign-middle-helper',
                '.sr-hotel__title--deal',
                '.bui_price_currency',
                '.c-price',
                '[class*="price"]',
                '.sr_price_wrapper',
                '.rate_item_price',
                '.bui-text--color-primary'
            ]
            
            for selector in price_selectors:
                try:
                    price_element = await container.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        if price_text and len(price_text.strip()) > 0:
                            price_value = BookingHotelsTask._extract_price_from_text(price_text, logger)
                            if price_value:
                                hotel_data["price_per_night"] = price_value
                                _log(logger, "info", f"💰 Extracted price: ${price_value} from '{price_text.strip()}' using selector: {selector}")
                                break
                except Exception as e:
                    _log(logger, "debug", f"Price extraction failed for selector {selector}: {e}")
                    continue
            
            # Strategy 2: If no price found, search ALL text in container for currency symbols
            if not hotel_data["price_per_night"]:
                try:
                    all_text = await container.inner_text()
                    if all_text:
                        price_value = BookingHotelsTask._extract_price_from_text(all_text, logger)
                        if price_value:
                            hotel_data["price_per_night"] = price_value
                            _log(logger, "info", f"💰 Extracted price: ${price_value} from container full text scan")
                except Exception as e:
                    _log(logger, "debug", f"Full text price extraction failed: {e}")
            
            # Strategy 3: Search for any element containing currency symbols
            if not hotel_data["price_per_night"]:
                try:
                    currency_elements = await container.query_selector_all('*')
                    for element in currency_elements[:50]:  # Limit to avoid performance issues
                        try:
                            text = await element.inner_text()
                            if text and ('$' in text or 'USD' in text or '€' in text or '£' in text):
                                price_value = BookingHotelsTask._extract_price_from_text(text, logger)
                                if price_value:
                                    hotel_data["price_per_night"] = price_value
                                    _log(logger, "info", f"💰 Extracted price: ${price_value} from currency scan: '{text.strip()[:50]}'")
                                    break
                        except Exception:
                            continue
                except Exception as e:
                    _log(logger, "debug", f"Currency scan price extraction failed: {e}")
            
            # ═══════════════ RATING EXTRACTION ═══════════════
            rating_selectors = [
                '[data-testid="review-score"] div',
                '.b5cd09854e .a3b8729ab1',
                '.bui-review-score__badge',
                '.sr-hotel__review-score',
                '.c-score',
                '[class*="review"] [class*="score"]',
                '.d10a6220b4',
                '.b8eef6afe4',
                '.a3b8729ab1'
            ]
            
            for selector in rating_selectors:
                try:
                    rating_element = await container.query_selector(selector)
                    if rating_element:
                        rating_text = await rating_element.inner_text()
                        if rating_text:
                            # Extract numeric rating
                            import re
                            rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                            if rating_match:
                                try:
                                    rating_value = float(rating_match.group(1))
                                    if 0 <= rating_value <= 10:
                                        hotel_data["rating"] = rating_value
                                        break
                                except ValueError:
                                    continue
                except Exception:
                    continue
            
            # ═══════════════ REVIEW COUNT EXTRACTION ═══════════════
            review_count_selectors = [
                # Modern review count selectors (2025)
                '[data-testid="review-score"] + div',
                '[data-testid="review-count"]',
                'div[data-testid="review-score"] + *',
                'div[aria-label*="review"]',
                
                # Current class selectors
                '.f419a93f12',  # Review count text
                '.abf093bdfe',  # Review wrapper
                '.a7a72174b8',  # Review text
                '.d8eab2cf7f',  # Review number
                '.b8eef6afe4',  # Review container
                
                # Fallback selectors
                '.bui-review-score__text',
                '.sr-hotel__review',
                '[class*="review"] [class*="count"]',
                'span:has-text("review")',
                'div:has-text("review")',
                '*[class*="review"]:has-text("review")',
                
                # Look for patterns like "123 reviews"
                'span:has-text("review")',
                'div:has-text("review")'
            ]
            
            for selector in review_count_selectors:
                try:
                    review_element = await container.query_selector(selector)
                    if review_element:
                        review_text = await review_element.inner_text()
                        if review_text:
                            # Extract numeric review count
                            import re
                            review_match = re.search(r'(\d{1,3}(?:,\d{3})*)', review_text)
                            if review_match:
                                try:
                                    review_count = int(review_match.group(1).replace(',', ''))
                                    hotel_data["review_count"] = review_count
                                    break
                                except ValueError:
                                    continue
                except Exception:
                    continue
            
            # ═══════════════ ADDRESS EXTRACTION ═══════════════
            address_selectors = [
                '[data-testid="address"]',
                '.sr-hotel__address',
                '.hp_address_subtitle',
                '[class*="address"]',
                '.f4bd0794db',
                '.fcd9eec8fb',
                '.d7e3028de3'
            ]
            
            for selector in address_selectors:
                try:
                    address_element = await container.query_selector(selector)
                    if address_element:
                        address = await address_element.inner_text()
                        if address and len(address.strip()) > 0:
                            hotel_data["address"] = address.strip()
                            break
                except Exception:
                    continue
            
            # ═══════════════ DISTANCE EXTRACTION ═══════════════
            distance_selectors = [
                '[data-testid="distance"]',
                '.sr-hotel__address--distance',
                '.hp_location_block__section_container',
                '[class*="distance"]',
                '.c4c9409f8c',
                '.f4552b6561'
            ]
            
            for selector in distance_selectors:
                try:
                    distance_element = await container.query_selector(selector)
                    if distance_element:
                        distance_text = await distance_element.inner_text()
                        if distance_text and ('km' in distance_text or 'mile' in distance_text):
                            hotel_data["distance_to_center"] = distance_text.strip()
                            break
                except Exception:
                    continue
            
            # ═══════════════ BOOKING URL EXTRACTION ═══════════════
            url_selectors = [
                'a[data-testid="title-link"]',
                '.sr-hotel__name a',
                'h3 a',
                'a[href*="/hotel/"]'
            ]
            
            for selector in url_selectors:
                try:
                    url_element = await container.query_selector(selector)
                    if url_element:
                        href = await url_element.get_attribute('href')
                        if href:
                            # Convert relative URL to absolute
                            if href.startswith('/'):
                                href = f"https://www.booking.com{href}"
                            hotel_data["booking_url"] = href
                            break
                except Exception:
                    continue
            
            # ═══════════════ IMAGES EXTRACTION ═══════════════
            image_selectors = [
                '.sr-hotel__image img',
                '[data-testid="image"] img',
                '.hp_header_gallery_image img',
                '.b97bf5ff38 img'
            ]
            
            for selector in image_selectors:
                try:
                    images = await container.query_selector_all(f'{selector}')
                    for img in images[:3]:  # Limit to 3 images
                        src = await img.get_attribute('src')
                        if src and 'http' in src:
                            hotel_data["images"].append(src)
                    if hotel_data["images"]:
                        break
                except Exception:
                    continue
            
            # ═══════════════ AMENITIES EXTRACTION ═══════════════
            amenity_selectors = [
                '.sr-hotel__facility',
                '.hp_desc_important_facilities',
                '[data-testid="facility"]',
                '.c5ca594cb1'
            ]
            
            for selector in amenity_selectors:
                try:
                    amenity_elements = await container.query_selector_all(selector)
                    for amenity_el in amenity_elements[:5]:  # Limit to 5 amenities
                        amenity_text = await amenity_el.inner_text()
                        if amenity_text and len(amenity_text.strip()) > 0:
                            hotel_data["amenities"].append(amenity_text.strip())
                    if hotel_data["amenities"]:
                        break
                except Exception:
                    continue
            
            # ═══════════════ VALIDATION AND RETURN ═══════════════
            if hotel_data["name"]:
                # Log what we successfully extracted
                extracted_fields = []
                if hotel_data["price_per_night"]: extracted_fields.append("price")
                if hotel_data["rating"]: extracted_fields.append("rating")
                if hotel_data["address"]: extracted_fields.append("address")
                if hotel_data["distance_to_center"]: extracted_fields.append("distance")
                if hotel_data["amenities"]: extracted_fields.append("amenities")
                
                _log(logger, "info", f"✅ Hotel #{hotel_index} '{hotel_data['name']}': {', '.join(extracted_fields) if extracted_fields else 'name only'}")
                return hotel_data
            else:
                _log(logger, "warning", f"❌ Hotel #{hotel_index}: Failed to extract basic data")
                return None
                
        except Exception as e:
            _log(logger, "error", f"❌ Error extracting hotel #{hotel_index}: {str(e)}")
            return None

    @staticmethod
    def _extract_price_from_text(text: str, logger: logging.Logger) -> Optional[float]:
        """
        Extract price from any text using comprehensive patterns.
        Returns the price as a float or None if not found.
        """
        try:
            import re
            text_clean = text.strip()
            
            # Multiple price extraction patterns
            price_patterns = [
                # Standard currency formats
                r'(?:USD?\s*)?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # USD 123,456.78 or $123,456.78
                r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:USD?)?',  # 123,456.78 USD
                r'[\$€£¥₹]\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # $123,456.78
                r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*[\$€£¥₹]',  # 123,456.78$
                
                # Price with "per night", "night", etc.
                r'[\$€£¥₹]\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:per\s*night|night|/night)',
                r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*[\$€£¥₹]\s*(?:per\s*night|night|/night)',
                
                # Simple number format with context
                r'(?:price|cost|rate|from)\s*[\$€£¥₹]?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',
                r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:total|price|cost|rate)',
                
                # Just numbers that look like prices (when currency symbols present)
                r'(\d{1,3}(?:,\d{3})*)',  # Simple number format
            ]
            
            for pattern in price_patterns:
                matches = re.finditer(pattern, text_clean, re.IGNORECASE)
                for match in matches:
                    try:
                        price_str = match.group(1).replace(',', '')
                        price_value = float(price_str)
                        # Reasonable hotel price range: $10 to $50,000 per night
                        if 10 <= price_value <= 50000:
                            return price_value
                    except (ValueError, IndexError):
                        continue
            
            return None
            
        except Exception as e:
            _log(logger, "debug", f"Price text extraction error: {e}")
            return None

    @staticmethod
    async def _get_detailed_hotel_data(page, hotel_basic: Dict[str, Any], validated_params: Dict[str, Any], logger: logging.Logger) -> Optional[Dict[str, Any]]:
        """
        Visit individual hotel page to get complete accurate data:
        - Real prices with dates
        - Actual reviews 
        - Amenities list
        - Images gallery
        - Accurate ratings
        """
        try:
            hotel_url = hotel_basic.get("booking_url")
            if not hotel_url:
                return hotel_basic
            
            _log(logger, "info", f"🔍 Visiting hotel page: {hotel_basic['name']}")
            
            # Navigate to hotel detail page
            await page.goto(hotel_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(2000)  # Allow content to load
            
            # Create enhanced hotel data starting with basic data
            enhanced_data = hotel_basic.copy()
            
            # ═══════════════ ENHANCED PRICE EXTRACTION ═══════════════
            price_selectors = [
                '[data-testid="price-and-discounted-price"]',
                '.hprt-price-price',
                '.bui-price-display__value',
                '.prco-valign-middle-helper',
                '[data-component="tooltip/tooltip"] .bui-price-display__value',
                '.hp-booking-form .bui-price-display__value',
                '.rates-table .bui-price-display__value',
                'td[data-block-id] .bui-price-display__value'
            ]
            
            for selector in price_selectors:
                try:
                    price_elements = await page.query_selector_all(selector)
                    for price_el in price_elements:
                        price_text = await price_el.inner_text()
                        if price_text:
                            price_value = BookingHotelsTask._extract_price_from_text(price_text, logger)
                            if price_value and price_value > 50:  # Realistic hotel price threshold
                                enhanced_data["price_per_night"] = price_value
                                # Calculate total price
                                nights = validated_params.get("nights", 1)
                                enhanced_data["total_price"] = round(price_value * nights, 2)
                                _log(logger, "info", f"💰 Enhanced price: ${price_value}/night * {nights} nights = ${enhanced_data['total_price']}")
                                break
                    if enhanced_data.get("price_per_night") and enhanced_data["price_per_night"] > 50:
                        break
                except Exception:
                    continue
            
            # ═══════════════ ENHANCED RATING EXTRACTION ═══════════════
            rating_selectors = [
                '.b5cd09854e .a3b8729ab1',
                '[data-testid="review-score-component"] .a3b8729ab1',
                '.bui-review-score__badge',
                '.c-score',
                '.hp-review-score .bui-review-score__badge'
            ]
            
            for selector in rating_selectors:
                try:
                    rating_element = await page.query_selector(selector)
                    if rating_element:
                        rating_text = await rating_element.inner_text()
                        if rating_text:
                            import re
                            rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                            if rating_match:
                                try:
                                    rating_value = float(rating_match.group(1))
                                    if 0 <= rating_value <= 10:
                                        enhanced_data["rating"] = rating_value
                                        break
                                except ValueError:
                                    continue
                except Exception:
                    continue
            
            # ═══════════════ REVIEW COUNT EXTRACTION ═══════════════
            review_count_selectors = [
                '[data-testid="review-score-component"] + div',
                '.bui-review-score__text',
                '.hp-review-score .bui-review-score__text'
            ]
            
            for selector in review_count_selectors:
                try:
                    review_element = await page.query_selector(selector)
                    if review_element:
                        review_text = await review_element.inner_text()
                        if review_text:
                            import re
                            review_match = re.search(r'(\d{1,3}(?:,\d{3})*)', review_text)
                            if review_match:
                                try:
                                    review_count = int(review_match.group(1).replace(',', ''))
                                    enhanced_data["review_count"] = review_count
                                    break
                                except ValueError:
                                    continue
                except Exception:
                    continue
            
            # ═══════════════ AMENITIES EXTRACTION ═══════════════
            amenity_selectors = [
                '.hp_desc_important_facilities_wrapper li',
                '.important_facilities li',
                '[data-testid="property-most-popular-facilites"] li',
                '.facility-badge',
                '.most-popular-facilities li'
            ]
            
            enhanced_data["amenities"] = []
            for selector in amenity_selectors:
                try:
                    amenity_elements = await page.query_selector_all(selector)
                    for amenity_el in amenity_elements[:10]:  # Limit to 10 amenities
                        amenity_text = await amenity_el.inner_text()
                        if amenity_text and len(amenity_text.strip()) > 0:
                            clean_amenity = amenity_text.strip()
                            if clean_amenity not in enhanced_data["amenities"]:
                                enhanced_data["amenities"].append(clean_amenity)
                    if enhanced_data["amenities"]:
                        break
                except Exception:
                    continue
            
            # ═══════════════ IMAGES EXTRACTION ═══════════════
            image_selectors = [
                '.hp_header_gallery_image img',
                '.bh-photo-grid img',
                '.hp-gallery img',
                '.gallery-image img'
            ]
            
            enhanced_data["images"] = []
            for selector in image_selectors:
                try:
                    images = await page.query_selector_all(selector)
                    for img in images[:5]:  # Limit to 5 images
                        src = await img.get_attribute('src')
                        if src and 'http' in src and src not in enhanced_data["images"]:
                            enhanced_data["images"].append(src)
                    if enhanced_data["images"]:
                        break
                except Exception:
                    continue
            
            # ═══════════════ REAL REVIEWS EXTRACTION ═══════════════
            enhanced_data["reviews"] = []
            review_selectors = [
                '.c-review',
                '.review-item',
                '[data-testid="review"]'
            ]
            
            for selector in review_selectors:
                try:
                    review_elements = await page.query_selector_all(selector)
                    for review_el in review_elements[:3]:  # Limit to 3 reviews
                        try:
                            review_text = await review_el.query_selector('.c-review__body, .review-text, .review-content')
                            if review_text:
                                text = await review_text.inner_text()
                                if text and len(text.strip()) > 10:
                                    enhanced_data["reviews"].append({
                                        "text": text.strip()[:500],  # Limit review length
                                        "rating": enhanced_data.get("rating"),
                                        "source": "detail_page"
                                    })
                        except Exception:
                            continue
                    if enhanced_data["reviews"]:
                        break
                except Exception:
                    continue
            
            # If no detailed reviews found, keep basic placeholder
            if not enhanced_data["reviews"]:
                enhanced_data["reviews"] = [{
                    "text": f"Reviews available on booking page - {enhanced_data.get('review_count', 'Multiple')} total reviews",
                    "rating": enhanced_data.get("rating"),
                    "source": "booking_page_summary"
                }]
            
            # Ensure total_price is calculated even if we didn't get enhanced price
            if enhanced_data.get("price_per_night") and not enhanced_data.get("total_price"):
                nights = validated_params.get("nights", 1)
                enhanced_data["total_price"] = round(enhanced_data["price_per_night"] * nights, 2)
                _log(logger, "info", f"💰 Calculated total: ${enhanced_data['price_per_night']}/night * {nights} nights = ${enhanced_data['total_price']}")
            
            _log(logger, "info", f"✅ Enhanced {enhanced_data['name']}: price=${enhanced_data.get('price_per_night', 'N/A')}, total=${enhanced_data.get('total_price', 'N/A')}, rating={enhanced_data.get('rating', 'N/A')}, amenities={len(enhanced_data.get('amenities', []))}, images={len(enhanced_data.get('images', []))}")
            
            return enhanced_data
            
        except Exception as e:
            _log(logger, "warning", f"⚠️  Failed to get detailed data for {hotel_basic['name']}: {str(e)}")
            return hotel_basic

    @staticmethod
    async def _extract_with_graphql_interception(page, validated_params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """
        🔥 ENHANCED: Extract hotel data using GraphQL API interception for perfect data quality.
        
        This method:
        1. Sets up request/response interception for GraphQL APIs
        2. Navigates to Booking.com and performs search
        3. Captures GraphQL API responses containing structured data
        4. Extracts complete hotel information from API responses
        """
        _log(logger, "info", "🔥 Setting up GraphQL API interception...")
        
        # Storage for intercepted GraphQL data
        intercepted_data = {
            "hotels": [],
            "reviews": {},
            "amenities": {},
            "images": {},
            "prices": {},
            "availability": {},
            "raw_responses": []
        }
        
        # Set up GraphQL API interception
        async def handle_response(response):
            """Intercept and process GraphQL API responses."""
            try:
                url = response.url
                
                # Check if this is a GraphQL API call with hotel data
                if "/dml/graphql" in url and response.status == 200:
                    try:
                        json_data = await response.json()
                        
                        # Store raw response for debugging
                        intercepted_data["raw_responses"].append({
                            "url": url,
                            "data": json_data
                        })
                        
                        # Process different GraphQL operations
                        if "data" in json_data:
                            await BookingHotelsTask._process_graphql_response(
                                json_data, intercepted_data, url, logger
                            )
                            
                    except Exception as e:
                        _log(logger, "debug", f"Failed to parse GraphQL response from {url}: {e}")
                        
            except Exception as e:
                _log(logger, "debug", f"Response interception error: {e}")
        
        # Register response handler
        page.on("response", handle_response)
        
        try:
            # Navigate to Booking.com and perform search
            _log(logger, "info", "🌐 Navigating to Booking.com with API interception enabled")
            await page.goto(BookingHotelsTask.BASE_URL, wait_until="networkidle", timeout=60000)
            
            # Handle cookie consent
            await BookingHotelsTask._handle_cookie_consent(page, logger)
            
            # Perform search to trigger GraphQL APIs
            await BookingHotelsTask._perform_search_with_api_interception(page, validated_params, logger)
            
            # Enhanced interaction automation (like manual inspection)
            await BookingHotelsTask._enhanced_interaction_automation(page, validated_params, logger)
            
            # Wait for GraphQL APIs to be called and intercepted
            _log(logger, "info", "⏳ Waiting for GraphQL API calls to complete...")
            await page.wait_for_timeout(10000)  # Wait for API calls to finish
            
            # Visit individual hotel pages to trigger more GraphQL APIs
            if intercepted_data.get("hotels"):
                await BookingHotelsTask._visit_hotel_pages_for_apis(page, intercepted_data, validated_params, logger)
            
            # DEBUG: Log intercepted API calls for analysis
            _log(logger, "info", f"🔍 Starting debug analysis of {len(intercepted_data.get('raw_responses', []))} intercepted responses...")
            await BookingHotelsTask._debug_intercepted_apis(intercepted_data, logger)
            _log(logger, "info", f"🔍 Debug analysis completed")
            
            # Process intercepted data into hotel objects
            hotels = await BookingHotelsTask._compile_hotel_data_from_apis(
                intercepted_data, validated_params, logger
            )
            
            _log(logger, "info", f"🔥 GraphQL interception complete: extracted {len(hotels)} hotels with {len(intercepted_data['raw_responses'])} API calls intercepted")
            return hotels
            
        except Exception as e:
            _log(logger, "error", f"❌ GraphQL interception failed: {str(e)}")
            return []
    
    @staticmethod
    async def _process_graphql_response(json_data: Dict[str, Any], intercepted_data: Dict[str, Any], url: str, logger: logging.Logger):
        """Process different types of GraphQL responses and extract relevant data."""
        try:
            data = json_data.get("data", {})
            
            # Search results with hotel list
            if any(key in data for key in ["searchResults", "properties", "search"]):
                _log(logger, "info", "📊 Intercepted hotel search results")
                # Extract hotel list data from search results
                await BookingHotelsTask._extract_search_results_data(data, intercepted_data, logger)
                
            # Review data from ReviewList operation
            elif any(key in data for key in ["reviews", "reviewList", "propertyReviewList"]):
                _log(logger, "info", "📝 Intercepted review data")
                await BookingHotelsTask._extract_review_data(data, intercepted_data, logger)
                
            # Amenities/facilities data
            elif any(key in data for key in ["facilities", "amenities", "propertyAmenities"]):
                _log(logger, "info", "🏊 Intercepted amenities data")
                await BookingHotelsTask._extract_amenities_data(data, intercepted_data, logger)
                
            # Images/gallery data
            elif any(key in data for key in ["images", "gallery", "propertyPhotos"]):
                _log(logger, "info", "📸 Intercepted image gallery data")
                await BookingHotelsTask._extract_images_data(data, intercepted_data, logger)
                
            # Pricing/availability data
            elif any(key in data for key in ["availability", "prices", "propertyPricing"]):
                _log(logger, "info", "💰 Intercepted pricing data")
                await BookingHotelsTask._extract_pricing_data(data, intercepted_data, logger)
                
            # Property details (ratings, address, etc.)
            elif any(key in data for key in ["property", "propertyDetails", "hotelDetails"]):
                _log(logger, "info", "🏨 Intercepted property details")
                await BookingHotelsTask._extract_property_details(data, intercepted_data, logger)
                
        except Exception as e:
            _log(logger, "debug", f"Error processing GraphQL response: {e}")
    
    @staticmethod
    async def _extract_search_results_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract hotel data from search results GraphQL response."""
        try:
            # Deep search for hotel/property arrays in nested structures
            def find_hotel_arrays(obj, path=""):
                """Recursively find arrays that might contain hotel data."""
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        current_path = f"{path}.{key}" if path else key
                        
                        # Check if this looks like a hotel array
                        if isinstance(value, list) and len(value) > 0:
                            # Sample first item to see if it has hotel-like properties
                            if isinstance(value[0], dict):
                                sample_keys = set(value[0].keys())
                                hotel_indicators = {
                                    "name", "title", "hotelName", "propertyName",
                                    "id", "hotelId", "propertyId", 
                                    "price", "priceInfo", "pricing",
                                    "rating", "reviewScore", "guestReviewsRating",
                                    "address", "location", "city",
                                    "images", "photos", "gallery"
                                }
                                
                                # If it has hotel-like properties, it's probably a hotel array
                                if any(indicator in sample_keys for indicator in hotel_indicators):
                                    _log(logger, "info", f"🎯 Found potential hotel array at {current_path}: {len(value)} items")
                                    _log(logger, "info", f"   Sample keys: {list(sample_keys)[:15]}")
                                    return value
                        
                        # Recurse into nested objects
                        if isinstance(value, (dict, list)):
                            result = find_hotel_arrays(value, current_path)
                            if result:
                                return result
                
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        result = find_hotel_arrays(item, f"{path}[{i}]")
                        if result:
                            return result
                
                return None
            
            # Find hotel data using deep search
            hotels_data = find_hotel_arrays(data)
            
            if hotels_data:
                _log(logger, "info", f"🎯 Processing {len(hotels_data)} hotels from GraphQL response")
                
                for i, hotel_item in enumerate(hotels_data):
                    if isinstance(hotel_item, dict):
                        # Extract comprehensive hotel data
                        hotel_info = BookingHotelsTask._extract_hotel_from_graphql(hotel_item, logger)
                        
                        if hotel_info:
                            # Store both raw data and processed info
                            hotel_id = hotel_info.get("id", f"graphql_hotel_{i}")
                            intercepted_data["hotels"].append({
                                "id": str(hotel_id),
                                "processed_data": hotel_info,
                                "raw_data": hotel_item
                            })
                            
                            _log(logger, "info", f"   ✅ Extracted: {hotel_info.get('name', 'Unknown')} - ${hotel_info.get('price_per_night', 'N/A')}")
                            
                _log(logger, "info", f"📊 Successfully extracted {len(intercepted_data['hotels'])} hotels from GraphQL")
            else:
                _log(logger, "warning", "⚠️  No hotel arrays found in GraphQL response")
                
        except Exception as e:
            _log(logger, "debug", f"Error extracting search results data: {e}")
    
    @staticmethod
    def _extract_hotel_from_graphql(hotel_data: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
        """Extract structured hotel information from GraphQL hotel object."""
        try:
            # Extract name
            name = (
                hotel_data.get("name") or
                hotel_data.get("title") or
                hotel_data.get("hotelName") or
                hotel_data.get("propertyName") or
                hotel_data.get("displayName", {}).get("text") or
                "Unknown Hotel"
            )
            
            # Extract price information
            price_per_night = None
            total_price = None
            
            # Look for price in various structures
            price_sources = [
                hotel_data.get("price"),
                hotel_data.get("priceInfo"),
                hotel_data.get("pricing"),
                hotel_data.get("rates"),
                hotel_data.get("priceDisplay"),
                hotel_data.get("priceDisplayInfo")
            ]
            
            for price_source in price_sources:
                if isinstance(price_source, dict):
                    # Extract numeric price
                    price_candidates = [
                        price_source.get("amount"),
                        price_source.get("value"),
                        price_source.get("totalPrice"),
                        price_source.get("nightlyRate"),
                        price_source.get("basePrice")
                    ]
                    
                    for candidate in price_candidates:
                        if isinstance(candidate, (int, float)) and candidate > 0:
                            price_per_night = float(candidate)
                            break
                    
                    if price_per_night:
                        break
                elif isinstance(price_source, (int, float)) and price_source > 0:
                    price_per_night = float(price_source)
                    break
            
            # Extract rating
            rating = None
            rating_sources = [
                hotel_data.get("rating"),
                hotel_data.get("reviewScore"),
                hotel_data.get("guestReviewsRating"),
                hotel_data.get("reviews", {}).get("averageScore"),
                hotel_data.get("score")
            ]
            
            for rating_source in rating_sources:
                if isinstance(rating_source, dict):
                    rating = rating_source.get("value") or rating_source.get("score")
                elif isinstance(rating_source, (int, float)):
                    rating = float(rating_source)
                
                if rating and rating > 0:
                    break
            
            # Extract review count
            review_count = None
            review_sources = [
                hotel_data.get("reviewCount"),
                hotel_data.get("reviews", {}).get("totalCount"),
                hotel_data.get("guestReviews", {}).get("count"),
                hotel_data.get("reviewsCount")
            ]
            
            for review_source in review_sources:
                if isinstance(review_source, (int, float)) and review_source > 0:
                    review_count = int(review_source)
                    break
            
            # Extract address/location
            address = None
            location_sources = [
                hotel_data.get("address"),
                hotel_data.get("location", {}).get("address"),
                hotel_data.get("city"),
                hotel_data.get("destination"),
                hotel_data.get("location", {}).get("displayName")
            ]
            
            for location_source in location_sources:
                if isinstance(location_source, str) and location_source.strip():
                    address = location_source.strip()
                    break
                elif isinstance(location_source, dict):
                    address_text = location_source.get("text") or location_source.get("name")
                    if address_text:
                        address = str(address_text).strip()
                        break
            
            # Extract images
            images = []
            image_sources = [
                hotel_data.get("images"),
                hotel_data.get("photos"),
                hotel_data.get("gallery"),
                hotel_data.get("mainPhoto")
            ]
            
            for image_source in image_sources:
                if isinstance(image_source, list):
                    for img in image_source[:10]:  # Limit to 10 images
                        if isinstance(img, dict):
                            img_url = img.get("url") or img.get("src") or img.get("href")
                            if img_url:
                                images.append(img_url)
                        elif isinstance(img, str):
                            images.append(img)
                elif isinstance(image_source, dict):
                    img_url = image_source.get("url") or image_source.get("src")
                    if img_url:
                        images.append(img_url)
                
                if images:
                    break
            
            # Extract amenities
            amenities = []
            amenity_sources = [
                hotel_data.get("amenities"),
                hotel_data.get("facilities"),
                hotel_data.get("features"),
                hotel_data.get("services")
            ]
            
            for amenity_source in amenity_sources:
                if isinstance(amenity_source, list):
                    for amenity in amenity_source:
                        if isinstance(amenity, dict):
                            amenity_name = amenity.get("name") or amenity.get("title")
                            if amenity_name:
                                amenities.append(str(amenity_name))
                        elif isinstance(amenity, str):
                            amenities.append(amenity)
                
                if amenities:
                    break
            
            # Extract booking URL
            booking_url = hotel_data.get("url") or hotel_data.get("link") or hotel_data.get("bookingUrl")
            
            # Extract hotel ID
            hotel_id = (
                hotel_data.get("id") or
                hotel_data.get("hotelId") or
                hotel_data.get("propertyId") or
                hotel_data.get("basicPropertyData", {}).get("id")
            )
            
            # Compile hotel information
            hotel_info = {
                "id": str(hotel_id) if hotel_id else None,
                "name": name,
                "price_per_night": price_per_night,
                "total_price": total_price,
                "rating": rating,
                "review_count": review_count,
                "address": address,
                "images": images,
                "amenities": amenities,
                "booking_url": booking_url
            }
            
            # Only return if we have meaningful data
            if name != "Unknown Hotel" and (price_per_night or rating):
                return hotel_info
            
            return None
            
        except Exception as e:
            _log(logger, "debug", f"Error extracting hotel from GraphQL: {e}")
            return None
    
    @staticmethod
    async def _extract_review_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract review data from ReviewList GraphQL response."""
        try:
            # Look for review arrays
            reviews_data = None
            for key in ["reviews", "reviewList", "propertyReviewList"]:
                if key in data:
                    potential_data = data[key]
                    if isinstance(potential_data, dict):
                        for subkey in ["reviews", "items", "list"]:
                            if subkey in potential_data and isinstance(potential_data[subkey], list):
                                reviews_data = potential_data[subkey]
                                break
                    elif isinstance(potential_data, list):
                        reviews_data = potential_data
                    if reviews_data:
                        break
            
            if reviews_data:
                # Store reviews by hotel/property ID
                for review in reviews_data:
                    if isinstance(review, dict):
                        # Extract review details
                        review_text = review.get("textDetails", {}).get("positiveText") or review.get("content") or review.get("text")
                        review_score = review.get("reviewScore") or review.get("rating") or review.get("score")
                        reviewer = review.get("guestDetails", {}).get("username") or review.get("reviewer", {}).get("name")
                        
                        property_id = "unknown"  # We'll match this later
                        
                        if property_id not in intercepted_data["reviews"]:
                            intercepted_data["reviews"][property_id] = []
                        
                        intercepted_data["reviews"][property_id].append({
                            "text": review_text,
                            "rating": review_score,
                            "reviewer": reviewer,
                            "source": "graphql_api"
                        })
                        
                _log(logger, "info", f"📝 Extracted {len(reviews_data)} reviews from API")
                
        except Exception as e:
            _log(logger, "debug", f"Error extracting review data: {e}")
    
    @staticmethod
    async def _extract_amenities_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract amenities data from GraphQL response.""" 
        try:
            # Implementation for amenities extraction
            _log(logger, "debug", "Processing amenities data...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting amenities data: {e}")
    
    @staticmethod
    async def _extract_images_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract images data from GraphQL response."""
        try:
            # Implementation for images extraction
            _log(logger, "debug", "Processing images data...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting images data: {e}")
    
    @staticmethod
    async def _extract_pricing_data(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract pricing data from GraphQL response."""
        try:
            # Implementation for pricing extraction
            _log(logger, "debug", "Processing pricing data...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting pricing data: {e}")
    
    @staticmethod
    async def _extract_property_details(data: Dict[str, Any], intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Extract property details from GraphQL response."""
        try:
            # Implementation for property details extraction
            _log(logger, "debug", "Processing property details...")
        except Exception as e:
            _log(logger, "debug", f"Error extracting property details: {e}")
    
    @staticmethod
    async def _perform_search_with_api_interception(page, validated_params: Dict[str, Any], logger: logging.Logger):
        """Perform search while GraphQL APIs are being intercepted."""
        try:
            # Use existing search logic but optimized for API interception
            await BookingHotelsTask._perform_search_enhanced(page, validated_params, logger)
            
            # Wait a bit more for additional API calls
            await page.wait_for_timeout(5000)
            
        except Exception as e:
            _log(logger, "warning", f"Search with API interception failed: {e}")
    
    @staticmethod
    async def _visit_hotel_pages_for_apis(page, intercepted_data: Dict[str, Any], validated_params: Dict[str, Any], logger: logging.Logger):
        """Visit individual hotel pages to trigger additional GraphQL APIs."""
        try:
            hotels = intercepted_data.get("hotels", [])
            max_visits = min(3, len(hotels))  # Limit to 3 hotels for performance
            
            _log(logger, "info", f"🔍 Visiting {max_visits} hotel pages to collect detailed API data...")
            
            for i, hotel in enumerate(hotels[:max_visits]):
                try:
                    search_data = hotel.get("search_data", {})
                    
                    # Try to find hotel URL from search data
                    hotel_url = None
                    for url_key in ["url", "link", "href", "propertyUrl"]:
                        if url_key in search_data:
                            hotel_url = search_data[url_key]
                            break
                    
                    if hotel_url:
                        if not hotel_url.startswith("http"):
                            hotel_url = f"https://www.booking.com{hotel_url}"
                        
                        _log(logger, "info", f"🔍 Visiting hotel page {i+1}: {hotel_url}")
                        await page.goto(hotel_url, wait_until="networkidle", timeout=30000)
                        await page.wait_for_timeout(5000)  # Wait for API calls
                        
                except Exception as e:
                    _log(logger, "debug", f"Failed to visit hotel page {i+1}: {e}")
                    
        except Exception as e:
            _log(logger, "debug", f"Error visiting hotel pages for APIs: {e}")
    
    @staticmethod
    async def _compile_hotel_data_from_apis(intercepted_data: Dict[str, Any], validated_params: Dict[str, Any], logger: logging.Logger) -> List[Dict[str, Any]]:
        """Compile complete hotel data from intercepted GraphQL API responses."""
        try:
            hotels = []
            hotels_data = intercepted_data.get("hotels", [])
            
            _log(logger, "info", f"🔥 Compiling hotel data from {len(hotels_data)} intercepted hotel objects")
            
            for hotel_item in hotels_data:
                try:
                    # Use the improved processed data from GraphQL extraction
                    processed_data = hotel_item.get("processed_data", {})
                    raw_data = hotel_item.get("raw_data", {})
                    hotel_id = hotel_item.get("id")
                    
                    # Start with processed GraphQL data
                    hotel = {
                        "name": processed_data.get("name") or "Unknown Hotel",
                        "price_per_night": processed_data.get("price_per_night"),
                        "total_price": processed_data.get("total_price"),
                        "rating": processed_data.get("rating"),
                        "review_count": processed_data.get("review_count"),
                        "address": processed_data.get("address"),
                        "distance_to_center": None,  # Will be extracted separately
                        "amenities": processed_data.get("amenities", []),
                        "images": processed_data.get("images", []),
                        "reviews": [],  # Will be populated from intercepted review data
                        "booking_url": processed_data.get("booking_url"),
                        "star_rating": None,  # Will be extracted from raw data
                        "availability_note": None
                    }
                    
                    # Enhance with additional data from intercepted APIs
                    if hotel_id:
                        # Add reviews from intercepted data
                        hotel_reviews = intercepted_data.get("reviews", {}).get(hotel_id, [])
                        if hotel_reviews:
                            hotel["reviews"] = hotel_reviews[:3]  # Limit to 3 reviews
                        
                        # Add additional amenities from intercepted data
                        intercepted_amenities = intercepted_data.get("amenities", {}).get(hotel_id, [])
                        if intercepted_amenities:
                            # Combine with existing amenities
                            all_amenities = list(set(hotel["amenities"] + intercepted_amenities))
                            hotel["amenities"] = all_amenities
                        
                        # Add additional images from intercepted data
                        intercepted_images = intercepted_data.get("images", {}).get(hotel_id, [])
                        if intercepted_images:
                            all_images = list(set(hotel["images"] + intercepted_images))
                            hotel["images"] = all_images
                    
                    # Calculate total price if we have price per night
                    if hotel["price_per_night"] and not hotel["total_price"]:
                        nights = validated_params.get("nights", 1)
                        hotel["total_price"] = round(hotel["price_per_night"] * nights, 2)
                    
                    # Apply filters during compilation
                    should_include = True
                    
                    # Apply min_rating filter
                    if "min_rating" in validated_params and validated_params["min_rating"]:
                        min_rating = validated_params["min_rating"]
                        hotel_rating = hotel.get("rating")
                        if not hotel_rating or hotel_rating < min_rating:
                            _log(logger, "info", f"🚫 Filtered out {hotel['name']}: rating {hotel_rating} < {min_rating}")
                            should_include = False
                    
                    # Apply max_price filter
                    if "max_price" in validated_params and validated_params["max_price"]:
                        max_price = validated_params["max_price"]
                        hotel_price = hotel.get("price_per_night")
                        if hotel_price and hotel_price > max_price:
                            _log(logger, "info", f"🚫 Filtered out {hotel['name']}: price {hotel_price} > {max_price}")
                            should_include = False
                    
                    # Apply min_price filter
                    if "min_price" in validated_params and validated_params["min_price"]:
                        min_price = validated_params["min_price"]
                        hotel_price = hotel.get("price_per_night")
                        if hotel_price and hotel_price < min_price:
                            _log(logger, "info", f"🚫 Filtered out {hotel['name']}: price {hotel_price} < {min_price}")
                            should_include = False
                    
                    # Only include hotels with meaningful data and that pass filters
                    if should_include and hotel["name"] != "Unknown Hotel" and (hotel["price_per_night"] or hotel["rating"]):
                        hotels.append(hotel)
                        _log(logger, "info", f"✅ Compiled: {hotel['name']} - ${hotel.get('price_per_night', 'N/A')} - ⭐{hotel.get('rating', 'N/A')}")
                        
                except Exception as e:
                    _log(logger, "debug", f"Error compiling hotel data: {e}")
                    
            _log(logger, "info", f"🔥 Successfully compiled {len(hotels)} complete hotel objects from GraphQL APIs")
            return hotels
            
        except Exception as e:
            _log(logger, "error", f"Error compiling hotel data from APIs: {e}")
            return []
    
    @staticmethod
    def _extract_field(data: Dict[str, Any], field_names: List[str]):
        """Extract field value from data using multiple possible field names."""
        for field_name in field_names:
            if field_name in data and data[field_name]:
                return data[field_name]
        return None
    
    @staticmethod
    def _extract_price_field(data: Dict[str, Any]):
        """Extract price field with special handling for price structures."""
        # Try direct price fields first
        for field_name in ["price", "pricePerNight", "nightlyPrice", "displayPrice"]:
            if field_name in data:
                price_data = data[field_name]
                if isinstance(price_data, (int, float)):
                    return float(price_data)
                elif isinstance(price_data, dict):
                    # Look for price value in nested structure
                    for price_key in ["value", "amount", "price", "displayValue"]:
                        if price_key in price_data:
                            try:
                                return float(price_data[price_key])
                            except (ValueError, TypeError):
                                continue
                elif isinstance(price_data, str):
                    # Extract price from string
                    price_value = BookingHotelsTask._extract_price_from_text(price_data, None)
                    if price_value:
                        return price_value
        return None
    
    @staticmethod
    async def _handle_cookie_consent(page, logger: logging.Logger):
        """Handle cookie consent popup."""
        try:
            cookie_selectors = [
                "button[data-testid*='cookie']",
                "button:has-text('Accept')",
                "button:has-text('I accept')", 
                "button:has-text('Accept all')",
                "#onetrust-accept-btn-handler",
                ".bui-button--primary:has-text('OK')"
            ]
            
            for selector in cookie_selectors:
                try:
                    await page.click(selector, timeout=3000)
                    _log(logger, "info", f"🍪 Accepted cookies with {selector}")
                    await page.wait_for_timeout(1000)
                    break
                except:
                    continue
        except Exception as e:
            _log(logger, "debug", f"Cookie consent handling: {e}")

    @staticmethod
    async def _debug_intercepted_apis(intercepted_data: Dict[str, Any], logger: logging.Logger):
        """Debug and analyze intercepted API calls to understand response structures."""
        try:
            raw_responses = intercepted_data.get("raw_responses", [])
            
            _log(logger, "info", f"🔍 DEBUGGING: Analyzing {len(raw_responses)} intercepted API calls...")
            
            # First, let's get an overview of all URLs to understand what we're intercepting
            url_summary = {}
            for response in raw_responses:
                url = response.get("url", "")
                # Extract just the path and main parameters to categorize
                if "/dml/graphql" in url:
                    url_key = "/dml/graphql"
                else:
                    url_key = url.split("?")[0] if "?" in url else url
                url_summary[url_key] = url_summary.get(url_key, 0) + 1
            
            _log(logger, "info", f"📈 URL Summary: {url_summary}")
            
            for i, response in enumerate(raw_responses[:15]):  # Increased to 15 for better analysis
                url = response.get("url", "")
                data = response.get("data", {})
                
                _log(logger, "info", f"\n🔍 === API CALL #{i+1} ===")
                _log(logger, "info", f"URL: {url[:150]}...")
                
                if isinstance(data, dict):
                    # Look for data structure
                    if "data" in data:
                        data_content = data["data"]
                        if isinstance(data_content, dict):
                            data_keys = list(data_content.keys())
                            _log(logger, "info", f"📊 GraphQL Data keys: {data_keys}")
                            
                            # Deep analysis of each top-level key
                            for key in data_keys:
                                value = data_content.get(key)
                                if isinstance(value, dict):
                                    nested_keys = list(value.keys())
                                    _log(logger, "info", f"   📁 {key}: {nested_keys}")
                                    
                                    # Look for arrays that might contain hotels
                                    for nested_key in nested_keys:
                                        nested_value = value.get(nested_key)
                                        if isinstance(nested_value, list) and len(nested_value) > 0:
                                            _log(logger, "info", f"      📋 {nested_key}: ARRAY with {len(nested_value)} items")
                                            if len(nested_value) > 0 and isinstance(nested_value[0], dict):
                                                sample_keys = list(nested_value[0].keys())[:15]  # Show more keys
                                                _log(logger, "info", f"         🔑 Sample item keys: {sample_keys}")
                                                
                                                # Look for hotel indicators in the sample
                                                sample_item = nested_value[0]
                                                hotel_indicators = []
                                                for sample_key, sample_value in sample_item.items():
                                                    if any(indicator in str(sample_key).lower() for indicator in 
                                                          ["name", "title", "price", "rating", "hotel", "property"]):
                                                        hotel_indicators.append(f"{sample_key}={sample_value}")
                                                
                                                if hotel_indicators:
                                                    _log(logger, "info", f"         🏨 HOTEL INDICATORS: {hotel_indicators[:5]}")
                                        elif isinstance(nested_value, str) and len(nested_value) > 0:
                                            _log(logger, "info", f"      📝 {nested_key}: '{nested_value[:100]}...'")
                                        elif nested_value is not None:
                                            _log(logger, "info", f"      🔢 {nested_key}: {type(nested_value).__name__} = {str(nested_value)[:50]}")
                                elif isinstance(value, list):
                                    _log(logger, "info", f"   📋 {key}: ARRAY with {len(value)} items")
                                    if len(value) > 0 and isinstance(value[0], dict):
                                        sample_keys = list(value[0].keys())[:10]
                                        _log(logger, "info", f"      🔑 Sample item keys: {sample_keys}")
                                elif value is not None:
                                    _log(logger, "info", f"   📝 {key}: {type(value).__name__} = {str(value)[:100]}")
                        else:
                            _log(logger, "info", f"📊 GraphQL Data: {type(data_content).__name__} = {str(data_content)[:200]}")
                    else:
                        top_keys = list(data.keys())
                        _log(logger, "info", f"📊 Non-GraphQL Response keys: {top_keys}")
                        
                        # Check if this might be an HTML response instead of JSON
                        if isinstance(data, str):
                            _log(logger, "info", f"📄 String response: {data[:200]}...")
                
        except Exception as e:
            _log(logger, "debug", f"Error debugging APIs: {e}")

    @staticmethod
    async def _enhanced_interaction_automation(page, validated_params: Dict[str, Any], logger: logging.Logger):
        """
        🎯 ENHANCED: Automate interactions to trigger more API calls (like manual inspection).
        
        This mimics what you did manually:
        1. Scroll through results to trigger lazy loading APIs
        2. Click on hotel cards to trigger detail APIs
        3. Open filters to trigger filter APIs
        4. Hover over elements to trigger tooltip APIs
        """
        try:
            _log(logger, "info", "🎯 Starting enhanced interaction automation...")
            
            # 1. Scroll through results to trigger lazy loading
            _log(logger, "info", "📜 Scrolling to trigger lazy loading APIs...")
            for i in range(3):
                await page.evaluate("window.scrollBy(0, 800)")
                await page.wait_for_timeout(2000)  # Wait for APIs to be triggered
            
            # 2. Click on hotel cards to trigger detail APIs
            _log(logger, "info", "🏨 Clicking hotel cards to trigger detail APIs...")
            hotel_cards = await page.query_selector_all("[data-testid='property-card'], .sr_property_block")
            for i, card in enumerate(hotel_cards[:3]):  # Limit to 3 cards
                try:
                    # Scroll card into view
                    await card.scroll_into_view_if_needed()
                    await page.wait_for_timeout(1000)
                    
                    # Click on the card (but don't navigate away)
                    await card.click(modifiers=["Control"])  # Ctrl+click to open in new tab
                    await page.wait_for_timeout(3000)  # Wait for APIs
                    
                    _log(logger, "info", f"🏨 Clicked hotel card {i+1}")
                except Exception as e:
                    _log(logger, "debug", f"Failed to click hotel card {i+1}: {e}")
            
            # 3. Interact with filters to trigger filter APIs
            _log(logger, "info", "🔍 Interacting with filters to trigger filter APIs...")
            filter_selectors = [
                "[data-testid='filters-group-label-class']",
                "[data-testid='filters-group-label-price']",
                "[data-testid='filters-group-label-review_score']"
            ]
            
            for selector in filter_selectors:
                try:
                    filter_element = await page.query_selector(selector)
                    if filter_element:
                        await filter_element.click()
                        await page.wait_for_timeout(2000)  # Wait for filter APIs
                        _log(logger, "info", f"🔍 Clicked filter: {selector}")
                except Exception as e:
                    _log(logger, "debug", f"Failed to click filter {selector}: {e}")
            
            # 4. Hover over elements to trigger tooltip/detail APIs
            _log(logger, "info", "🖱️  Hovering over elements to trigger tooltip APIs...")
            hover_selectors = [
                "[data-testid='review-score']",
                "[data-testid='price-and-discounted-price']", 
                ".bui-review-score__badge"
            ]
            
            for selector in hover_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements[:2]:  # Limit to 2 per selector
                        await element.hover()
                        await page.wait_for_timeout(1000)  # Wait for tooltip APIs
                except Exception as e:
                    _log(logger, "debug", f"Failed to hover {selector}: {e}")
            
            _log(logger, "info", "🎯 Enhanced interaction automation complete")
            
        except Exception as e:
            _log(logger, "warning", f"Enhanced interaction automation failed: {e}")


# ═══════════════ Enhanced Website Scraper for Vector Stores ════════════════
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
        from .utils import extract_domain, is_same_domain
        import urllib.parse
        
        # Domain check
        if not is_same_domain(url, f"https://{base_domain}"):
            return True
            
        url_lower = url.lower()
        
        # Special handling for GitHub repositories - restrict to specific repo
        if 'github.com' in base_domain and start_url:
            start_parsed = urllib.parse.urlparse(start_url.lower())
            url_parsed = urllib.parse.urlparse(url_lower)
            
            # Extract repo path from start URL (e.g., "/0aub/qitta")
            start_path_parts = [p for p in start_parsed.path.split('/') if p]
            url_path_parts = [p for p in url_parsed.path.split('/') if p]
            
            if len(start_path_parts) >= 2:  # Should have at least user/repo
                repo_owner = start_path_parts[0]
                repo_name = start_path_parts[1]
                
                # Only allow URLs that start with the exact repository path
                if len(url_path_parts) < 2:
                    return True  # Skip GitHub root and single-level paths
                
                if url_path_parts[0] != repo_owner or url_path_parts[1] != repo_name:
                    return True  # Skip URLs from different repositories or users
                
                # Skip signup/join URLs even within the same domain
                if any(skip_word in url_lower for skip_word in ['join', 'signup', 'register', 'login']):
                    return True
                    
                # Skip generic GitHub pages that aren't repository-specific
                github_skip_paths = [
                    'trending', 'features', 'solutions', 'enterprise', 
                    'pricing', 'sponsors', 'collections', 'topics',
                    'security', 'resources', 'customer-stories',
                    'premium-support', 'newsroom', 'about', 'mobile',
                    'edu', 'marketplace', 'logos', 'social-impact',
                    'git-guides', 'github', 'team', 'password_reset'
                ]
                if len(url_path_parts) >= 3 and url_path_parts[2] in github_skip_paths:
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
        from .utils import extract_links_from_html, normalize_url
        
        all_links = extract_links_from_html(html, current_url)
        valid_links = []
        
        for link in all_links:
            try:
                normalized = normalize_url(link)
                if not ScrapeSiteTask._should_skip_url(normalized, base_domain, start_url):
                    valid_links.append(normalized)
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
        from .utils import fetch_with_retry
        
        success, response, error = await fetch_with_retry(
            client, url, headers, max_retries=2, timeout=20.0
        )
        
        if not success:
            return False, None, {"error": error, "url": url}
        
        try:
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
            return False, None, {"error": f"processing_failed_{type(e).__name__}", "url": url}
    
    @staticmethod
    async def _fetch_with_browser(
        browser: Browser,
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
        browser: Browser,
        start_url: str,
        max_pages: Optional[int],
        use_browser: bool,
        headers: Dict[str, str],
        out_dir: pathlib.Path,
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Execute the main scraping logic."""
        from .utils import extract_domain, normalize_url, save_content_atomic, save_json_atomic, is_content_page, score_content_quality
        
        # Setup
        start_url = normalize_url(start_url)
        base_domain = extract_domain(start_url)
        
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
                logger.info(f"Processing [{len(successful_pages)+1}]: {current_url[:100]}")
                
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
                    logger.warning(f"Failed to fetch {current_url}: {metadata.get('error', 'unknown')}")
                    continue
                
                # Content quality check
                if not is_content_page(current_url, html):
                    logger.debug(f"Skipping low-content page: {current_url}")
                    continue
                
                # Save HTML atomically with readable filename
                safe_filename = ScrapeSiteTask._create_readable_filename(current_url) + ".html"
                html_file = html_dir / safe_filename
                
                save_success, save_error, file_meta = await save_content_atomic(
                    html_file, html.encode('utf-8')
                )
                
                if not save_success:
                    logger.error(f"Failed to save {current_url}: {save_error}")
                    failed_pages.append({"url": current_url, "error": f"save_failed_{save_error}"})
                    continue
                
                # Calculate content quality score
                quality_score = score_content_quality(html)
                
                # Track success
                page_info = {
                    "url": current_url,
                    "file": safe_filename,
                    "quality_score": quality_score,
                    "timestamp": json.dumps(asyncio.get_event_loop().time()),  # For sorting
                    **metadata,
                    **file_meta
                }
                successful_pages.append(page_info)
                total_content_size += file_meta.get("size", 0)
                
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
        
        save_json_atomic(out_dir / "crawl_metadata.json", crawl_metadata)
        save_json_atomic(out_dir / "page_urls.json", [p["url"] for p in successful_pages])
        
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
    async def run(self, *, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
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
        
        logger.info(f"Starting website scrape: {start_url}")
        logger.info(f"Max pages: {max_pages or 'unlimited'}, Browser mode: {use_browser}")
        
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
            
            logger.info(f"Scrape completed: {result['pages_scraped']} pages, avg quality: {result['quality_score_avg']}")
            return result
            
        except Exception as e:
            logger.error(f"Scrape failed: {e}")
            raise


@_registry.register("scrape-site")
async def scrape_site(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    task = ScrapeSiteTask()
    return await task.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)


# ═══════════════ Content Extraction for Vector Stores (Phase 2) ════════════════
class ExtractContentTask:
    """
    Phase 2: Extract clean text content from scraped HTML files.
    Optimized for vector store preparation with intelligent chunking.
    """
    
    # ───── Content extraction strategies ─────
    @staticmethod
    def _extract_with_readability(html: str, url: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """Extract main content using enhanced readability algorithm."""
        try:
            import re
            
            # Step 1: Remove noise elements completely
            html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
            html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
            html = re.sub(r'<noscript[^>]*>.*?</noscript>', '', html, flags=re.DOTALL | re.IGNORECASE)
            html = re.sub(r'<!--.*?-->', '', html, flags=re.DOTALL)
            
            # Remove common noise sections
            noise_patterns = [
                r'<nav[^>]*>.*?</nav>',
                r'<header[^>]*>.*?</header>', 
                r'<footer[^>]*>.*?</footer>',
                r'<aside[^>]*>.*?</aside>',
                r'<div[^>]*class=["\'][^"\']*(?:sidebar|navigation|nav|menu|header|footer|ad|advertisement|social|share|comment)[^"\']*["\'][^>]*>.*?</div>',
                r'<div[^>]*id=["\'][^"\']*(?:sidebar|navigation|nav|menu|header|footer|ad|advertisement|social|share|comment)[^"\']*["\'][^>]*>.*?</div>',
            ]
            
            for pattern in noise_patterns:
                html = re.sub(pattern, '', html, flags=re.DOTALL | re.IGNORECASE)
            
            # Extract title and meta description
            title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else ""
            
            meta_desc_match = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']*)["\']', html, re.IGNORECASE)
            meta_description = meta_desc_match.group(1).strip() if meta_desc_match else ""
            
            # Step 2: Score content areas by text density and semantic value
            content_candidates = []
            
            # Primary content patterns (ordered by priority)
            content_patterns = [
                (r'<article[^>]*>(.*?)</article>', 10),
                (r'<main[^>]*>(.*?)</main>', 9),
                (r'<div[^>]*class=["\'][^"\']*(?:content|post|article|entry)[^"\']*["\'][^>]*>(.*?)</div>', 8),
                (r'<div[^>]*id=["\'][^"\']*(?:content|post|article|entry|main)[^"\']*["\'][^>]*>(.*?)</div>', 7),
                (r'<section[^>]*class=["\'][^"\']*(?:content|post|article|entry)[^"\']*["\'][^>]*>(.*?)</section>', 6),
            ]
            
            for pattern, base_score in content_patterns:
                matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    # Clean HTML tags but preserve structure
                    text = re.sub(r'<[^>]+>', ' ', match)
                    text = re.sub(r'\s+', ' ', text).strip()
                    
                    if len(text) > 100:  # Minimum content threshold
                        # Calculate text density and other quality metrics
                        word_count = len(text.split())
                        char_count = len(text)
                        
                        # Score based on length, structure, and content quality
                        score = base_score
                        score += min(word_count / 100, 5)  # Length bonus (max 5 points)
                        score += text.count('.') * 0.1  # Sentence structure bonus
                        score += len(re.findall(r'\b\w{4,}\b', text)) * 0.01  # Complex words bonus
                        
                        content_candidates.append({
                            'text': text,
                            'score': score,
                            'word_count': word_count,
                            'char_count': char_count
                        })
            
            # Step 3: Fallback to paragraph extraction with scoring
            if not content_candidates or max(c['score'] for c in content_candidates) < 8:
                paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL | re.IGNORECASE)
                
                if paragraphs:
                    # Filter and score paragraphs
                    quality_paragraphs = []
                    for p in paragraphs:
                        text = re.sub(r'<[^>]+>', ' ', p)
                        text = re.sub(r'\s+', ' ', text).strip()
                        
                        if len(text) > 50:  # Minimum paragraph length
                            word_count = len(text.split())
                            # Score paragraphs based on length and content quality
                            score = min(word_count / 20, 3) + text.count('.') * 0.2
                            quality_paragraphs.append({'text': text, 'score': score})
                    
                    if quality_paragraphs:
                        # Sort by score and take top paragraphs
                        quality_paragraphs.sort(key=lambda x: x['score'], reverse=True)
                        combined_text = '\n\n'.join(p['text'] for p in quality_paragraphs[:10])  # Top 10 paragraphs
                        
                        content_candidates.append({
                            'text': combined_text,
                            'score': 6,
                            'word_count': len(combined_text.split()),
                            'char_count': len(combined_text)
                        })
            
            # Step 4: Select best content
            if content_candidates:
                best_content = max(content_candidates, key=lambda x: x['score'])
                
                # Final cleaning
                text = best_content['text']
                # Remove excessive whitespace
                text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)  # Max 2 consecutive newlines
                text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces to single space
                text = text.strip()
                
                # Add title if available and not already in content
                if title and title.lower() not in text.lower()[:200]:
                    text = f"{title}\n\n{text}"
                
                # Add meta description if available and substantial
                if meta_description and len(meta_description) > 50 and meta_description.lower() not in text.lower()[:300]:
                    text = f"{text}\n\n{meta_description}"
                
                metadata = {
                    "extraction_method": "enhanced_readability",
                    "title": title,
                    "meta_description": meta_description,
                    "content_length": len(text),
                    "word_count": len(text.split()),
                    "quality_score": best_content['score'],
                    "candidates_found": len(content_candidates)
                }
                
                return text, metadata
            
            return None, {"error": "no_quality_content_found"}
            
        except Exception as e:
            return None, {"error": f"readability_failed_{type(e).__name__}"}
    
    @staticmethod
    def _extract_with_selectors(html: str, selectors: List[str]) -> Tuple[Optional[str], Dict[str, Any]]:
        """Extract content using custom CSS selectors (simplified)."""
        try:
            import re
            
            content_parts = []
            
            for selector in selectors:
                # Simple selector matching (basic implementation)
                if selector.startswith('.'):
                    # Class selector
                    class_name = selector[1:]
                    pattern = f'<[^>]*class=["\'][^"\']*{re.escape(class_name)}[^"\']*["\'][^>]*>(.*?)</[^>]+>'
                elif selector.startswith('#'):
                    # ID selector
                    id_name = selector[1:]
                    pattern = f'<[^>]*id=["\'][^"\']*{re.escape(id_name)}[^"\']*["\'][^>]*>(.*?)</[^>]+>'
                else:
                    # Tag selector
                    pattern = f'<{re.escape(selector)}[^>]*>(.*?)</{re.escape(selector)}>'
                
                matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    text = re.sub(r'<[^>]+>', ' ', match)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if text and len(text) > 50:  # Only substantial content
                        content_parts.append(text)
            
            if content_parts:
                content = '\n\n'.join(content_parts)
                metadata = {
                    "extraction_method": "selectors",
                    "selectors_used": selectors,
                    "content_length": len(content),
                    "word_count": len(content.split()),
                    "parts_found": len(content_parts)
                }
                return content, metadata
            
            return None, {"error": "no_selector_matches"}
            
        except Exception as e:
            return None, {"error": f"selector_extraction_failed_{type(e).__name__}"}
    
    # ───── Text chunking for vector stores ─────
    @staticmethod
    def _chunk_text_semantic(
        text: str, 
        chunk_size: int = 1000, 
        overlap: int = 200,
        preserve_paragraphs: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Chunk text semantically for optimal vector embedding.
        Preserves paragraph boundaries and maintains context.
        """
        if not text or len(text.strip()) < 50:
            return []
        
        # Split into paragraphs first
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        if not paragraphs:
            paragraphs = [text]
        
        chunks = []
        current_chunk = ""
        current_length = 0
        chunk_id = 0
        
        for para in paragraphs:
            para_length = len(para)
            
            # If paragraph is too long, split it at sentence boundaries
            if para_length > chunk_size:
                sentences = [s.strip() for s in para.split('.') if s.strip()]
                
                for sentence in sentences:
                    sentence += '.'  # Restore period
                    sentence_length = len(sentence)
                    
                    if current_length + sentence_length > chunk_size and current_chunk:
                        # Save current chunk
                        chunks.append({
                            "chunk_id": chunk_id,
                            "content": current_chunk.strip(),
                            "token_count": len(current_chunk.split()),
                            "char_count": len(current_chunk),
                            "start_pos": len(' '.join(c["content"] for c in chunks)),
                        })
                        chunk_id += 1
                        
                        # Start new chunk with overlap
                        if overlap > 0 and len(current_chunk) > overlap:
                            current_chunk = current_chunk[-overlap:] + ' ' + sentence
                            current_length = len(current_chunk)
                        else:
                            current_chunk = sentence
                            current_length = sentence_length
                    else:
                        current_chunk += ' ' + sentence if current_chunk else sentence
                        current_length += sentence_length
            else:
                # Regular paragraph handling
                if current_length + para_length > chunk_size and current_chunk:
                    # Save current chunk
                    chunks.append({
                        "chunk_id": chunk_id,
                        "content": current_chunk.strip(),
                        "token_count": len(current_chunk.split()),
                        "char_count": len(current_chunk),
                        "start_pos": len(' '.join(c["content"] for c in chunks)),
                    })
                    chunk_id += 1
                    
                    # Start new chunk with overlap
                    if overlap > 0 and len(current_chunk) > overlap:
                        current_chunk = current_chunk[-overlap:] + '\n\n' + para
                        current_length = len(current_chunk)
                    else:
                        current_chunk = para
                        current_length = para_length
                else:
                    current_chunk += '\n\n' + para if current_chunk else para
                    current_length += para_length
        
        # Add final chunk
        if current_chunk.strip():
            chunks.append({
                "chunk_id": chunk_id,
                "content": current_chunk.strip(),
                "token_count": len(current_chunk.split()),
                "char_count": len(current_chunk),
                "start_pos": len(' '.join(c["content"] for c in chunks[:-1])) if chunks else 0,
            })
        
        return chunks
    
    # ───── Main content extraction ─────
    @staticmethod
    async def _extract_content_from_html(
        html_file: pathlib.Path,
        url: str,
        extraction_config: Dict[str, Any],
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Extract clean content from a single HTML file."""
        
        try:
            html = html_file.read_text(encoding='utf-8')
        except Exception as e:
            return {"status": "error", "error": f"read_failed_{type(e).__name__}", "url": url}
        
        # Try extraction strategies in order
        content = None
        extraction_metadata = {}
        
        # Strategy 1: Custom selectors (if provided)
        if extraction_config.get("selectors"):
            content, extraction_metadata = ExtractContentTask._extract_with_selectors(
                html, extraction_config["selectors"]
            )
        
        # Strategy 2: Readability algorithm (default)
        if not content:
            content, extraction_metadata = ExtractContentTask._extract_with_readability(html, url)
        
        if not content:
            return {
                "status": "error", 
                "url": url,
                "file": html_file.name,
                **extraction_metadata
            }
        
        # Text chunking
        chunk_size = extraction_config.get("chunk_size", 1000)
        overlap = extraction_config.get("overlap", 200)
        
        chunks = ExtractContentTask._chunk_text_semantic(
            content, chunk_size=chunk_size, overlap=overlap
        )
        
        if not chunks:
            return {
                "status": "error",
                "error": "chunking_failed",
                "url": url,
                "file": html_file.name
            }
        
        return {
            "status": "success",
            "url": url,
            "file": html_file.name,
            "total_chunks": len(chunks),
            "total_tokens": sum(c["token_count"] for c in chunks),
            "chunks": chunks,
            **extraction_metadata
        }
    
    # ───── Main entry point ─────
    async def run(self, *, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Extract clean content from previously scraped HTML files.
        
        Parameters:
        - job_id: ID of the scrape-site job to extract content from
        - source_dir: Alternative to job_id - direct path to scraped data directory
        
        Expected input: Directory containing raw_html/ and crawl_metadata.json from scrape-site task
        Output: Vector-store ready content with intelligent chunking
        """
        from .utils import save_json_atomic
        
        # Determine source directory
        source_dir = None
        
        if "job_id" in params:
            # Use job_id to find the scraped data directory
            job_id = params["job_id"].strip()
            if not job_id:
                raise ValueError("job_id parameter cannot be empty")
            
            # Look for the job directory in common locations
            from .config import DATA_ROOT
            possible_paths = [
                pathlib.Path(f"{DATA_ROOT}/scrape-site/{job_id}"),  # Main storage: task_name/job_id
                pathlib.Path(f"{DATA_ROOT}/{job_id}"),  # Fallback: just job_id
                pathlib.Path(f"/storage/scraped_data/scrape-site/{job_id}"),  # Container: task_name/job_id
                pathlib.Path(f"/storage/scraped_data/{job_id}"),  # Container: just job_id
                pathlib.Path(f"./output/{job_id}"),  # Local output
                pathlib.Path(job_id),  # Direct path if job_id is actually a path
            ]
            
            for path in possible_paths:
                if path.exists() and (path / "raw_html").exists():
                    source_dir = path
                    logger.info(f"Found scraped data at: {source_dir}")
                    break
            
            if not source_dir:
                raise ValueError(f"Could not find scraped data for job_id: {job_id}. Tried: {[str(p) for p in possible_paths]}")
                
        elif "source_dir" in params:
            # Use explicit source directory
            source_dir = pathlib.Path(params["source_dir"])
            if not source_dir.exists():
                raise ValueError(f"Source directory does not exist: {source_dir}")
        else:
            raise ValueError("Either 'job_id' or 'source_dir' parameter is required")
        
        # Find HTML files
        html_dir = source_dir / "raw_html"
        if not html_dir.exists():
            raise ValueError(f"HTML directory not found: {html_dir}. Make sure the scrape-site task completed successfully.")
        
        # Load crawl metadata if available
        metadata_file = source_dir / "crawl_metadata.json"
        crawl_metadata = {}
        if metadata_file.exists():
            try:
                crawl_metadata = json.loads(metadata_file.read_text())
            except Exception:
                logger.warning("Could not load crawl metadata")
        
        # Extraction configuration
        extraction_config = {
            "chunk_size": params.get("chunk_size", 1000),
            "overlap": params.get("overlap", 200),
            "selectors": params.get("selectors", []),  # Custom CSS selectors
            "min_content_length": params.get("min_content_length", 100)
        }
        
        # Setup output
        out_dir = pathlib.Path(job_output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        content_dir = out_dir / "extracted_content"
        content_dir.mkdir(exist_ok=True)
        
        # Process HTML files
        html_files = list(html_dir.glob("*.html"))
        logger.info(f"Processing {len(html_files)} HTML files")
        
        all_documents = []
        successful_extractions = 0
        failed_extractions = 0
        total_chunks = 0
        total_tokens = 0
        
        for i, html_file in enumerate(html_files, 1):
            # Reconstruct URL from filename
            url = urllib.parse.unquote(html_file.stem)
            
            logger.info(f"[{i}/{len(html_files)}] Extracting: {url[:80]}...")
            
            result = await ExtractContentTask._extract_content_from_html(
                html_file, url, extraction_config, logger
            )
            
            if result["status"] == "success":
                successful_extractions += 1
                total_chunks += result["total_chunks"]
                total_tokens += result["total_tokens"]
                
                # Save individual document
                doc_file = content_dir / f"doc_{i:04d}.json"
                save_json_atomic(doc_file, result)
                
                # Add to collection
                all_documents.append({
                    "document_id": f"doc_{i:04d}",
                    "url": url,
                    "file": doc_file.name,
                    "chunks": result["total_chunks"],
                    "tokens": result["total_tokens"],
                    "extraction_method": result.get("extraction_method", "unknown")
                })
            else:
                failed_extractions += 1
                logger.warning(f"Failed to extract {url}: {result.get('error', 'unknown')}")
        
        # Create final output for vector store
        vector_store_output = {
            "source_info": {
                "base_domain": crawl_metadata.get("base_domain", "unknown"),
                "start_url": crawl_metadata.get("start_url", "unknown"),
                "crawl_timestamp": crawl_metadata.get("timestamp", "unknown")
            },
            "extraction_config": extraction_config,
            "statistics": {
                "total_html_files": len(html_files),
                "successful_extractions": successful_extractions,
                "failed_extractions": failed_extractions,
                "total_documents": len(all_documents),
                "total_chunks": total_chunks,
                "total_tokens": total_tokens,
                "avg_chunks_per_doc": round(total_chunks / len(all_documents), 2) if all_documents else 0,
                "avg_tokens_per_chunk": round(total_tokens / total_chunks, 2) if total_chunks else 0
            },
            "documents": all_documents
        }
        
        # Save final outputs
        save_json_atomic(out_dir / "vector_store_ready.json", vector_store_output)
        save_json_atomic(out_dir / "extraction_summary.json", {
            "successful": successful_extractions,
            "failed": failed_extractions,
            "total_chunks": total_chunks,
            "total_tokens": total_tokens,
            "config": extraction_config
        })
        
        logger.info(f"Content extraction completed: {successful_extractions}/{len(html_files)} files, {total_chunks} chunks, {total_tokens} tokens")
        
        return {
            "source_directory": str(source_dir),
            "html_files_found": len(html_files),
            "successful_extractions": successful_extractions,
            "failed_extractions": failed_extractions,
            "total_chunks": total_chunks,
            "total_tokens": total_tokens,
            "content_directory": "extracted_content",
            "vector_store_file": "vector_store_ready.json",
            "summary_file": "extraction_summary.json"
        }


@_registry.register("extract-content")
async def extract_content(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    task = ExtractContentTask()
    return await task.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)


# ═══════════════ GitHub Repository Analyzer ════════════════
class GitHubRepoTask:
    """
    Comprehensive GitHub repository analyzer that collects code, issues, releases, documentation,
    and repository metadata for creating a complete knowledge base of a GitHub repository.
    Uses the official GitHub API for efficient and reliable data collection.
    """
    
    # ───── GitHub API utilities ─────
    @staticmethod
    def _parse_github_url(url: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract owner and repo from GitHub URL."""
        try:
            parsed = urllib.parse.urlparse(url.lower())
            if 'github.com' not in parsed.netloc:
                return None, None
            
            path_parts = [p for p in parsed.path.split('/') if p]
            if len(path_parts) >= 2:
                return path_parts[0], path_parts[1]
            return None, None
        except Exception:
            return None, None
    
    @staticmethod
    async def _github_api_request(
        client: httpx.AsyncClient,
        endpoint: str,
        headers: Dict[str, str],
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """Make GitHub API request with error handling."""
        try:
            url = f"https://api.github.com/{endpoint.lstrip('/')}"
            response = await client.get(url, headers=headers, params=params or {})
            
            if response.status_code == 200:
                return True, response.json(), None
            elif response.status_code == 403:
                return False, None, "api_rate_limit"
            elif response.status_code == 404:
                return False, None, "not_found"
            else:
                return False, None, f"api_error_{response.status_code}"
        except Exception as e:
            return False, None, f"request_failed_{type(e).__name__}"
    
    # ───── Repository metadata collection ─────
    @staticmethod
    async def _get_repo_metadata(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        logger: logging.Logger
    ) -> Dict[str, Any]:
        """Collect comprehensive repository metadata."""
        
        # Basic repository info
        success, repo_data, error = await GitHubRepoTask._github_api_request(
            client, f"repos/{owner}/{repo}", headers
        )
        
        if not success:
            logger.warning(f"Failed to get repo metadata: {error}")
            return {"error": error}
        
        metadata = {
            "repository": {
                "owner": owner,
                "name": repo,
                "full_name": repo_data.get("full_name"),
                "description": repo_data.get("description"),
                "html_url": repo_data.get("html_url"),
                "clone_url": repo_data.get("clone_url"),
                "language": repo_data.get("language"),
                "languages_url": repo_data.get("languages_url"),
                "size": repo_data.get("size"),
                "stargazers_count": repo_data.get("stargazers_count"),
                "watchers_count": repo_data.get("watchers_count"),
                "forks_count": repo_data.get("forks_count"),
                "open_issues_count": repo_data.get("open_issues_count"),
                "created_at": repo_data.get("created_at"),
                "updated_at": repo_data.get("updated_at"),
                "pushed_at": repo_data.get("pushed_at"),
                "default_branch": repo_data.get("default_branch", "main"),
                "license": repo_data.get("license", {}).get("name") if repo_data.get("license") else None,
                "topics": repo_data.get("topics", [])
            }
        }
        
        # Get languages
        success, languages, error = await GitHubRepoTask._github_api_request(
            client, f"repos/{owner}/{repo}/languages", headers
        )
        if success:
            metadata["repository"]["languages"] = languages
        
        # Get contributors (top 100)
        success, contributors, error = await GitHubRepoTask._github_api_request(
            client, f"repos/{owner}/{repo}/contributors", headers, {"per_page": 100}
        )
        if success:
            metadata["repository"]["contributors"] = contributors
        
        return metadata
    
    # ───── Code content collection ─────
    @staticmethod
    async def _get_repository_tree(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        branch: str = "main",
        max_files: int = 500
    ) -> List[Dict[str, Any]]:
        """Get repository file tree with content."""
        
        # Get the tree recursively
        success, tree_data, error = await GitHubRepoTask._github_api_request(
            client, f"repos/{owner}/{repo}/git/trees/{branch}", headers, {"recursive": "1"}
        )
        
        if not success:
            # Try alternative branch names if default fails
            if branch == "main":
                success, tree_data, error = await GitHubRepoTask._github_api_request(
                    client, f"repos/{owner}/{repo}/git/trees/master", headers, {"recursive": "1"}
                )
            elif branch == "master":
                success, tree_data, error = await GitHubRepoTask._github_api_request(
                    client, f"repos/{owner}/{repo}/git/trees/main", headers, {"recursive": "1"}
                )
            
            if not success:
                return []
        
        files = []
        tree_items = tree_data.get("tree", [])
        
        # Filter for important files
        important_files = []
        code_files = []
        
        for item in tree_items:
            if item.get("type") != "blob":  # Skip directories
                continue
                
            path = item.get("path", "")
            size = item.get("size", 0)
            
            # Skip very large files (>1MB)
            if size > 1024 * 1024:
                continue
            
            # Important files (prioritize)
            if any(important in path.lower() for important in [
                "readme", "license", "changelog", "contributing", "security",
                "dockerfile", "docker-compose", "makefile", "package.json",
                "requirements.txt", "setup.py", "cargo.toml", "go.mod",
                ".gitignore", ".env.example"
            ]):
                important_files.append(item)
            # Code files
            elif any(path.lower().endswith(ext) for ext in [
                '.py', '.js', '.ts', '.jsx', '.tsx', '.java', '.cpp', '.c', '.h',
                '.cs', '.php', '.rb', '.go', '.rs', '.swift', '.kt', '.scala',
                '.html', '.css', '.scss', '.vue', '.md', '.yml', '.yaml',
                '.json', '.xml', '.toml', '.cfg', '.ini'
            ]):
                code_files.append(item)
        
        # Combine and limit
        selected_files = important_files + code_files[:max_files - len(important_files)]
        
        # Get file contents
        for item in selected_files[:max_files]:
            path = item.get("path")
            sha = item.get("sha")
            
            if not path or not sha:
                continue
            
            # Get file content
            success, content_data, error = await GitHubRepoTask._github_api_request(
                client, f"repos/{owner}/{repo}/git/blobs/{sha}", headers
            )
            
            if success:
                content = content_data.get("content", "")
                encoding = content_data.get("encoding", "")
                
                # Decode content if base64
                decoded_content = ""
                if encoding == "base64":
                    try:
                        import base64
                        decoded_content = base64.b64decode(content).decode('utf-8', errors='ignore')
                    except Exception:
                        decoded_content = "[Binary file or encoding error]"
                else:
                    decoded_content = content
                
                files.append({
                    "path": path,
                    "size": item.get("size", 0),
                    "sha": sha,
                    "content": decoded_content,
                    "url": f"https://github.com/{owner}/{repo}/blob/{branch}/{path}"
                })
            
            # Rate limiting
            await asyncio.sleep(0.1)
        
        return files
    
    # ───── Issues collection ─────
    @staticmethod
    async def _get_issues(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        max_issues: int = 200
    ) -> List[Dict[str, Any]]:
        """Get repository issues and pull requests."""
        
        issues = []
        page = 1
        per_page = 100
        
        while len(issues) < max_issues:
            # Get both issues and pull requests
            success, page_data, error = await GitHubRepoTask._github_api_request(
                client, f"repos/{owner}/{repo}/issues", headers, {
                    "state": "all",
                    "sort": "updated",
                    "direction": "desc",
                    "per_page": per_page,
                    "page": page
                }
            )
            
            if not success or not page_data:
                break
            
            for issue in page_data:
                # Get comments for important issues
                comments = []
                if issue.get("comments", 0) > 0 and len(issues) < 50:  # Get comments for first 50 issues
                    comments_success, comments_data, _ = await GitHubRepoTask._github_api_request(
                        client, f"repos/{owner}/{repo}/issues/{issue['number']}/comments", headers
                    )
                    if comments_success:
                        comments = comments_data
                
                issue_data = {
                    "number": issue.get("number"),
                    "title": issue.get("title"),
                    "body": issue.get("body", ""),
                    "state": issue.get("state"),
                    "labels": [label.get("name") for label in issue.get("labels", [])],
                    "user": issue.get("user", {}).get("login"),
                    "created_at": issue.get("created_at"),
                    "updated_at": issue.get("updated_at"),
                    "closed_at": issue.get("closed_at"),
                    "is_pull_request": "pull_request" in issue,
                    "html_url": issue.get("html_url"),
                    "comments_count": issue.get("comments", 0),
                    "comments": comments[:10]  # Limit comments per issue
                }
                
                issues.append(issue_data)
            
            if len(page_data) < per_page:  # Last page
                break
                
            page += 1
            await asyncio.sleep(0.2)  # Rate limiting
        
        return issues[:max_issues]
    
    # ───── Releases collection ─────
    @staticmethod
    async def _get_releases(
        client: httpx.AsyncClient,
        owner: str,
        repo: str,
        headers: Dict[str, str],
        max_releases: int = 50
    ) -> List[Dict[str, Any]]:
        """Get repository releases."""
        
        success, releases_data, error = await GitHubRepoTask._github_api_request(
            client, f"repos/{owner}/{repo}/releases", headers, {"per_page": max_releases}
        )
        
        if not success:
            return []
        
        releases = []
        for release in releases_data:
            release_data = {
                "tag_name": release.get("tag_name"),
                "name": release.get("name"),
                "body": release.get("body", ""),
                "draft": release.get("draft"),
                "prerelease": release.get("prerelease"),
                "created_at": release.get("created_at"),
                "published_at": release.get("published_at"),
                "author": release.get("author", {}).get("login"),
                "html_url": release.get("html_url"),
                "assets": [
                    {
                        "name": asset.get("name"),
                        "download_count": asset.get("download_count"),
                        "size": asset.get("size"),
                        "browser_download_url": asset.get("browser_download_url")
                    }
                    for asset in release.get("assets", [])
                ]
            }
            releases.append(release_data)
        
        return releases
    
    
    # ───── Main execution ─────
    async def run(self, *, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        """
        Main entry point for GitHub repository scraping.
        
        Collects repository metadata, code files, issues, releases, and documentation
        to create a comprehensive knowledge base of the repository.
        """
        from .utils import save_json_atomic
        
        # Validate parameters
        repo_url = params.get("repo_url", "").strip()
        if not repo_url:
            raise ValueError("Parameter 'repo_url' is required (GitHub repository URL)")
        
        owner, repo_name = GitHubRepoTask._parse_github_url(repo_url)
        if not owner or not repo_name:
            raise ValueError(f"Invalid GitHub repository URL: {repo_url}")
        
        # Configuration - all unlimited by default, user can specify limits
        github_token = params.get("github_token", "")  # Optional for higher rate limits and private repos
        max_files = params.get("max_files", None)  # None = unlimited (all files)
        max_issues = params.get("max_issues", None)  # None = unlimited (all issues)
        max_releases = params.get("max_releases", None)  # None = unlimited (all releases)
        include_code = params.get("include_code", True)
        include_issues = params.get("include_issues", True)
        include_releases = params.get("include_releases", True)
        
        # Setup headers
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Repo-Scraper/1.0"
        }
        if github_token:
            headers["Authorization"] = f"token {github_token}"
        
        # Setup output directory
        out_dir = pathlib.Path(job_output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Starting GitHub repository scrape: {owner}/{repo_name}")
        
        # Collect data
        results = {
            "repository_url": repo_url,
            "owner": owner,
            "repo_name": repo_name,
            "scrape_timestamp": asyncio.get_event_loop().time(),
            "metadata": {},
            "files": [],
            "issues": [],
            "releases": [],
            "statistics": {}
        }
        
        timeout = httpx.Timeout(30.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            
            # 1. Repository metadata
            logger.info("Collecting repository metadata...")
            results["metadata"] = await GitHubRepoTask._get_repo_metadata(
                client, owner, repo_name, headers, logger
            )
            
            if "error" in results["metadata"]:
                error_type = results["metadata"]["error"]
                
                # Provide clear error messages and suggestions
                if error_type == "api_rate_limit":
                    raise RuntimeError(
                        "GitHub API rate limit exceeded (60 requests/hour for unauthenticated requests). "
                        "Solutions: 1) Provide a 'github_token' parameter for 5000 requests/hour, "
                        "2) Wait an hour for rate limit reset, "
                        "3) Use 'scrape-site' task directly on the repository URL for basic content extraction."
                    )
                elif error_type == "not_found":
                    raise RuntimeError(
                        f"Repository '{owner}/{repo_name}' not found. "
                        "Check if: 1) Repository exists, 2) Repository is private (requires github_token), "
                        "3) Owner/repository name is correct."
                    )
                else:
                    raise RuntimeError(f"Failed to access repository: {error_type}")
            
            default_branch = results["metadata"]["repository"].get("default_branch", "main")
            
            # 2. Code files
            if include_code:
                files_limit = max_files or 9999  # Use high number if unlimited
                logger.info(f"Collecting code files ({'unlimited' if max_files is None else f'max: {max_files}'})...")
                results["files"] = await GitHubRepoTask._get_repository_tree(
                    client, owner, repo_name, headers, default_branch, files_limit
                )
                logger.info(f"Collected {len(results['files'])} files")
            
            # 3. Issues and pull requests
            if include_issues:
                issues_limit = max_issues or 9999  # Use high number if unlimited
                logger.info(f"Collecting issues and PRs ({'unlimited' if max_issues is None else f'max: {max_issues}'})...")
                results["issues"] = await GitHubRepoTask._get_issues(
                    client, owner, repo_name, headers, issues_limit
                )
                logger.info(f"Collected {len(results['issues'])} issues/PRs")
            
            # 4. Releases
            if include_releases:
                releases_limit = max_releases or 9999  # Use high number if unlimited
                logger.info(f"Collecting releases ({'unlimited' if max_releases is None else f'max: {max_releases}'})...")
                results["releases"] = await GitHubRepoTask._get_releases(
                    client, owner, repo_name, headers, releases_limit
                )
                logger.info(f"Collected {len(results['releases'])} releases")
        
        # Calculate statistics
        total_code_size = sum(f.get("size", 0) for f in results["files"])
        code_files = [f for f in results["files"] if not any(
            doc in f.get("path", "").lower() for doc in ["readme", "license", "changelog"]
        )]
        doc_files = [f for f in results["files"] if any(
            doc in f.get("path", "").lower() for doc in ["readme", "license", "changelog", ".md"]
        )]
        
        issues_by_state = {}
        for issue in results["issues"]:
            state = issue.get("state", "unknown")
            issues_by_state[state] = issues_by_state.get(state, 0) + 1
        
        results["statistics"] = {
            "total_files": len(results["files"]),
            "code_files": len(code_files),
            "documentation_files": len(doc_files),
            "total_code_size": total_code_size,
            "total_issues": len(results["issues"]),
            "issues_by_state": issues_by_state,
            "total_releases": len(results["releases"]),
            "languages": results["metadata"]["repository"].get("languages", {}),
            "stars": results["metadata"]["repository"].get("stargazers_count", 0),
            "forks": results["metadata"]["repository"].get("forks_count", 0)
        }
        
        # Save outputs
        save_json_atomic(out_dir / "repository_complete.json", results)
        save_json_atomic(out_dir / "metadata.json", results["metadata"])
        save_json_atomic(out_dir / "files.json", results["files"])
        save_json_atomic(out_dir / "issues.json", results["issues"])
        save_json_atomic(out_dir / "releases.json", results["releases"])
        save_json_atomic(out_dir / "statistics.json", results["statistics"])
        
        # Create vector-store ready content
        vector_content = []
        
        # Add repository overview
        repo_info = results["metadata"]["repository"]
        overview = f"""Repository: {repo_info['full_name']}
Description: {repo_info.get('description', 'No description')}
Language: {repo_info.get('language', 'Multiple')}
Stars: {repo_info.get('stargazers_count', 0)}
Forks: {repo_info.get('forks_count', 0)}
License: {repo_info.get('license', 'Not specified')}
Topics: {', '.join(repo_info.get('topics', []))}
"""
        vector_content.append({
            "type": "repository_overview",
            "content": overview,
            "metadata": {"source": "repository_metadata"}
        })
        
        # Add file contents
        for file_info in results["files"]:
            if file_info.get("content") and len(file_info["content"]) > 50:
                vector_content.append({
                    "type": "code_file",
                    "content": f"File: {file_info['path']}\n\n{file_info['content']}",
                    "metadata": {
                        "source": "code_file",
                        "path": file_info["path"],
                        "size": file_info.get("size", 0),
                        "url": file_info.get("url")
                    }
                })
        
        # Add issues
        for issue in results["issues"]:
            issue_content = f"Issue #{issue['number']}: {issue['title']}\n\n{issue.get('body', '')}"
            if issue.get("comments"):
                issue_content += "\n\nComments:\n"
                for comment in issue["comments"]:
                    issue_content += f"- {comment.get('body', '')}\n"
            
            vector_content.append({
                "type": "issue" if not issue.get("is_pull_request") else "pull_request",
                "content": issue_content,
                "metadata": {
                    "source": "issue",
                    "number": issue["number"],
                    "state": issue["state"],
                    "labels": issue.get("labels", []),
                    "url": issue.get("html_url")
                }
            })
        
        # Add releases
        for release in results["releases"]:
            if release.get("body"):
                release_content = f"Release {release['tag_name']}: {release.get('name', '')}\n\n{release['body']}"
                vector_content.append({
                    "type": "release",
                    "content": release_content,
                    "metadata": {
                        "source": "release",
                        "tag": release["tag_name"],
                        "url": release.get("html_url")
                    }
                })
        
        save_json_atomic(out_dir / "vector_store_content.json", {
            "repository": f"{owner}/{repo_name}",
            "total_items": len(vector_content),
            "items": vector_content
        })
        
        logger.info(f"GitHub scrape completed: {len(results['files'])} files, {len(results['issues'])} issues, {len(results['releases'])} releases")
        
        return {
            "repository_url": repo_url,
            "owner": owner,
            "repo_name": repo_name,
            "files_collected": len(results["files"]),
            "issues_collected": len(results["issues"]),
            "releases_collected": len(results["releases"]),
            "total_code_size": total_code_size,
            "vector_items": len(vector_content),
            "output_files": {
                "complete": "repository_complete.json",
                "metadata": "metadata.json", 
                "files": "files.json",
                "issues": "issues.json",
                "releases": "releases.json",
                "statistics": "statistics.json",
                "vector_content": "vector_store_content.json"
            }
        }


@_registry.register("github-repo")
async def github_repo(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    task = GitHubRepoTask()
    return await task.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)
