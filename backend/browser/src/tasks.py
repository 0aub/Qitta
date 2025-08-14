# tasks.py — registry + tasks (Saudi Open Data kept in one module)
# 2025-08-10

from __future__ import annotations

import asyncio
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


# ═══════════════ Generic website crawler (class wrapper, logic unchanged) ════════════════
class ScrapeSiteTask:
    async def run(self, *, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
        import re
        start = params["url"]
        dom = urllib.parse.urlparse(start).netloc
        lim = None if str(params.get("max_pages", "")).strip() in {"", "0", "-1"} else int(params["max_pages"])
        q, seen, saved = [start], set(), []
        out = pathlib.Path(job_output_dir)
        out.mkdir(parents=True, exist_ok=True)
        async with httpx.AsyncClient(follow_redirects=True, timeout=20) as cl:
            while q and (lim is None or len(seen) < lim):
                url = q.pop(0)
                seen.add(url)
                try:
                    r = await cl.get(url, headers={"User-Agent": params.get("user_agent", "Mozilla/5.0")})
                except Exception as e:
                    logger.error(f"{url}: {e}")
                    continue
                if r.status_code != 200:
                    continue
                html = r.text
                saved.append(url)
                (out / f"{urllib.parse.quote(url, safe='')}.html").write_text(html, "utf-8")
                for h in re.findall(r'href=["\\\'](.*?)["\\\']', html, re.I):
                    if not h:
                        continue
                    full = h if h.startswith("http") else urllib.parse.urljoin(url, h)
                    p = urllib.parse.urlparse(full)
                    if p.netloc != dom:
                        continue
                    full = urllib.parse.urlunparse(p._replace(fragment=""))
                    if full not in seen and full not in q:
                        q.append(full)
                await asyncio.sleep(random.uniform(1, 2))
        (out / "urls.json").write_text(json.dumps(saved, indent=2, ensure_ascii=False), "utf-8")
        return {"url": start, "pages_scraped": len(saved), "unbounded": lim is None, "urls_file": "urls.json"}


@_registry.register("scrape-site")
async def scrape_site(*, browser: Browser, params: Dict[str, Any], job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    task = ScrapeSiteTask()
    return await task.run(browser=browser, params=params, job_output_dir=job_output_dir, logger=logger)
