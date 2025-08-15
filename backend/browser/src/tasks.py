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


# ═══════════════ Enhanced Website Scraper for Vector Stores ════════════════
class ScrapeSiteTask:
    """
    Professional two-phase website scraper optimized for vector store preparation.
    
    Phase 1: Intelligent HTML collection with JS rendering support
    Phase 2: Offline content extraction and chunking (separate task: extract-content)
    """
    
    # ───── URL management utilities ─────
    @staticmethod
    def _should_skip_url(url: str, base_domain: str) -> bool:
        """Filter out non-content URLs before fetching."""
        from .utils import extract_domain, is_same_domain
        
        # Domain check
        if not is_same_domain(url, f"https://{base_domain}"):
            return True
            
        url_lower = url.lower()
        
        # Skip obvious non-content URLs
        skip_patterns = [
            '/search', '/login', '/register', '/cart', '/checkout', '/admin',
            '/api/', '/ajax/', '/.well-known/', '/wp-admin/', '/wp-json/',
            '.xml', '.json', '.rss', '.atom', '.pdf', '.doc', '.docx',
            '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.tar', '.gz',
            '/contact', '/privacy', '/terms', '/sitemap', '/robots.txt',
            '/favicon.ico', '/apple-touch-icon', '/.css', '/.js', '/.map'
        ]
        
        return any(pattern in url_lower for pattern in skip_patterns)
    
    @staticmethod
    def _extract_urls_from_page(html: str, current_url: str, base_domain: str) -> List[str]:
        """Extract valid URLs from a page's HTML."""
        from .utils import extract_links_from_html, normalize_url
        
        all_links = extract_links_from_html(html, current_url)
        valid_links = []
        
        for link in all_links:
            try:
                normalized = normalize_url(link)
                if not ScrapeSiteTask._should_skip_url(normalized, base_domain):
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
                
                # Save HTML atomically
                safe_filename = urllib.parse.quote(current_url, safe='') + ".html"
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
                    new_urls = ScrapeSiteTask._extract_urls_from_page(html, current_url, base_domain)
                    
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
        """Extract main content using readability algorithm."""
        try:
            # Simple readability-like algorithm without external dependencies
            import re
            
            # Remove script and style elements
            html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
            html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
            
            # Extract title
            title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
            title = title_match.group(1).strip() if title_match else ""
            
            # Look for main content areas
            content_patterns = [
                r'<article[^>]*>(.*?)</article>',
                r'<main[^>]*>(.*?)</main>',
                r'<div[^>]*class=["\'][^"\']*content[^"\']*["\'][^>]*>(.*?)</div>',
                r'<div[^>]*class=["\'][^"\']*article[^"\']*["\'][^>]*>(.*?)</div>',
                r'<div[^>]*id=["\'][^"\']*content[^"\']*["\'][^>]*>(.*?)</div>',
            ]
            
            best_content = ""
            max_text_length = 0
            
            for pattern in content_patterns:
                matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                for match in matches:
                    # Convert to text and measure
                    text = re.sub(r'<[^>]+>', ' ', match)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if len(text) > max_text_length:
                        max_text_length = len(text)
                        best_content = text
            
            # Fallback: extract all paragraph text
            if not best_content or len(best_content) < 200:
                paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html, re.DOTALL | re.IGNORECASE)
                paragraph_text = ' '.join(re.sub(r'<[^>]+>', ' ', p) for p in paragraphs)
                paragraph_text = re.sub(r'\s+', ' ', paragraph_text).strip()
                if len(paragraph_text) > len(best_content):
                    best_content = paragraph_text
            
            if best_content:
                metadata = {
                    "extraction_method": "readability",
                    "title": title,
                    "content_length": len(best_content),
                    "word_count": len(best_content.split())
                }
                return best_content, metadata
            
            return None, {"error": "no_content_found"}
            
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
        
        Expected input: Directory containing raw_html/ and crawl_metadata.json from scrape-site task
        Output: Vector-store ready content with intelligent chunking
        """
        from .utils import save_json_atomic
        
        # Get source directory (either from params or same as output)
        source_dir = pathlib.Path(params.get("source_dir", job_output_dir))
        if not source_dir.exists():
            raise ValueError(f"Source directory does not exist: {source_dir}")
        
        # Find HTML files
        html_dir = source_dir / "raw_html"
        if not html_dir.exists():
            raise ValueError(f"HTML directory not found: {html_dir}. Run scrape-site task first.")
        
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
