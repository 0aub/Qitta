# tasks.py — Saudi Open Data scraper (robust v1 downloads, strict validation, correct filenames)
# 2025-08-10

from __future__ import annotations
import asyncio, hashlib, json, logging, mimetypes, os, pathlib, random, re, urllib.parse
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx
from playwright.async_api import Browser, BrowserContext, Page, TimeoutError

# ───────────────── registry ─────────────────
task_registry: Dict[str, Callable[..., Awaitable[Dict[str, Any]]]] = {}
def register_task(name: str):
    def deco(fn): task_registry[name] = fn; return fn
    return deco

# ──────────────── utils ────────────────────
UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")

def _log(lg: logging.Logger, lvl: str, msg: str):
    getattr(lg, lvl)(msg.replace("\n", " ")[:1000])

def _safe_name(s: str) -> str:
    s = (s or "").strip().replace(os.sep, "_")
    s = re.sub(r"[^\w\-.]+", "_", s).strip("._")
    return s[:180] or "file"

def _classify_payload(head: bytes, content_type: str | None) -> str:
    if head.startswith(b"PK\x03\x04"):  # zip container (xlsx)
        return "xlsx_zip"
    if head.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):  # OLE CF (xls)
        return "xls_ole"
    lower = head.lower()
    if lower.startswith(b"<!doctype html") or lower.startswith(b"<html") \
       or b"/tspd/" in lower or b"<apm_do_not_touch>" in lower:
        return "html_interstitial"

    ct = (content_type or "").split(";")[0].strip().lower()
    text = head.decode("utf-8", "ignore")
    if ct in {"application/json", "application/ld+json"} or text.lstrip().startswith(("{","[")):
        return "json_text"
    if ct in {"application/xml", "text/xml"} or text.lstrip().startswith(("<?xml", "<root", "<dataset")):
        return "xml_text"
    if ct in {"text/csv", "application/csv", "text/plain"}:
        return "csv_text" if (("," in text or ";" in text or "\t" in text) and ("\n" in text)) else "text_plain"
    if (("," in text or ";" in text or "\t" in text) and ("\n" in text)):
        return "csv_text"
    return "binary/unknown"

def _ext_from_ct(ct: str | None) -> str:
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

def _ext_from_format(fmt: Optional[str]) -> str:
    f = (fmt or "").strip().lower()
    mapping = {
        "xlsx": ".xlsx",
        "xls": ".xls",
        "csv": ".csv",
        "json": ".json",
        "xml": ".xml",
        "zip": ".zip"
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

def _derive_name(resource: dict, link: str, headers: Dict[str,str], content_type: Optional[str]) -> str:
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
        return _safe_name(from_disp)

    # 2) From URL path (works for odp-public links, not for /v1/.../download)
    path_seg = urllib.parse.urlsplit(link).path.rsplit("/", 1)[-1]
    if path_seg and "." in path_seg:
        return _safe_name(path_seg)

    # 3) From resource + format
    fmt_ext = _ext_from_format(resource.get("format"))
    base = (resource.get("name") or resource.get("titleEn") or resource.get("titleAr") or "").strip()
    if base:
        base = _safe_name(base.replace(" ", "_"))
        return _safe_name(base + (fmt_ext or ""))

    # 4) resourceID + format
    rid = resource.get("resourceID") or resource.get("id") or ""
    if rid:
        return _safe_name(rid + (fmt_ext or ""))

    # 5) MIME (avoid .bin)
    ct_ext = _ext_from_ct(content_type)
    if ct_ext and ct_ext != ".bin":
        return "file" + ct_ext

    # last resort
    return "file"

def _validate_and_save(dst_dir: pathlib.Path, suggested: str, body: bytes, content_type: str | None,
                       resource: Optional[dict] = None) -> Tuple[bool, str, Dict[str, Any]]:
    """Return (ok, filename, meta). Only ok=True when bytes aren't HTML interstitial and look like the format."""
    head = body[:2048]
    kind = _classify_payload(head, content_type)
    if kind == "html_interstitial":
        (dst_dir / "blocked.html").write_bytes(body)
        return False, "", {"reason": "html_interstitial"}

    # Ensure filename has a base + reasonable extension
    name = _safe_name(suggested)
    root, ext = os.path.splitext(name)

    if not root:
        # Guarantee non-empty base using resource name or id
        if resource:
            root = _safe_name((resource.get("name") or resource.get("titleEn") or resource.get("titleAr") or resource.get("resourceID") or resource.get("id") or "file").replace(" ", "_"))
        else:
            root = "file"

    # pick extension preference: FORMAT > Content-Type (if specific) > kind
    fmt_ext = _ext_from_format(resource.get("format") if resource else None)
    ct_ext = _ext_from_ct(content_type)
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

    final_name = _safe_name(root + ext)

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

def _dl_headers(base: dict) -> dict:
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
    if base.get("referer"): h["referer"] = base["referer"]
    return h

# ─────────── playwright context helpers ───────────
async def _new_ctx(browser: Browser, head: dict, proxy: Optional[str]) -> BrowserContext:
    args = {
        "user_agent": head["user-agent"],
        "accept_downloads": True,
        "locale": "en-US",
    }
    if proxy: args["proxy"] = {"server": proxy}
    ctx = await browser.new_context(**args)
    await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
    return ctx

async def _warm_resources(ctx: BrowserContext, dsid: str, log: logging.Logger):
    url = f"https://open.data.gov.sa/en/datasets/view/{dsid}/resources"
    pg = await ctx.new_page()
    try:
        await pg.goto(url, timeout=90_000, wait_until="domcontentloaded")
        await pg.wait_for_timeout(600)
        _log(log, "info", f"{dsid}: warmed resources page")
    except Exception as e:
        _log(log, "warning", f"{dsid}: warm failed – {e}")
    finally:
        await pg.close()

# ─────────── metadata (resources) ───────────
async def _resources_via_api(ctx: BrowserContext, dsid: str, head: dict, log: logging.Logger) -> List[dict]:
    url = f"https://open.data.gov.sa/data/api/datasets/resources?version=-1&dataset={dsid}"
    try:
        r = await ctx.request.get(url, headers={
            "accept":"application/json",
            "x-requested-with":"XMLHttpRequest",
            "x-security-request":"required",
        }, timeout=45_000)
        if r.status != 200:
            raise RuntimeError(f"http_{r.status}")
        body = await r.body()
        kind = _classify_payload(body[:2048], r.headers.get("content-type"))
        if kind == "html_interstitial":
            raise RuntimeError("html_interstitial")
        j = json.loads(body.decode("utf-8", "ignore"))
        if isinstance(j, dict) and j.get("resources"):
            _log(log, "info", f"{dsid}: resources via API datasets/resources?version=-1&dataset={dsid}")
            return j["resources"]
    except Exception as e:
        _log(log, "warning", f"{dsid}: resources API failed – {e}")
    try:
        url2 = f"https://open.data.gov.sa/api/datasets/{dsid}"
        r2 = await ctx.request.get(url2, headers={"accept":"application/json"}, timeout=45_000)
        if r2.status == 200:
            body2 = await r2.body()
            kind2 = _classify_payload(body2[:2048], r2.headers.get("content-type"))
            if kind2 != "html_interstitial":
                j2 = json.loads(body2.decode("utf-8", "ignore"))
                if isinstance(j2, dict) and j2.get("resources"):
                    _log(log, "info", f"{dsid}: resources via API /api/datasets/{dsid}")
                    return j2["resources"]
    except Exception:
        pass
    raise RuntimeError("metadata_blocked")

# ─────────── downloads ───────────
async def _ctx_v1_download(ctx: BrowserContext, dsid: str, rid: str, resource: dict,
                           out_dir: pathlib.Path, head: dict, log: logging.Logger) -> Dict[str, Any]:
    url = f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download"
    try:
        r = await ctx.request.get(url, headers=_dl_headers(head), timeout=90_000)
        if r.status != 200:
            return {"stage":"ctx(v1)","status":"error","reason":f"http_{r.status}"}
        body = await r.body()
        suggested = _derive_name(resource, url, r.headers, r.headers.get("content-type"))
        ok, fname, meta = _validate_and_save(out_dir, suggested, body, r.headers.get("content-type"), resource)
        if not ok:
            return {"stage":"ctx(v1)","status":"error", **meta}
        return {"stage":"ctx(v1)","status":"ok","file":fname, **meta}
    except TimeoutError:
        return {"stage":"ctx(v1)","status":"error","reason":"timeout"}
    except Exception as e:
        return {"stage":"ctx(v1)","status":"error","reason":str(e)}

async def _httpx_download(url: str, resource: dict, out_dir: pathlib.Path, head: dict) -> Dict[str, Any]:
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=90) as cl:
            r = await cl.get(url, headers=_dl_headers(head))
        if r.status_code != 200:
            return {"stage":"httpx","status":"error","reason":f"http_{r.status_code}"}
        suggested = _derive_name(resource, url, r.headers, r.headers.get("content-type"))
        ok, fname, meta = _validate_and_save(out_dir, suggested, r.content, r.headers.get("content-type"), resource)
        if not ok:
            return {"stage":"httpx","status":"error", **meta}
        return {"stage":"httpx","status":"ok","file":fname, **meta}
    except Exception as e:
        return {"stage":"httpx","status":"error","reason":str(e)}

async def _tab_nav(ctx: BrowserContext, url: str, resource: dict, out_dir: pathlib.Path, head: dict) -> Dict[str, Any]:
    pg = await ctx.new_page()
    try:
        await pg.set_extra_http_headers(_dl_headers(head))
        async with pg.expect_download() as info:
            await pg.goto(url, timeout=90_000)
        dl = await info.value
        # suggested from Playwright, else our derivation
        suggested = dl.suggested_filename or _derive_name(resource, url, {}, None)
        tmp = out_dir / (suggested + ".part")
        await dl.save_as(tmp)
        body = tmp.read_bytes() if tmp.exists() else b""
        ok, fname, meta = _validate_and_save(out_dir, suggested, body, None, resource)
        tmp.unlink(missing_ok=True)
        if not ok:
            return {"stage":"tab-nav","status":"error", **meta}
        return {"stage":"tab-nav","status":"ok","file":fname, **meta}
    except TimeoutError:
        return {"stage":"tab-nav","status":"error","reason":"timeout"}
    except Exception as e:
        return {"stage":"tab-nav","status":"error","reason":str(e)}
    finally:
        await pg.close()

async def _download_resource(ctx: BrowserContext, dsid: str, resource: dict,
                             out_dir: pathlib.Path, head: dict, log: logging.Logger) -> Dict[str, Any]:
    link = (resource.get("downloadUrl") or resource.get("url") or "").strip()
    if link and not link.lower().startswith("http"):
        link = "https://open.data.gov.sa/data/" + link.lstrip("/")
    rid = resource.get("resourceID") or resource.get("id") or ""
    attempts: List[Dict[str, Any]] = []

    # 0) v1 official endpoint
    if dsid and rid:
        r = await _ctx_v1_download(ctx, dsid, rid, resource, out_dir, head, log)
        if r["status"] == "ok":
            return {"status":"ok","via":r["stage"],"url":f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download","path":r["file"],"size":r.get("size"),"sha256":r.get("sha256")}
        attempts.append(r)

        r2 = await _httpx_download(f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download", resource, out_dir, head)
        if r2["status"] == "ok":
            return {"status":"ok","via":"httpx(v1)","url":f"https://open.data.gov.sa/data/api/v1/datasets/{dsid}/resources/{rid}/download","path":r2["file"],"size":r2.get("size"),"sha256":r2.get("sha256")}
        attempts.append(r2)

    # 1) odp-public link
    if link:
        r3 = await _httpx_download(link, resource, out_dir, head)
        if r3["status"] == "ok":
            return {"status":"ok","via":"httpx","url":link,"path":r3["file"],"size":r3.get("size"),"sha256":r3.get("sha256")}
        attempts.append(r3)

        r4 = await _tab_nav(ctx, link, resource, out_dir, head)
        if r4["status"] == "ok":
            return {"status":"ok","via":"tab-nav","url":link,"path":r4["file"],"size":r4.get("size"),"sha256":r4.get("sha256")}
        attempts.append(r4)

    return {"status":"error","url": link or f"/v1/{dsid}/{rid}", "reason":"all_attempts_failed", "attempts":attempts}

# ─────────── one dataset ───────────
async def _run_dataset(browser: Browser, dsid: str, head: dict, out_root: pathlib.Path, logger: logging.Logger) -> Dict[str, Any]:
    out_dir = out_root / dsid
    (out_dir / "downloads").mkdir(parents=True, exist_ok=True)

    ctx = await _new_ctx(browser, head, proxy=None)
    try:
        await _warm_resources(ctx, dsid, logger)
        resources = await _resources_via_api(ctx, dsid, head, logger)

        for r in resources:
            lk = r.get("downloadUrl") or r.get("url") or ""
            if lk and not lk.lower().startswith("http"):
                lk = "https://open.data.gov.sa/data/" + lk.lstrip("/")
            p = urllib.parse.urlsplit(lk)
            r["downloadUrl"] = urllib.parse.urlunsplit(
                (p.scheme, p.netloc, urllib.parse.quote(p.path, safe="/"), p.query, "")
            )

        (out_dir / "resources.json").write_text(json.dumps(resources, indent=2, ensure_ascii=False), "utf-8")

        results: List[Dict[str, Any]] = []
        for r in resources:
            item = await _download_resource(ctx, dsid, r, out_dir / "downloads", head, logger.getChild("dl"))
            item["resource_name"] = r.get("name") or r.get("titleEn") or r.get("titleAr")
            results.append(item)
            await asyncio.sleep(random.uniform(0.5, 1.2))

        (out_dir / "downloads.json").write_text(json.dumps(results, indent=2, ensure_ascii=False), "utf-8")

        ok = sum(1 for x in results if x["status"] == "ok")
        fail = sum(1 for x in results if x["status"] == "error")
        # Try to include dataset title if API response carried it
        ds_title = None
        for r in resources:
            t = r.get("datasetTitle") or r.get("dataset_title") or r.get("titleEn")
            if t: ds_title = t; break

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

# ─────────── list datasets for publisher ───────────
async def _list_datasets_for_publisher(browser: Browser, pub_id: str, head: dict, logger: logging.Logger) -> List[Dict[str,str]]:
    async with httpx.AsyncClient(follow_redirects=True, timeout=45) as cl:
        url = f"https://open.data.gov.sa/data/api/organizations?version=-1&organization={pub_id}"
        r = await cl.get(url, headers={"accept":"application/json"})
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

# ═════════════════ TASK: saudi-open-data ═════════════════
@register_task("saudi-open-data")
async def saudi_open_data(*, browser: Browser, params: Dict[str, Any],
                          job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    BASE = "https://open.data.gov.sa"
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
        "referer": BASE + "/",
    }

    out_root = pathlib.Path(job_output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    if dsid := params.get("dataset_id"):
        _log(logger, "info", f"{dsid}: single dataset flow")
        res = await _run_dataset(browser, dsid, HEAD, out_root, logger)
        return res

    pub_id = params["publisher_id"]
    _log(logger, "info", f"{pub_id}: list via org API")
    all_items = await _list_datasets_for_publisher(browser, pub_id, HEAD, logger)

    if "dataset_range" in params:
        a, b = params["dataset_range"]
        all_items = all_items[int(a): int(b)+1]
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
            dsres = await _run_dataset(browser, dsid, HEAD, out_root, logger)
        except Exception as e:
            _log(logger, "warning", f"{dsid}: error – {e}")
            dsres = {
                "dataset_id": dsid, "dataset_title": title,
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

    return {
        "publisher_id": pub_id,
        "total_datasets": len(all_items),
        "datasets_complete": datasets_complete,
        "datasets_partial": datasets_partial,
        "datasets_failed": datasets_failed,
        "total_files_ok": total_files_ok,
        "total_files_failed": total_files_err,
        "details_file": "publisher_results.json",
    }

# ═══════════════ generic website crawler (unchanged) ════════════════
@register_task("scrape-site")
async def scrape_site(*, browser: Browser, params: Dict[str, Any],
                      job_output_dir: str, logger: logging.Logger) -> Dict[str, Any]:
    import re
    start = params["url"]
    dom = urllib.parse.urlparse(start).netloc
    lim = None if str(params.get("max_pages", "")).strip() in {"", "0", "-1"} else int(params["max_pages"])
    q, seen, saved = [start], set(), []
    out = pathlib.Path(job_output_dir)
    out.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as cl:
        while q and (lim is None or len(seen) < lim):
            url = q.pop(0); seen.add(url)
            try:
                r = await cl.get(url, headers={"User-Agent": params.get("user_agent", "Mozilla/5.0")})
            except Exception as e:
                logger.error(f"{url}: {e}"); continue
            if r.status_code != 200: continue
            html = r.text; saved.append(url)
            (out / f"{urllib.parse.quote(url, safe='')}.html").write_text(html, "utf-8")
            for h in re.findall(r'href=["\\\'](.*?)["\\\']', html, re.I):
                if not h: continue
                full = h if h.startswith("http") else urllib.parse.urljoin(url, h)
                p = urllib.parse.urlparse(full)
                if p.netloc != dom: continue
                full = urllib.parse.urlunparse(p._replace(fragment=""))
                if full not in seen and full not in q: q.append(full)
            await asyncio.sleep(random.uniform(1, 2))
    (out / "urls.json").write_text(json.dumps(saved, indent=2, ensure_ascii=False), "utf-8")
    return {"url": start, "pages_scraped": len(saved), "unbounded": lim is None, "urls_file": "urls.json"}
