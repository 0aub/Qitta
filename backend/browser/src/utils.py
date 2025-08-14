from __future__ import annotations
import os
import re

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
