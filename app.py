import io
import json
import re
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import streamlit as st

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# ===============================
# Model / constants
# ===============================
SEARCH_URL = "https://www.pinterest.com/search/pins/?q={query}"
PIN_ID_RE = re.compile(r"/pin/(\d+)/?")

@dataclass
class PinRow:
    keyword: str
    pin_url: str
    pin_id: str
    title: str
    description: str
    image_url: str
    image_width: str
    image_height: str
    save_count: str
    comment_count: str
    pinner_username: str
    pinner_fullname: str
    pinner_profile_url: str
    board_name: str
    board_url: str
    outbound_link: str
    created_at: str

# ===============================
# Driver (with CDP performance logs)
# ===============================
def build_driver(headless: bool = False, window_size: str = "1400,1000") -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument(f"--window-size={window_size}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    )
    # enable performance logs so we can call Network.getResponseBody
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.enable", {})
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
        )
    except Exception:
        pass
    return driver

# ===============================
# Helpers
# ===============================
def safe_get_attr(elem, attr: str) -> str:
    try:
        return elem.get_attribute(attr) or ""
    except Exception:
        return ""

def extract_image_src(img_el) -> str:
    for c in [
        safe_get_attr(img_el, "src"),
        safe_get_attr(img_el, "data-src"),
        safe_get_attr(img_el, "data-lazy-src"),
        safe_get_attr(img_el, "srcset"),
    ]:
        if not c:
            continue
        if " " in c and "http" in c:
            first = c.split(",")[0].strip().split(" ")[0]
            return first
        return c
    return ""

NUM_RE = re.compile(r"([\d,.]+)\s*([kKmMbB]?)")  # 12.3K, 4,321, etc.

def parse_compact_number(s: str) -> str:
    s = (s or "").strip()
    m = NUM_RE.search(s)
    if not m:
        return ""
    num, suf = m.groups()
    try:
        base = float(num.replace(",", ""))
    except Exception:
        return ""
    mul = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}.get(suf.lower(), 1)
    return str(int(base * mul))

def retry_stale(fn, tries: int = 3, wait: float = 0.4):
    for _ in range(tries):
        try:
            return fn()
        except StaleElementReferenceException:
            time.sleep(wait)
    return fn()

def get_text_safe(driver, css: str) -> str:
    try:
        return driver.find_element(By.CSS_SELECTOR, css).text.strip()
    except NoSuchElementException:
        return ""

def attr_safe(driver, css: str, attr: str) -> str:
    try:
        return driver.find_element(By.CSS_SELECTOR, css).get_attribute(attr) or ""
    except NoSuchElementException:
        return ""

def find_text_contains(driver, word: str) -> str:
    word_low = word.lower()
    try:
        els = driver.find_elements(
            By.XPATH,
            f"//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{word_low}')]"
        )
        for el in els:
            t = el.text.strip()
            if t and word_low in t.lower():
                return t
    except Exception:
        pass
    return ""

# ===============================
# JSON hunters (DOM + CDP logs)
# ===============================
def find_first_pin_like_dict(obj: Any, pin_id_hint: Optional[str] = None) -> Optional[Dict]:
    """Fallback: walk any JSON structure and grab the first dict that looks like a pin."""
    stack = [obj]
    fallback = None
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if "id" in cur and (("images" in cur) or ("grid_title" in cur) or ("title" in cur)):
                if pin_id_hint and str(cur.get("id")) == str(pin_id_hint):
                    return cur
                if fallback is None:
                    fallback = cur
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return fallback

def _cdp_get_body_text(driver, request_id: str) -> str:
    """Fetch body; if base64-encoded, decode (brotli/gzip/deflate/plain)."""
    body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
    txt = body.get("body") or ""
    if body.get("base64Encoded"):
        import base64, zlib
        try:
            import brotli
        except Exception:
            brotli = None
        raw = base64.b64decode(txt)
        decoded = None
        if brotli:
            try:
                decoded = brotli.decompress(raw)
            except Exception:
                decoded = None
        if decoded is None:
            for try_decode in (
                lambda b: zlib.decompress(b, 16 + zlib.MAX_WBITS),
                lambda b: zlib.decompress(b),
                lambda b: b,
            ):
                try:
                    decoded = try_decode(raw)
                    break
                except Exception:
                    continue
        txt = (decoded or b"").decode("utf-8", "ignore")
    return txt.strip()

def _extract_closeup_from_json(obj: Any) -> Optional[Dict]:
    """
    Look for the Closeup JSON shape used in your coursework:
    requestParameters.name == "CloseupDetailQuery" → response.data.v3GetPinQuery.data
    """
    try:
        # Sometimes the JSON itself is exactly that object
        rp = obj.get("requestParameters", {})
        if isinstance(rp, dict) and rp.get("name") == "CloseupDetailQuery":
            data = obj.get("response", {}).get("data", {}).get("v3GetPinQuery", {}).get("data")
            if isinstance(data, dict):
                return data
    except Exception:
        pass

    # Otherwise, walk nested
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if "requestParameters" in cur and isinstance(cur["requestParameters"], dict) and cur["requestParameters"].get("name") == "CloseupDetailQuery":
                try:
                    data = cur.get("response", {}).get("data", {}).get("v3GetPinQuery", {}).get("data")
                    if isinstance(data, dict):
                        return data
                except Exception:
                    pass
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None

def hunt_json_in_scripts(driver) -> List[dict]:
    """Existing generic hunter + keep full JSON scripts when possible."""
    blobs = []
    scripts = driver.find_elements(By.TAG_NAME, "script")
    for s in scripts:
        try:
            t = s.get_attribute("innerHTML") or ""
            sid = s.get_attribute("id") or ""
        except Exception:
            t, sid = "", ""
        if sid == "__PWS_DATA__" and t:
            try:
                blobs.append(json.loads(t))
            except Exception:
                pass
        # NEW: many pages embed pure JSON blocks (not JS) that include CloseupDetailQuery
        if "requestParameters" in t and "CloseupDetailQuery" in t:
            try:
                blobs.append(json.loads(t))
            except Exception:
                # fall back to rough object snips if needed
                pass
        elif "{\"" in t and "}" in t:
            # rough object snips fallback
            for chunk in re.findall(r"\{.*?\}", t, flags=re.DOTALL):
                if any(k in chunk for k in ['"id"','"images"','"pinner"','"board"','"commentCount"','"saveCount"','"CloseupDetailQuery"']):
                    try:
                        blobs.append(json.loads(chunk))
                    except Exception:
                        pass

    html = driver.page_source or ""
    if "CloseupDetailQuery" in html and "requestParameters" in html:
        # try to capture whole <script> JSONs
        for m in re.finditer(r"<script[^>]*>(\{.*?\})</script>", html, re.DOTALL|re.IGNORECASE):
            block = m.group(1)
            if "CloseupDetailQuery" in block:
                try:
                    blobs.append(json.loads(block))
                except Exception:
                    pass
    return blobs

def hunt_json_in_cdp_logs(driver, pin_id: str) -> Optional[dict]:
    """
    Read Chrome 'performance' logs, pull Network.responseReceived events with JSON,
    then fetch bodies via Network.getResponseBody(requestId) and look for a pin dict.
    Also checks for CloseupDetailQuery responses.
    """
    try:
        logs = driver.get_log("performance")
    except Exception:
        return None

    for entry in reversed(logs[-600:]):
        try:
            msg = json.loads(entry.get("message", "{}"))
            m = msg.get("message", {})
        except Exception:
            continue
        if m.get("method") != "Network.responseReceived":
            continue

        params = m.get("params", {})
        res = params.get("response", {})
        mime = (res.get("mimeType") or "").lower()
        if "json" not in mime:
            continue

        req_id = params.get("requestId")
        if not req_id:
            continue

        # get body via CDP
        try:
            txt = _cdp_get_body_text(driver, req_id)
            if not (txt.startswith("{") or txt.startswith("[")):
                continue
            data = json.loads(txt)
        except Exception:
            continue

        # 1) Prefer CloseupDetailQuery data if present
        closeup = _extract_closeup_from_json(data)
        if isinstance(closeup, dict):
            return closeup

        # 2) Else fall back to generic pin-like dict
        cand = find_first_pin_like_dict(data, pin_id_hint=pin_id)
        if isinstance(cand, dict) and (not pin_id or str(cand.get("id")) == pin_id):
            return cand

    return None

# ===============================
# Search page primitives
# ===============================
def parse_pin_id(url: str) -> str:
    m = PIN_ID_RE.search(url)
    return m.group(1) if m else ""

def extract_pin_urls_from_view(driver, seen_urls: Set[str]) -> List[str]:
    urls: List[str] = []
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/pin/']")
    for a in anchors:
        href = safe_get_attr(a, "href")
        if not href or "/pin/" not in href:
            continue
        href = href.split("?")[0].split("#")[0]
        if href in seen_urls:
            continue
        urls.append(href)
    return urls

def scroll_results(driver, slow: float = 1.0, step_px: int = 1200):
    driver.execute_script(f"window.scrollBy(0, {step_px});")
    time.sleep(slow)

def collect_pin_urls_for_keyword(driver, keyword: str, limit_n: int, slow: float) -> List[str]:
    url = SEARCH_URL.format(query=keyword.replace(" ", "%20"))
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test-id='SearchPageContent']"))
        )
    except Exception:
        pass

    collected: List[str] = []
    seen: Set[str] = set()
    no_growth = 0

    while len(collected) < limit_n and no_growth < 6:
        batch = extract_pin_urls_from_view(driver, seen)
        prev = len(collected)
        for u in batch:
            if u not in seen:
                collected.append(u)
                seen.add(u)
                if len(collected) >= limit_n:
                    break
        if len(collected) == prev:
            no_growth += 1
        else:
            no_growth = 0
        scroll_results(driver, slow=slow, step_px=1200)

    return collected[:limit_n]

def text_from_meta(driver, name_or_prop: str, is_prop=False) -> str:
    sel = f"meta[{'property' if is_prop else 'name'}='{name_or_prop}']"
    try:
        el = driver.find_element(By.CSS_SELECTOR, sel)
        return safe_get_attr(el, "content")
    except NoSuchElementException:
        return ""

# ===============================
# Closeup field mapping helpers
# ===============================
def enrich_from_closeup_data(close: Dict[str, Any], fields: Dict[str, str]) -> Dict[str, str]:
    """
    Map the CloseupDetailQuery 'data' to our output fields.
    We’re conservative and only fill fields that are still empty.
    """
    out = dict(fields)  # copy

    # Names seen in your coursework JSON:
    #  - gridTitle / closeupUnifiedDescription / createdAt
    #  - pinner.username / pinner.full_name
    #  - images.{orig,564x,...}.url
    #  - aggregatedPinData.commentCount / saveCount
    #  - repinCount/shareCount (fallback)
    #  - board.{name, owner{username}, slug}
    #  - link (outbound)
    title = str(close.get("gridTitle") or close.get("title") or "")
    desc = str(close.get("closeupUnifiedDescription") or close.get("description") or "")
    created = str(close.get("createdAt") or "")

    if not out.get("title") and title:
        out["title"] = title
    if not out.get("description") and desc:
        out["description"] = desc
    if not out.get("created_at") and created:
        out["created_at"] = created

    # counts
    agg = close.get("aggregatedPinData") or close.get("aggregated_pin_data") or {}
    if isinstance(agg, dict):
        sc = agg.get("saveCount")
        cc = agg.get("commentCount")
        if not out.get("save_count") and sc is not None:
            out["save_count"] = str(sc)
        if not out.get("comment_count") and cc is not None:
            out["comment_count"] = str(cc)

    # sometimes only repinCount/shareCount present
    if not out.get("save_count"):
        rc = close.get("repinCount")
        if rc is not None:
            out["save_count"] = str(rc)

    # pinner
    pinner = close.get("pinner") or {}
    if isinstance(pinner, dict):
        if not out.get("pinner_username"):
            out["pinner_username"] = str(pinner.get("username") or "")
        if not out.get("pinner_fullname"):
            out["pinner_fullname"] = str(pinner.get("full_name") or pinner.get("fullName") or "")
        if not out.get("pinner_profile_url") and out.get("pinner_username"):
            out["pinner_profile_url"] = f"https://www.pinterest.com/{out['pinner_username']}/"

    # board
    board = close.get("board") or {}
    if isinstance(board, dict):
        if not out.get("board_name"):
            out["board_name"] = str(board.get("name") or "")
        if not out.get("board_url"):
            candidate = board.get("url") or ""
            if candidate:
                out["board_url"] = str(candidate)
            else:
                owner = board.get("owner") or {}
                owner_usr = owner.get("username") or ""
                slug = board.get("slug") or ""
                if owner_usr and slug:
                    out["board_url"] = f"https://www.pinterest.com/{owner_usr}/{slug}/"

    # outbound link
    if not out.get("outbound_link"):
        out["outbound_link"] = str(close.get("link") or close.get("dominant_link") or "")

    # images
    if not out.get("image_url"):
        images = close.get("images") or {}
        if isinstance(images, dict):
            for size_key in ["orig", "564x", "474x", "236x"]:
                d = images.get(size_key)
                if isinstance(d, dict):
                    out["image_url"] = str(d.get("url") or out.get("image_url") or "")
                    if not out.get("image_width") and d.get("width"):
                        out["image_width"] = str(d.get("width"))
                    if not out.get("image_height") and d.get("height"):
                        out["image_height"] = str(d.get("height"))
                    if out.get("image_url"):
                        break

    return out

def get_closeup_data_from_any(driver) -> Optional[Dict[str, Any]]:
    """
    Try CDP logs first (most reliable), then script tags / HTML.
    """
    # CDP
    close = hunt_json_in_cdp_logs(driver, pin_id="")
    if isinstance(close, dict):
        return close

    # Scripts & HTML
    blobs = hunt_json_in_scripts(driver)
    for b in blobs:
        close = _extract_closeup_from_json(b)
        if isinstance(close, dict):
            return close
    return None

# ===============================
# Scrape a pin (CDP + Closeup + fallbacks)
# ===============================
def scrape_pin_detail(driver, pin_url: str, keyword: str, wait_sec: float = 12.0) -> PinRow:
    driver.get(pin_url)
    try:
        WebDriverWait(driver, wait_sec).until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "script#__PWS_DATA__")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test-id='pin']")),
                EC.presence_of_element_located((By.TAG_NAME, "img")),
            )
        )
    except Exception:
        pass

    pin_id = parse_pin_id(pin_url)

    # 1) __PWS_DATA__ or inline JSON (generic)
    pin_dict: Dict[str, Any] = {}
    try:
        data_el = driver.find_element(By.CSS_SELECTOR, "script#__PWS_DATA__")
        raw_json = data_el.get_attribute("innerHTML") or ""
        if raw_json:
            data = json.loads(raw_json)
            cand = find_first_pin_like_dict(data, pin_id_hint=pin_id)
            if isinstance(cand, dict):
                pin_dict = cand
    except Exception:
        pass

    # 2) CDP bodies (generic or Closeup)
    if not pin_dict or not any(k in pin_dict for k in ("pinner", "board", "images", "counts", "aggregatedStats", "saveCount")):
        net_cand = hunt_json_in_cdp_logs(driver, pin_id)
        if isinstance(net_cand, dict):
            pin_dict = net_cand

    # 3) Script/HTML brute hunt
    if not pin_dict or not any(k in pin_dict for k in ("pinner", "board", "images")):
        blobs = hunt_json_in_scripts(driver)
        cand = None
        for b in blobs:
            # prefer Closeup if available
            close = _extract_closeup_from_json(b)
            if isinstance(close, dict):
                cand = close
                break
            c2 = find_first_pin_like_dict(b, pin_id_hint=pin_id)
            if isinstance(c2, dict):
                cand = c2
                break
        if isinstance(cand, dict):
            pin_dict = cand

    # 4) Base fields from whatever pin_dict we have
    title = str(pin_dict.get("grid_title") or pin_dict.get("gridTitle") or pin_dict.get("title") or "")
    description = str(pin_dict.get("description") or pin_dict.get("closeupUnifiedDescription") or "")
    created_at = str(pin_dict.get("created_at") or pin_dict.get("createdAt") or "")

    image_url = ""
    image_width = ""
    image_height = ""
    images = pin_dict.get("images") if isinstance(pin_dict, dict) else None
    if isinstance(images, dict):
        for size_key in ["orig", "564x", "474x", "236x"]:
            d = images.get(size_key)
            if isinstance(d, dict):
                image_url = str(d.get("url") or image_url)
                image_width = str(d.get("width") or image_width)
                image_height = str(d.get("height") or image_height)
                if image_url:
                    break

    save_count = ""
    comment_count = ""
    for key in ["saveCount", "aggregated_pin_data", "aggregatedPinData", "aggregatedStats", "counts", "stats"]:
        d = pin_dict.get(key) if isinstance(pin_dict, dict) else None
        if isinstance(d, dict):
            save_count = save_count or str(d.get("saveCount") or d.get("saves") or "")
            comment_count = comment_count or str(d.get("commentCount") or d.get("comments") or "")
    save_count = save_count or str(pin_dict.get("saveCount") or pin_dict.get("repinCount") or "")
    comment_count = comment_count or str(pin_dict.get("commentCount") or "")

    outbound_link = str(pin_dict.get("link") or pin_dict.get("dominant_link") or "")

    pinner_username = pinner_fullname = pinner_profile_url = ""
    pinner = pin_dict.get("pinner") if isinstance(pin_dict, dict) else None
    if isinstance(pinner, dict):
        pinner_username = str(pinner.get("username") or "")
        pinner_fullname = str(pinner.get("full_name") or pinner.get("fullName") or "")
        if pinner_username:
            pinner_profile_url = f"https://www.pinterest.com/{pinner_username}/"

    board_name = board_url = ""
    board = pin_dict.get("board") if isinstance(pin_dict, dict) else None
    if isinstance(board, dict):
        board_name = str(board.get("name") or "")
        board_url = str(board.get("url") or "")
        if not board_url:
            owner = board.get("owner") if isinstance(board.get("owner"), dict) else {}
            owner_usr = owner.get("username") or ""
            slug = board.get("slug") or ""
            if owner_usr and slug:
                board_url = f"https://www.pinterest.com/{owner_usr}/{slug}/"

    # 5) Try to enrich using CloseupDetailQuery explicitly (even if we already had some data)
    closeup = get_closeup_data_from_any(driver)
    if isinstance(closeup, dict):
        fields_now = {
            "title": title, "description": description, "created_at": created_at,
            "image_url": image_url, "image_width": image_width, "image_height": image_height,
            "save_count": save_count, "comment_count": comment_count,
            "pinner_username": pinner_username, "pinner_fullname": pinner_fullname, "pinner_profile_url": pinner_profile_url,
            "board_name": board_name, "board_url": board_url, "outbound_link": outbound_link
        }
        enriched = enrich_from_closeup_data(closeup, fields_now)
        title = enriched["title"]
        description = enriched["description"]
        created_at = enriched["created_at"]
        image_url = enriched["image_url"]
        image_width = enriched["image_width"]
        image_height = enriched["image_height"]
        save_count = enriched["save_count"]
        comment_count = enriched["comment_count"]
        pinner_username = enriched["pinner_username"]
        pinner_fullname = enriched["pinner_fullname"]
        pinner_profile_url = enriched["pinner_profile_url"]
        board_name = enriched["board_name"]
        board_url = enriched["board_url"]
        outbound_link = enriched["outbound_link"]

    # 6) DOM/XPath fallbacks for any remaining blanks
    if not title:
        title = text_from_meta(driver, "og:title", is_prop=True) or text_from_meta(driver, "twitter:title")
        if not title:
            title = retry_stale(lambda: get_text_safe(driver, "h1")) or retry_stale(lambda: get_text_safe(driver, "h2"))

    if not description:
        description = text_from_meta(driver, "og:description", is_prop=True) or text_from_meta(driver, "description")

    if not image_url:
        image_url = text_from_meta(driver, "og:image", is_prop=True)
    if (not image_url) or (not image_width) or (not image_height):
        try:
            img = retry_stale(lambda: driver.find_element(By.CSS_SELECTOR, "img"))
            if not image_url:
                image_url = extract_image_src(img)
            if not image_width:
                image_width = safe_get_attr(img, "width")
            if not image_height:
                image_height = safe_get_attr(img, "height")
        except Exception:
            pass

    if not save_count:
        t1 = retry_stale(lambda: get_text_safe(driver, "[data-test-id='socialCount']"))
        t2 = retry_stale(lambda: get_text_safe(driver, "div[class*='SocialCounts'], span[class*='SocialCounts']"))
        t3 = find_text_contains(driver, "save")
        for t in [t1, t2, t3]:
            if not t:
                continue
            m = re.search(r"([\d.,KMBkmb]+)\s*saves?", t)
            if m:
                save_count = parse_compact_number(m.group(1))
                break

    if not comment_count:
        t1 = retry_stale(lambda: get_text_safe(driver, "[data-test-id='commentsCount']"))
        t2 = find_text_contains(driver, "comment")
        for t in [t1, t2]:
            if not t:
                continue
            m = re.search(r"([\d.,KMBkmb]+)\s*comments?", t)
            if m:
                comment_count = parse_compact_number(m.group(1))
                break

    if not pinner_username or not pinner_profile_url:
        try:
            links = retry_stale(lambda: driver.find_elements(By.CSS_SELECTOR, "a[href^='https://www.pinterest.com/'], a[href^='/']"))
            for a in links:
                href = safe_get_attr(a, "href")
                if not href or "/pin/" in href:
                    continue
                if href.startswith("/"):
                    href = "https://www.pinterest.com" + href
                path = href.replace("https://www.pinterest.com/", "")
                if path and "/" in path and path.count("/") == 1:
                    maybe_user = path.strip("/")
                    if maybe_user and "ideas" not in maybe_user.lower():
                        pinner_username = pinner_username or maybe_user
                        pinner_profile_url = pinner_profile_url or href
                        if not pinner_fullname:
                            pinner_fullname = a.text.strip() or pinner_fullname
                        break
        except Exception:
            pass

    if not board_url:
        try:
            links = retry_stale(lambda: driver.find_elements(By.CSS_SELECTOR, "a[href*='pinterest.com/']"))
            for a in links:
                href = safe_get_attr(a, "href") or ""
                if "/pin/" in href:
                    continue
                path = href.replace("https://www.pinterest.com/", "").strip("/")
                if path.count("/") == 2:  # user/board/
                    board_url = href
                    if not board_name:
                        board_name = a.text.strip()
                    break
        except Exception:
            pass

    if not outbound_link:
        outbound_link = retry_stale(lambda: attr_safe(driver, "a[data-test-id='PinActionBar-visitButton']", "href"))
    if not outbound_link:
        try:
            links = retry_stale(lambda: driver.find_elements(By.CSS_SELECTOR, "a[target='_blank']"))
            for a in links:
                rel = a.get_attribute("rel") or ""
                href = a.get_attribute("href") or ""
                if not href or href.startswith("https://www.pinterest.com/"):
                    continue
                if any(flag in rel for flag in ["nofollow", "noopener", "noreferrer"]):
                    outbound_link = href
                    break
        except Exception:
            pass

    if not created_at:
        try:
            times = driver.find_elements(By.TAG_NAME, "time")
            for t in times:
                dt = safe_get_attr(t, "datetime")
                if dt:
                    created_at = dt
                    break
        except Exception:
            pass

    if save_count and not save_count.isdigit():
        save_count = parse_compact_number(save_count)
    if comment_count and not comment_count.isdigit():
        comment_count = parse_compact_number(comment_count)

    return PinRow(
        keyword=keyword,
        pin_url=pin_url,
        pin_id=pin_id,
        title=title or "",
        description=description or "",
        image_url=image_url or "",
        image_width=str(image_width or ""),
        image_height=str(image_height or ""),
        save_count=str(save_count or ""),
        comment_count=str(comment_count or ""),
        pinner_username=pinner_username or "",
        pinner_fullname=pinner_fullname or "",
        pinner_profile_url=pinner_profile_url or "",
        board_name=board_name or "",
        board_url=board_url or "",
        outbound_link=outbound_link or "",
        created_at=created_at or "",
    )

# ===============================
# Streamlit UI
# ===============================
st.set_page_config(page_title="Pinterest Scraper UI", layout="wide")

if "kw_df" not in st.session_state:
    st.session_state.kw_df = pd.DataFrame([{"keyword": "technology", "limit": 40}])

st.title("Pinterest Scraper UI (Selenium + CDP + Closeup JSON)")

with st.sidebar:
    st.header("Controls")
    headful = st.checkbox("Show browser window", value=True)
    slow_seconds = st.number_input("Scroll delay (seconds)", min_value=0.1, max_value=5.0, value=1.2, step=0.1)
    default_limit = st.number_input("Default limit if missing", min_value=1, max_value=1000, value=40, step=1)

    st.markdown("### Sample CSV")
    sample = "keyword,limit\ntechnology,60\ninterior design,40\nsmart home,50\n"
    st.download_button("Download sample.csv", data=sample, file_name="sample_keywords.csv", mime="text/csv")

    st.markdown("### Import CSV")
    uploaded = st.file_uploader("Upload keywords CSV", type=["csv"])
    if uploaded:
        try:
            df_up = pd.read_csv(uploaded)
            if "keyword" not in df_up.columns:
                st.error("CSV must include a 'keyword' column")
            else:
                if "limit" not in df_up.columns:
                    df_up["limit"] = default_limit
                df_up["keyword"] = df_up["keyword"].astype(str).str.strip()
                df_up["limit"] = pd.to_numeric(df_up["limit"], errors="coerce").fillna(default_limit).astype(int)
                st.session_state.kw_df = df_up[["keyword", "limit"]]
                st.success(f"Loaded {len(st.session_state.kw_df)} rows from CSV")
        except Exception as e:
            st.error(f"Failed to read CSV: {e}")

st.subheader("Keywords and limits")
st.caption("Edit directly, or import from CSV in the sidebar")
kw_df = st.data_editor(
    st.session_state.kw_df,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "keyword": st.column_config.TextColumn(required=True),
        "limit": st.column_config.NumberColumn(required=True, min_value=1, max_value=10000, step=1),
    },
    key="kw_editor",
)
st.session_state.kw_df = kw_df

col_a, col_b, _ = st.columns([1,1,2])
with col_a:
    if st.button("Add row"):
        st.session_state.kw_df = pd.concat(
            [st.session_state.kw_df, pd.DataFrame([{"keyword": "", "limit": default_limit}])],
            ignore_index=True
        )
with col_b:
    if st.button("Clear"):
        st.session_state.kw_df = pd.DataFrame(columns=["keyword", "limit"])

st.divider()

start = st.button("Start scraper")

out_df = pd.DataFrame()

if start:
    work_df = st.session_state.kw_df.copy()
    work_df["keyword"] = work_df["keyword"].astype(str).str.strip()
    work_df["limit"] = pd.to_numeric(work_df["limit"], errors="coerce").fillna(default_limit).astype(int)
    work_df = work_df[work_df["keyword"] != ""]
    if work_df.empty:
        st.error("Please provide at least one keyword.")
    else:
        progress = st.progress(0, text="Preparing Chrome...")
        status = st.empty()
        rows: List[PinRow] = []
        try:
            driver = build_driver(headless=not headful)
            total_kw = len(work_df)
            kw_index = 0

            for _, r in work_df.iterrows():
                kw_index += 1
                kw = r["keyword"]
                lim = int(r["limit"]) if r["limit"] else int(default_limit)
                status.write(f"Searching '{kw}' (limit {lim})")
                pin_urls = collect_pin_urls_for_keyword(driver, kw, lim, slow=float(slow_seconds))

                for i, pu in enumerate(pin_urls, 1):
                    status.write(f"[{kw_index}/{total_kw}] {kw}: pin {i}/{len(pin_urls)}")
                    try:
                        pr = scrape_pin_detail(driver, pu, kw, wait_sec=12.0)
                        rows.append(pr)
                    except Exception as e:
                        st.write(f"Failed pin: {pu} ({e})")

                    done_ratio = ((kw_index - 1) + i / max(1, len(pin_urls))) / max(1, total_kw)
                    progress.progress(min(1.0, done_ratio))

            try:
                driver.quit()
            except Exception:
                pass

            progress.progress(1.0)
            status.write("Done")

            out_df = pd.DataFrame([asdict(r) for r in rows])
            if not out_df.empty:
                out_df.drop_duplicates(subset=["pin_url"], inplace=True)

            st.success(f"Scraped {len(out_df)} pins")
        except Exception as e:
            st.error(f"Run failed: {e}")

if not out_df.empty:
    st.subheader("Preview")
    st.dataframe(out_df.head(100), use_container_width=True)

    csv_buf = io.StringIO()
    out_df.to_csv(csv_buf, index=False, encoding="utf-8")
    st.download_button(
        "Download results CSV",
        data=csv_buf.getvalue(),
        file_name="pinterest_results.csv",
        mime="text/csv",
    )
