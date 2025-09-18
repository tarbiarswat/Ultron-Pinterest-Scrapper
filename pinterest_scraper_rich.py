import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Any

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


SEARCH_URL = "https://www.pinterest.com/search/pins/?q={query}"


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


def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,1000")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
    )
    return driver


def read_keywords_csv(path: str, default_limit: int) -> List[Dict[str, int]]:
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "keyword" not in reader.fieldnames:
            raise ValueError("CSV must have a 'keyword' column")
        for r in reader:
            kw = (r.get("keyword") or "").strip()
            if not kw:
                continue
            lim_raw = (r.get("limit") or "").strip()
            try:
                limit_val = int(lim_raw) if lim_raw else default_limit
            except ValueError:
                limit_val = default_limit
            rows.append({"keyword": kw, "limit": limit_val})
    return rows


def safe_get_attr(elem, attr: str) -> str:
    try:
        return elem.get_attribute(attr) or ""
    except Exception:
        return ""


def extract_image_src(img_el) -> str:
    candidates = [
        safe_get_attr(img_el, "src"),
        safe_get_attr(img_el, "data-src"),
        safe_get_attr(img_el, "data-lazy-src"),
        safe_get_attr(img_el, "srcset"),
    ]
    for c in candidates:
        if not c:
            continue
        if " " in c and "http" in c:
            first = c.split(",")[0].strip().split(" ")[0]
            return first
        return c
    return ""


def extract_pin_items_from_view(driver, seen_urls: Set[str]) -> List[str]:
    """Return pin URLs currently visible on the search results page."""
    urls: List[str] = []
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/pin/']")
    for a in anchors:
        href = safe_get_attr(a, "href")
        if not href or "/pin/" not in href:
            continue
        # Normalize pin URL (remove query params/fragments)
        href = href.split("?")[0].split("#")[0]
        if href in seen_urls:
            continue
        urls.append(href)
    return urls


def scroll_results(driver, slow: float = 0.7, step_px: int = 1200):
    driver.execute_script(f"window.scrollBy(0, {step_px});")
    time.sleep(slow)


PIN_ID_RE = re.compile(r"/pin/(\d+)/?")


def parse_pin_id(url: str) -> str:
    m = PIN_ID_RE.search(url)
    return m.group(1) if m else ""


def find_first_pin_like_dict(obj: Any, pin_id_hint: Optional[str] = None) -> Optional[Dict]:
    """
    Walk a nested dict/list JSON to find a dict that looks like a Pin payload.
    We prefer matches where id == pin_id_hint if provided.
    """
    stack = [obj]
    fallback = None
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            # Pinterest Pin dicts often contain keys like: id, title/grid_title, images, saveCount/commentCount, link
            if "id" in cur and (("images" in cur) or ("grid_title" in cur) or ("title" in cur)):
                # Prefer the exact id match
                if pin_id_hint and str(cur.get("id")) == str(pin_id_hint):
                    return cur
                fallback = fallback or cur
            for v in cur.values():
                stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return fallback


def text_from_meta(driver, name_or_prop: str, is_prop=False) -> str:
    sel = f"meta[{'property' if is_prop else 'name'}='{name_or_prop}']"
    try:
        el = driver.find_element(By.CSS_SELECTOR, sel)
        return safe_get_attr(el, "content")
    except Exception:
        return ""


def scrape_pin_detail(driver, pin_url: str, keyword: str, wait_sec: float = 10.0) -> PinRow:
    driver.get(pin_url)

    # Wait for either the JSON payload or the main content to exist
    try:
        WebDriverWait(driver, wait_sec).until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "script#__PWS_DATA__")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test-id='pin']")),
            )
        )
    except Exception:
        pass

    pin_id = parse_pin_id(pin_url)
    raw_json = ""
    try:
        data_el = driver.find_element(By.CSS_SELECTOR, "script#__PWS_DATA__")
        raw_json = data_el.get_attribute("innerHTML") or ""
    except Exception:
        raw_json = ""

    pin_dict = {}
    if raw_json:
        try:
            data = json.loads(raw_json)
            # The real content is nested; search through it
            pin_candidate = find_first_pin_like_dict(data, pin_id_hint=pin_id)
            if isinstance(pin_candidate, dict):
                pin_dict = pin_candidate
        except Exception:
            pin_dict = {}

    # Extract fields from JSON if available
    title = (
        str(pin_dict.get("grid_title"))
        or str(pin_dict.get("title") or "")
    )
    description = str(pin_dict.get("description") or "")
    created_at = str(pin_dict.get("created_at") or pin_dict.get("createdAt") or "")

    # Images (Pinterest often puts images under images["orig"] or similar sizes)
    image_url = ""
    image_width = ""
    image_height = ""
    images = pin_dict.get("images") if isinstance(pin_dict, dict) else None
    if isinstance(images, dict):
        # try common size keys in preference
        for size_key in ["orig", "564x", "474x", "236x"]:
            if size_key in images and isinstance(images[size_key], dict):
                image_url = str(images[size_key].get("url") or "")
                image_width = str(images[size_key].get("width") or "")
                image_height = str(images[size_key].get("height") or "")
                if image_url:
                    break

    # Counts
    save_count = ""
    comment_count = ""
    for key in ["saveCount", "aggregated_pin_data", "aggregatedStats", "counts", "stats"]:
        if key in pin_dict and isinstance(pin_dict[key], dict):
            d = pin_dict[key]
            save_count = save_count or str(d.get("saveCount") or d.get("saves") or "")
            comment_count = comment_count or str(d.get("commentCount") or d.get("comments") or "")
    # direct at root if present
    save_count = save_count or str(pin_dict.get("saveCount") or "")
    comment_count = comment_count or str(pin_dict.get("commentCount") or "")

    # Outbound link
    outbound_link = str(pin_dict.get("link") or pin_dict.get("dominant_link") or "")

    # Pinner info
    pinner_username = ""
    pinner_fullname = ""
    pinner_profile_url = ""

    pinner = pin_dict.get("pinner") if isinstance(pin_dict, dict) else None
    if isinstance(pinner, dict):
        pinner_username = str(pinner.get("username") or "")
        pinner_fullname = str(pinner.get("full_name") or pinner.get("fullName") or "")
        if pinner_username:
            pinner_profile_url = f"https://www.pinterest.com/{pinner_username}/"

    # Board info
    board_name = ""
    board_url = ""
    board = pin_dict.get("board") if isinstance(pin_dict, dict) else None
    if isinstance(board, dict):
        board_name = str(board.get("name") or "")
        # Pinterest board URL fields vary: try url/owner/slug composition
        board_url = str(board.get("url") or "")
        if not board_url:
            owner = board.get("owner") if isinstance(board.get("owner"), dict) else {}
            owner_usr = owner.get("username") or ""
            slug = board.get("slug") or ""
            if owner_usr and slug:
                board_url = f"https://www.pinterest.com/{owner_usr}/{slug}/"

    # Fallbacks via meta tags (when JSON didn’t give enough)
    if not title:
        title = text_from_meta(driver, "og:title", is_prop=True) or text_from_meta(driver, "twitter:title")
    if not description:
        description = text_from_meta(driver, "og:description", is_prop=True) or text_from_meta(driver, "description")
    if not image_url:
        image_url = text_from_meta(driver, "og:image", is_prop=True)

    # If still no dimensions, try to sniff the rendered IMG
    if not image_url or not image_width or not image_height:
        try:
            img = driver.find_element(By.CSS_SELECTOR, "img")
            if not image_url:
                image_url = extract_image_src(img)
            if not image_width:
                image_width = safe_get_attr(img, "width")
            if not image_height:
                image_height = safe_get_attr(img, "height")
        except Exception:
            pass

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


def collect_pin_urls_for_keyword(driver, keyword: str, limit_n: int, slow: float) -> List[str]:
    url = SEARCH_URL.format(query=keyword.replace(" ", "%20"))
    driver.get(url)

    try:
        WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test-id='SearchPageContent']"))
        )
    except Exception:
        pass

    collected: List[str] = []
    seen: Set[str] = set()
    consecutive_no_growth = 0

    while len(collected) < limit_n and consecutive_no_growth < 6:
        batch_urls = extract_pin_items_from_view(driver, seen)
        before = len(collected)
        for u in batch_urls:
            if u not in seen:
                collected.append(u)
                seen.add(u)
                if len(collected) >= limit_n:
                    break
        after = len(collected)
        if after == before:
            consecutive_no_growth += 1
        else:
            consecutive_no_growth = 0
        scroll_results(driver, slow=slow, step_px=1200)

    return collected[:limit_n]


def main():
    ap = argparse.ArgumentParser(description="Pinterest scraper with rich pin details (Selenium).")
    ap.add_argument("--input", required=True, help="Path to keywords CSV (columns: keyword,limit)")
    ap.add_argument("--output", required=True, help="Path to output CSV")
    ap.add_argument("--default-limit", type=int, default=40, help="Default per-keyword limit")
    ap.add_argument("--headful", action="store_true", help="Run visible browser")
    ap.add_argument("--slow", type=float, default=0.7, help="Seconds to sleep between scrolls")
    args = ap.parse_args()

    try:
        keywords = read_keywords_csv(args.input, default_limit=args.default_limit)
        if not keywords:
            print("No keywords found in CSV.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)

    driver = build_driver(headless=not args.headful)

    rows: List[PinRow] = []
    try:
        for r in keywords:
            kw = r["keyword"]
            lim = int(r["limit"])
            print(f"\n>>> Searching '{kw}' (limit {lim}) ...")
            pin_urls = collect_pin_urls_for_keyword(driver, kw, lim, slow=args.slow)
            print(f"Found {len(pin_urls)} pin URLs for '{kw}'. Gathering details...")

            for i, pu in enumerate(pin_urls, 1):
                try:
                    pr = scrape_pin_detail(driver, pu, kw, wait_sec=12.0)
                    rows.append(pr)
                    print(f"  [{i}/{len(pin_urls)}] ok: {pu}")
                except Exception as e:
                    print(f"  [{i}/{len(pin_urls)}] failed: {pu} ({e})")
                    # continue to next pin
    finally:
        driver.quit()

    # Write output CSV
    df = pd.DataFrame([asdict(r) for r in rows])
    # Deduplicate by pin_url just in case
    if not df.empty:
        df.drop_duplicates(subset=["pin_url"], inplace=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"\nDone. Wrote {len(df)} rows to {args.output}")


if __name__ == "__main__":
    main()
