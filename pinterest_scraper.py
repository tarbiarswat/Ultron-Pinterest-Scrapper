import argparse
import csv
import sys
import time
from dataclasses import dataclass
from typing import List, Dict, Set, Optional

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
class PinItem:
    keyword: str
    pin_url: str
    image_url: str
    title: str


def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    # Make Selenium look less like automation
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,1000")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    # Stealth tweak
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """
        },
    )
    return driver


def read_keywords_csv(path: str, default_limit: int) -> List[Dict[str, str]]:
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
        val = elem.get_attribute(attr)
        return val or ""
    except Exception:
        return ""


def extract_image_src(img_el) -> str:
    """
    Pinterest images may use 'src', 'srcset', or lazy attributes.
    We try a few common attributes.
    """
    candidates = [
        safe_get_attr(img_el, "src"),
        safe_get_attr(img_el, "data-src"),
        safe_get_attr(img_el, "data-lazy-src"),
        safe_get_attr(img_el, "srcset"),
    ]
    # If srcset present, pick the first URL
    for c in candidates:
        if c:
            if " " in c and "http" in c:
                # srcset like "https://... 236w, https://... 474w"
                first = c.split(",")[0].strip().split(" ")[0]
                return first
            return c
    return ""


def extract_pin_items_from_view(driver, keyword: str, seen_urls: Set[str]) -> List[PinItem]:
    """
    Grab what’s currently rendered in the results grid.
    """
    items: List[PinItem] = []

    # Pin anchors live within a masonry grid. We look for links to /pin/<id>/
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/pin/']")
    for a in anchors:
        href = safe_get_attr(a, "href")
        if not href or "/pin/" not in href:
            continue
        if href in seen_urls:
            continue

        # Try to find an image under this anchor
        img_url = ""
        title = ""

        try:
            img = a.find_element(By.CSS_SELECTOR, "img")
            img_url = extract_image_src(img)
            # Title/alt
            title = safe_get_attr(img, "alt") or safe_get_attr(a, "aria-label") or ""
        except Exception:
            # Some cards might be video or background divs — skip if no image
            pass

        # Fallback: sometimes the aria-label on anchor holds text
        if not title:
            title = safe_get_attr(a, "aria-label")

        if not img_url:
            # As a last resort, try meta content from parent figure (rare)
            pass

        # Minimal filter: require at least a pin URL
        items.append(PinItem(keyword=keyword, pin_url=href, image_url=img_url or "", title=title or ""))

    return items


def scroll_results(driver, slow: float = 0.5, step_px: int = 1000):
    # Scroll down a bit, allow content to lazy-load
    driver.execute_script(f"window.scrollBy(0, {step_px});")
    time.sleep(slow)


def gather_for_keyword(driver, keyword: str, limit_n: int, slow: float) -> List[PinItem]:
    url = SEARCH_URL.format(query=keyword.replace(" ", "%20"))
    driver.get(url)

    # Wait for results to start rendering
    try:
        WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-test-id='SearchPageContent']"))
        )
    except Exception:
        # Continue anyway; sometimes content is present but selector differs
        pass

    collected: List[PinItem] = []
    seen: Set[str] = set()
    consecutive_no_growth = 0

    while len(collected) < limit_n and consecutive_no_growth < 6:
        batch = extract_pin_items_from_view(driver, keyword, seen)
        before = len(collected)
        for it in batch:
            if it.pin_url not in seen:
                collected.append(it)
                seen.add(it.pin_url)
                if len(collected) >= limit_n:
                    break

        after = len(collected)
        if after == before:
            consecutive_no_growth += 1
        else:
            consecutive_no_growth = 0

        scroll_results(driver, slow=slow, step_px=1200)

    # Deduplicate by pin_url (already handled) and trim to limit
    return collected[:limit_n]


def main():
    ap = argparse.ArgumentParser(description="Straightforward Pinterest scraper (Selenium).")
    ap.add_argument("--input", required=True, help="Path to keywords CSV (columns: keyword,limit)")
    ap.add_argument("--output", required=True, help="Path to output CSV")
    ap.add_argument("--default-limit", type=int, default=40, help="Default per-keyword limit if not given in CSV")
    ap.add_argument("--headful", action="store_true", help="Run with a visible Chrome window (not headless)")
    ap.add_argument("--slow", type=float, default=0.5, help="Seconds to sleep between scrolls")
    args = ap.parse_args()

    try:
        rows = read_keywords_csv(args.input, default_limit=args.default_limit)
        if not rows:
            print("No keywords found in CSV.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Failed to read CSV: {e}", file=sys.stderr)
        sys.exit(1)

    driver = build_driver(headless=not args.headful)

    all_items: List[PinItem] = []
    try:
        for r in rows:
            kw = r["keyword"]
            lim = int(r["limit"])
            print(f"\n>>> Searching '{kw}' (limit {lim}) ...")
            items = gather_for_keyword(driver, kw, lim, slow=args.slow)
            print(f"Collected {len(items)} pins for '{kw}'.")
            all_items.extend(items)
    finally:
        driver.quit()

    # Write combined CSV
    out_rows = [
        {
            "keyword": it.keyword,
            "pin_url": it.pin_url,
            "image_url": it.image_url,
            "title": it.title,
        }
        for it in all_items
    ]
    df = pd.DataFrame(out_rows)
    df.drop_duplicates(subset=["pin_url"], inplace=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"\nDone. Wrote {len(df)} unique rows to {args.output}")


if __name__ == "__main__":
    main()
