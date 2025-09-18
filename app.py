import io
import csv
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


# ---------------------------
# Core scraping logic (rich)
# ---------------------------

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
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
        )
    except Exception:
        pass
    return driver


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


def scroll_results(driver, slow: float = 0.7, step_px: int = 1200):
    driver.execute_script(f"window.scrollBy(0, {step_px});")
    time.sleep(slow)


def parse_pin_id(url: str) -> str:
    m = PIN_ID_RE.search(url)
    return m.group(1) if m else ""


def find_first_pin_like_dict(obj: Any, pin_id_hint: Optional[str] = None) -> Optional[Dict]:
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


def text_from_meta(driver, name_or_prop: str, is_prop=False) -> str:
    sel = f"meta[{'property' if is_prop else 'name'}='{name_or_prop}']"
    try:
        el = driver.find_element(By.CSS_SELECTOR, sel)
        return safe_get_attr(el, "content")
    except NoSuchElementException:
        return ""


# ------- Fix helpers (invalid selector + stale) -------

NUM_RE = re.compile(r"([\d,.]+)\s*([kKmMbB]?)")  # e.g., 12.3K

def parse_compact_number(s: str) -> str:
    s = (s or "").strip()
    m = NUM_RE.search(s)
    if not m:
        return ""
    num, suf = m.groups()
    try:
        base = float(num.replace(",", ""))
    except:
        return ""
    mul = 1
    if suf.lower() == "k": mul = 1_000
    elif suf.lower() == "m": mul = 1_000_000
    elif suf.lower() == "b": mul = 1_000_000_000
    return str(int(base * mul))

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

def retry_stale(fn, tries: int = 3, wait: float = 0.4):
    for _ in range(tries):
        try:
            return fn()
        except StaleElementReferenceException:
            time.sleep(wait)
    return fn()

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


def find_json_anywhere(driver) -> List[dict]:
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
        elif "{\"" in t and "}" in t:
            # grab candidate objects from inline scripts
            for chunk in re.findall(r"\{.*?\}", t, flags=re.DOTALL):
                if any(k in chunk for k in ["\"id\"", "\"images\"", "\"pinner\"", "\"board\"", "\"commentCount\"", "\"saveCount\""]):
                    try:
                        blobs.append(json.loads(chunk))
                    except Exception:
                        pass
    html = driver.page_source or ""
    for chunk in re.findall(r"\{.*?\}", html, flags=re.DOTALL):
        if any(k in chunk for k in ["\"id\"", "\"images\"", "\"pinner\"", "\"board\"", "\"commentCount\"", "\"saveCount\""]):
            try:
                blobs.append(json.loads(chunk))
            except Exception:
                pass
    return blobs

def pick_pin_dict(blobs: List[dict], pin_id_hint: str) -> dict:
    for b in blobs:
        cand = find_first_pin_like_dict(b, pin_id_hint=pin_id_hint)
        if isinstance(cand, dict) and str(cand.get("id")) == str(pin_id_hint):
            return cand
    for b in blobs:
        cand = find_first_pin_like_dict(b, pin_id_hint=None)
        if isinstance(cand, dict):
            return cand
    return {}


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

    # 1) JSON (official & aggressive)
    raw_json = ""
    try:
        data_el = driver.find_element(By.CSS_SELECTOR, "script#__PWS_DATA__")
        raw_json = data_el.get_attribute("innerHTML") or ""
    except NoSuchElementException:
        pass

    pin_dict = {}
    if raw_json:
        try:
            data = json.loads(raw_json)
            pin_dict = find_first_pin_like_dict(data, pin_id_hint=pin_id) or {}
        except Exception:
            pin_dict = {}

    if not pin_dict:
        blobs = find_json_anywhere(driver)
        pin_dict = pick_pin_dict(blobs, pin_id_hint=pin_id)

    # 2) Fields from JSON
    title = str(pin_dict.get("grid_title") or pin_dict.get("title") or "")
    description = str(pin_dict.get("description") or "")
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
    for key in ["saveCount", "aggregated_pin_data", "aggregatedStats", "counts", "stats"]:
        d = pin_dict.get(key) if isinstance(pin_dict, dict) else None
        if isinstance(d, dict):
            save_count = save_count or str(d.get("saveCount") or d.get("saves") or "")
            comment_count = comment_count or str(d.get("commentCount") or d.get("comments") or "")
    save_count = save_count or str(pin_dict.get("saveCount") or "")
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

    # 3) DOM/XPath fallbacks (retry to avoid stales; no :contains())
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

    # Saves / Comments text present in UI
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

    # Pinner from visible links (avoid holding stale refs)
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

    # Board ("/<user>/<board>/")
    if not board_url:
        try:
            links = retry_stale(lambda: driver.find_elements(By.CSS_SELECTOR, "a[href*='pinterest.com/']"))
            for a in links:
                href = safe_get_attr(a, "href") or ""
                if "/pin/" in href:
                    continue
                path = href.replace("https://www.pinterest.com/", "").strip("/")
                # user/board/ (two segments)
                if path.count("/") == 2:
                    board_url = href
                    if not board_name:
                        board_name = a.text.strip()
                    break
        except Exception:
            pass

    # Outbound link (Visit button or external)
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
                if "nofollow" in rel or "noopener" in rel or "noreferrer" in rel:
                    outbound_link = href
                    break
        except Exception:
            pass

    # Normalize numbers if still compact text
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


# ---------------------------
# Streamlit UI
# ---------------------------

st.set_page_config(page_title="Pinterest Scraper UI", layout="wide")

if "kw_df" not in st.session_state:
    st.session_state.kw_df = pd.DataFrame([{"keyword": "technology", "limit": 40}])

st.title("Pinterest Scraper UI (Selenium + Chrome)")

with st.sidebar:
    st.header("Controls")
    headful = st.checkbox("Show browser window", value=True)
    slow_seconds = st.number_input("Scroll delay (seconds)", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
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

col_a, col_b, col_c = st.columns([1,1,2])
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

            driver.quit()
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
