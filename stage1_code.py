# -*- coding: utf-8 -*-
"""
STAGE 1 — INCREMENTAL + MASTER + DELTA (GitHub-friendly, simple state files)

What this version does:
1. Reads last scraped datetime from a small txt file
2. Reads already-seen URLs from a small txt file
3. Crawls Becker sections incrementally
4. Keeps only truly new rows in DELTA_CSV
5. Appends/merges rows into MASTER CSV
6. Updates:
      - stage1_last_scraped_dt.txt
      - stage1_seen_urls.txt

Outputs:
- stage1_master.csv   = cumulative master history
- stage1_delta.csv    = only new rows from current run

State files:
- stage1_last_scraped_dt.txt
- stage1_seen_urls.txt
"""

from __future__ import annotations

import csv
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser


# ============================
# CONFIG
# ============================
BASE_SECTIONS = [
    ("https://www.beckerspayer.com/contracting/", "Beckers Payer", "contracting"),
    ("https://www.beckerspayer.com/payer/", "Beckers Payer", "payer"),
    ("https://www.beckerspayer.com/payer/medicare-advantage/", "Beckers Payer", "medicare_advantage"),
    ("https://www.beckerspayer.com/payer/medicaid/", "Beckers Payer", "medicaid"),
    ("https://www.beckerspayer.com/policy-updates/", "Beckers Payer", "policy_updates"),
    ("https://www.beckerspayer.com/payer/aca/", "Beckers Payer", "aca"),
    ("https://www.beckershospitalreview.com/finance/", "Beckers Hospital Review", "finance"),
]

FINANCE_RSS_URL = "https://news.google.com/rss/search?q=site:beckershospitalreview.com/finance"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "OUTPUT_STAGE1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = OUT_DIR / "stage1_master.csv"
DELTA_CSV = OUT_DIR / "stage1_delta.csv"
LAST_SCRAPED_FILE = OUT_DIR / "stage1_last_scraped_dt.txt"
SEEN_URLS_FILE = OUT_DIR / "stage1_seen_urls.txt"

MAX_PAGES = 3000
SLEEP_SEC = 4
TIMEOUT = 45

DEFAULT_CUTOFF_DT = datetime(2022, 1, 1, 0, 0, 0)


# ============================
# MODEL
# ============================
@dataclass
class Listing:
    title: str
    url: str
    published_dt: Optional[str]
    source: str
    section: str


# ============================
# SESSION
# ============================
session = requests.Session()
session.headers.update(HEADERS)


# ============================
# HELPERS
# ============================
def warm_up_session():
    warm_urls = [
        "https://www.beckerspayer.com/",
        "https://www.beckerspayer.com/payer/",
        "https://www.beckershospitalreview.com/",
    ]
    for u in warm_urls:
        try:
            session.get(u, timeout=TIMEOUT)
            time.sleep(2)
        except Exception:
            pass


def clean_url(url: str) -> str:
    p = urlparse(url)
    q = [
        (k, v)
        for k, v in parse_qsl(p.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in {"oly_enc_id", "origin"}
    ]
    path = p.path
    if path.endswith("/") and path != "/":
        path = path[:-1]
    return urlunparse((p.scheme, p.netloc, path, p.params, urlencode(q, doseq=True), ""))


def normalize_url(base_url: str, href: str) -> str:
    return clean_url(urljoin(base_url, href))


def normalize_title(title: str) -> str:
    if not title:
        return ""
    t = title.strip().lower()
    t = t.replace("â€˜", "'").replace("â€™", "'").replace("â€œ", '"').replace("â€�", '"')
    t = t.replace("â€“", "-").replace("â€”", "-").replace("Â", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_pub_date(published_dt: Optional[str]) -> str:
    if not published_dt:
        return "UNKNOWN"
    try:
        dt = date_parser.parse(published_dt)
        return dt.date().isoformat()
    except Exception:
        return "UNKNOWN"


def parse_date_loose(text: str) -> Optional[datetime]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text).strip()
    t = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", t, flags=re.IGNORECASE)
    try:
        return date_parser.parse(t, fuzzy=True)
    except Exception:
        return None


def parse_iso_any(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    s = dt_str.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ============================
# STATE FILE HELPERS
# ============================
def read_last_scraped_dt() -> datetime:
    if not LAST_SCRAPED_FILE.exists():
        return DEFAULT_CUTOFF_DT
    try:
        txt = LAST_SCRAPED_FILE.read_text(encoding="utf-8").strip()
        if not txt:
            return DEFAULT_CUTOFF_DT
        dt = date_parser.parse(txt)
        return dt
    except Exception:
        return DEFAULT_CUTOFF_DT


def write_last_scraped_dt(dt: datetime):
    LAST_SCRAPED_FILE.write_text(dt.isoformat(), encoding="utf-8")


def read_seen_urls() -> set:
    if not SEEN_URLS_FILE.exists():
        return set()
    try:
        return {
            line.strip()
            for line in SEEN_URLS_FILE.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    except Exception:
        return set()


def write_seen_urls(urls: set):
    ordered = sorted(urls)
    SEEN_URLS_FILE.write_text("\n".join(ordered), encoding="utf-8")


# ============================
# MASTER CSV HELPERS
# ============================
def load_existing_master_rows(master_csv: Path) -> Dict[Tuple[str, str], Dict[str, str]]:
    merged: Dict[Tuple[str, str], Dict[str, str]] = {}

    if not master_csv.exists():
        return merged

    try:
        with open(master_csv, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for r in reader:
                title = (r.get("title") or "").strip()
                published_dt = (r.get("published_dt") or "").strip()
                sources = (r.get("sources") or "").strip()
                sections = (r.get("sections") or "").strip()
                urls = (r.get("urls") or "").strip()

                tkey = normalize_title(title)
                dkey = normalize_pub_date(published_dt if published_dt else None)
                mkey = (tkey, dkey)

                merged[mkey] = {
                    "title": title,
                    "published_dt": published_dt,
                    "sources": sources,
                    "sections": sections,
                    "urls": urls,
                }
    except Exception:
        pass

    return merged


def merge_csv_values(old_val: str, new_val: str) -> str:
    old_items = [x.strip() for x in old_val.split(",") if x.strip()] if old_val else []
    new_items = [x.strip() for x in new_val.split(",") if x.strip()] if new_val else []

    combined = []
    seen = set()

    for item in old_items + new_items:
        if item not in seen:
            seen.add(item)
            combined.append(item)

    return ", ".join(combined)


def add_to_merged(merged: Dict[Tuple[str, str], Dict[str, str]], it: Listing):
    tkey = normalize_title(it.title)
    dkey = normalize_pub_date(it.published_dt)
    mkey = (tkey, dkey)

    if mkey not in merged:
        merged[mkey] = {
            "title": it.title,
            "published_dt": it.published_dt or "",
            "sources": it.source,
            "sections": it.section,
            "urls": it.url,
        }
        return

    merged[mkey]["sources"] = merge_csv_values(merged[mkey]["sources"], it.source)
    merged[mkey]["sections"] = merge_csv_values(merged[mkey]["sections"], it.section)
    merged[mkey]["urls"] = merge_csv_values(merged[mkey]["urls"], it.url)


# ============================
# FETCH
# ============================
def fetch_html(url: str) -> Tuple[Optional[str], Optional[str]]:
    last_err = None
    for attempt in range(5):
        try:
            r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                return r.text, None
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        time.sleep(3 * (attempt + 1))
    return None, last_err


def fetch_page_candidates(base_url: str, page: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if page == 1:
        candidates = [
            base_url,
            f"{base_url.rstrip('/')}/",
            f"{base_url.rstrip('/')}/page/1/",
        ]
    else:
        candidates = [
            f"{base_url.rstrip('/')}/page/{page}/",
        ]

    seen = set()
    candidates = [c for c in candidates if not (c in seen or seen.add(c))]

    last_err = None
    for candidate in candidates:
        html, err = fetch_html(candidate)
        if html:
            return html, None, candidate
        last_err = err

    return None, last_err, candidates[-1] if candidates else None


def fetch_finance_rss_items() -> List[Tuple[str, str, Optional[datetime]]]:
    try:
        r = session.get(FINANCE_RSS_URL, timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"  [finance rss error] HTTP {r.status_code}")
            return []

        root = ET.fromstring(r.text)
        items = []
        seen = set()

        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()

            if not title or not link:
                continue

            title = re.sub(
                r"\s*-\s*Becker'?s?\s+Hospital\s+Review\s*$",
                "",
                title,
                flags=re.IGNORECASE
            )

            pub = parse_date_loose(pub_date) if pub_date else None

            key = (title.lower(), link.lower())
            if key in seen:
                continue
            seen.add(key)

            items.append((title, link, pub))

        return items

    except Exception as e:
        print(f"  [finance rss exception] {type(e).__name__}: {e}")
        return []


# ============================
# PARSING
# ============================
def extract_bhr_cards(soup: BeautifulSoup):
    return soup.find_all("article", class_="bh-card")


def parse_bhr_card(card, base_url: str):
    title_tag = card.find("h3", class_="bh-card__title")
    if not title_tag:
        return None

    a = title_tag.find("a", href=True)
    if not a:
        return None

    title = a.get_text(" ", strip=True)
    url = normalize_url(base_url, a["href"])

    published = None
    t = card.find("time", class_="byline__time")
    if t:
        dt_str = t.get("datetime") or t.get_text(" ", strip=True)
        published = parse_iso_any(dt_str) or parse_date_loose(dt_str)

    return title, url, published


def parse_generic_listing(soup: BeautifulSoup, base_url: str):
    results = []
    seen = set()

    for a in soup.select("h2 a, h3 a, h4 a"):
        href = a.get("href")
        title = a.get_text(" ", strip=True)

        if not href or not title:
            continue

        url = normalize_url(base_url, href)

        if ("beckerspayer.com" not in url) and ("beckershospitalreview.com" not in url):
            continue

        if len(title) < 20:
            continue

        pub = None
        parent = a.parent
        for _ in range(8):
            if parent is None:
                break
            text = parent.get_text(" ", strip=True)
            if re.search(r"\b(20\d{2}|yesterday|\d+\s+hours?\s+ago|\d+\s+days?\s+ago)\b", text, re.IGNORECASE):
                pub = parse_date_loose(text)
                if pub:
                    break
            parent = parent.parent

        dedupe_key = (title.lower(), url)
        if dedupe_key not in seen:
            seen.add(dedupe_key)
            results.append((title, url, pub))

    return results


def extract_article_date(article_url: str) -> Tuple[Optional[datetime], Optional[str]]:
    html, err = fetch_html(article_url)
    if not html:
        return None, err or "Unknown error fetching article"

    soup = BeautifulSoup(html, "html.parser")

    t = soup.find("time")
    if t:
        dt_str = t.get("datetime") or t.get_text(" ", strip=True)
        d = parse_iso_any(dt_str) or parse_date_loose(dt_str)
        if d:
            return d, None

    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        d = parse_iso_any(meta["content"]) or parse_date_loose(meta["content"])
        if d:
            return d, None

    return None, "Date not found on article page"


# ============================
# MAIN
# ============================
def main():
    t0 = time.time()

    last_scraped_dt = read_last_scraped_dt()
    seen_urls = read_seen_urls()
    merged = load_existing_master_rows(MASTER_CSV)

    print(f"[watermark] Last scraped datetime = {last_scraped_dt.isoformat()}")
    print(f"[state] Seen URLs loaded = {len(seen_urls)}")

    warm_up_session()

    delta_rows: List[Dict[str, str]] = []
    max_dt_seen_this_run = last_scraped_dt

    section_stats: Dict[str, Dict[str, int]] = {}

    for base_url, source, section in BASE_SECTIONS:
        key = f"{source} | {section}"
        section_stats[key] = {
            "pages": 0,
            "found": 0,
            "new_kept": 0,
            "skipped_old": 0,
            "skipped_existing_url": 0,
            "missing_date": 0,
            "page_errors": 0,
            "article_date_errors": 0,
        }

        print(f"\n[crawl] {key}")

        # ----------------------------
        # FINANCE
        # ----------------------------
        if section == "finance":
            items: List[Tuple[str, str, Optional[datetime]]] = []

            html, err = fetch_html(base_url)
            if html:
                section_stats[key]["pages"] += 1
                soup = BeautifulSoup(html, "html.parser")

                cards = extract_bhr_cards(soup)
                if cards:
                    for c in cards:
                        parsed = parse_bhr_card(c, base_url)
                        if parsed:
                            items.append(parsed)
                else:
                    items.extend(parse_generic_listing(soup, base_url))

                if not items:
                    print("  [finance direct] No items parsed from direct page, trying RSS fallback...")
                    items = fetch_finance_rss_items()
                    if not items:
                        section_stats[key]["page_errors"] += 1

                section_stats[key]["found"] += len(items)

            else:
                section_stats[key]["page_errors"] += 1
                print(f"  [finance direct blocked] {base_url} -> {err}")
                print("  [finance fallback] Using Google News RSS...")
                items = fetch_finance_rss_items()
                section_stats[key]["found"] += len(items)

            resolved_items: List[Tuple[str, str, Optional[datetime]]] = []

            for title, url, pub in items:
                if pub is None:
                    got, _ = extract_article_date(url)
                    time.sleep(SLEEP_SEC)
                    pub = got
                    if pub is None:
                        section_stats[key]["missing_date"] += 1
                        section_stats[key]["article_date_errors"] += 1

                resolved_items.append((title, url, pub))

            for title, url, pub in resolved_items:
                it = Listing(
                    title=title,
                    url=url,
                    published_dt=pub.isoformat() if pub else None,
                    source=source,
                    section=section,
                )

                # older than last watermark -> skip
                if pub is not None and pub < last_scraped_dt:
                    section_stats[key]["skipped_old"] += 1
                    add_to_merged(merged, it)
                    continue

                # already seen url -> skip
                if url in seen_urls:
                    section_stats[key]["skipped_existing_url"] += 1
                    add_to_merged(merged, it)
                    continue

                seen_urls.add(url)
                add_to_merged(merged, it)

                delta_rows.append({
                    "title": it.title,
                    "published_dt": it.published_dt or "",
                    "sources": it.source,
                    "sections": it.section,
                    "urls": it.url,
                })

                section_stats[key]["new_kept"] += 1

                if pub is not None and pub > max_dt_seen_this_run:
                    max_dt_seen_this_run = pub

            s = section_stats[key]
            print(
                f"  [section summary] pages={s['pages']} found={s['found']} new_kept={s['new_kept']} "
                f"skipped_old={s['skipped_old']} skipped_existing_url={s['skipped_existing_url']} "
                f"missing_date={s['missing_date']} page_errors={s['page_errors']} "
                f"article_date_errors={s['article_date_errors']}"
            )
            continue

        # ----------------------------
        # NON-FINANCE
        # ----------------------------
        page = 1
        stop_section = False

        while not stop_section and page <= MAX_PAGES:
            html, err, used_url = fetch_page_candidates(base_url, page)

            if not html:
                section_stats[key]["page_errors"] += 1
                print(f"  [page error] {used_url} -> {err}")
                break

            section_stats[key]["pages"] += 1
            soup = BeautifulSoup(html, "html.parser")

            items: List[Tuple[str, str, Optional[datetime]]] = []
            cards = extract_bhr_cards(soup)

            if cards:
                for c in cards:
                    parsed = parse_bhr_card(c, base_url)
                    if parsed:
                        items.append(parsed)
            else:
                items.extend(parse_generic_listing(soup, base_url))

            if not items:
                break

            section_stats[key]["found"] += len(items)

            page_dts: List[datetime] = []
            page_all_have_dates = True
            resolved_items: List[Tuple[str, str, Optional[datetime]]] = []

            for title, url, pub in items:
                if pub is None:
                    got, _ = extract_article_date(url)
                    time.sleep(SLEEP_SEC)
                    pub = got
                    if pub is None:
                        page_all_have_dates = False
                        section_stats[key]["missing_date"] += 1
                        section_stats[key]["article_date_errors"] += 1

                if pub is not None:
                    page_dts.append(pub)

                resolved_items.append((title, url, pub))

            # stop when page is fully not newer than watermark
            if page_all_have_dates and page_dts:
                newest_on_page = max(page_dts)
                if newest_on_page < last_scraped_dt:
                    stop_section = True

            for title, url, pub in resolved_items:
                it = Listing(
                    title=title,
                    url=url,
                    published_dt=pub.isoformat() if pub else None,
                    source=source,
                    section=section,
                )

                if pub is not None and pub < last_scraped_dt:
                    section_stats[key]["skipped_old"] += 1
                    add_to_merged(merged, it)
                    continue

                if url in seen_urls:
                    section_stats[key]["skipped_existing_url"] += 1
                    add_to_merged(merged, it)
                    continue

                seen_urls.add(url)
                add_to_merged(merged, it)

                delta_rows.append({
                    "title": it.title,
                    "published_dt": it.published_dt or "",
                    "sources": it.source,
                    "sections": it.section,
                    "urls": it.url,
                })

                section_stats[key]["new_kept"] += 1

                if pub is not None and pub > max_dt_seen_this_run:
                    max_dt_seen_this_run = pub

            page += 1
            time.sleep(SLEEP_SEC)

        s = section_stats[key]
        print(
            f"  [section summary] pages={s['pages']} found={s['found']} new_kept={s['new_kept']} "
            f"skipped_old={s['skipped_old']} skipped_existing_url={s['skipped_existing_url']} "
            f"missing_date={s['missing_date']} page_errors={s['page_errors']} "
            f"article_date_errors={s['article_date_errors']}"
        )

    # write master
    with open(MASTER_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "published_dt", "sources", "sections", "urls"])
        writer.writeheader()
        for row in merged.values():
            writer.writerow(row)

    # write delta
    with open(DELTA_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "published_dt", "sources", "sections", "urls"])
        writer.writeheader()
        for row in delta_rows:
            writer.writerow(row)

    # update state files
    write_seen_urls(seen_urls)
    write_last_scraped_dt(max_dt_seen_this_run)

    t1 = time.time()
    total_seconds = t1 - t0

    print("\n==============================")
    print("[done] Stage 1 incremental complete")
    print(f"New rows kept in this run: {len(delta_rows)}")
    print(f"Master file: {MASTER_CSV}")
    print(f"Delta file: {DELTA_CSV}")
    print(f"Last scraped datetime saved: {max_dt_seen_this_run.isoformat()}")
    print(f"Seen URLs saved: {len(seen_urls)}")
    print(f"Runtime: {total_seconds:.2f} sec ({total_seconds/60:.2f} min)")
    print("==============================\n")


if __name__ == "__main__":
    main()

    import pandas as pd

    df = pd.read_csv(MASTER_CSV)

    print("Total rows in master file:", len(df))
    print("Unique titles:", df["title"].nunique())
    print("Unique URLs:", df["urls"].nunique())

    print("\nRows by source (raw merged field):")
    print(
        df.groupby("sources")
          .size()
          .reset_index(name="row_count")
          .sort_values("row_count", ascending=False)
    )

    print("\nRows by section (raw merged field):")
    print(
        df.groupby("sections")
          .size()
          .reset_index(name="row_count")
          .sort_values("row_count", ascending=False)
    )

    print("\nActual section-wise counts:")
    section_count = (
        df.assign(section=df["sections"].fillna("").str.split(", "))
          .explode("section")
          .groupby("section")
          .size()
          .reset_index(name="row_count")
          .sort_values("row_count", ascending=False)
    )
    print(section_count)

    print("\nActual source-section counts:")
    df2 = df.copy()
    df2["sources"] = df2["sources"].fillna("").str.split(", ")
    df2["sections"] = df2["sections"].fillna("").str.split(", ")
    df2 = df2.explode("sources").explode("sections")

    source_section_count = (
        df2.groupby(["sources", "sections"])
           .size()
           .reset_index(name="row_count")
           .sort_values(["sources", "row_count"], ascending=[True, False])
    )
    print(source_section_count)
