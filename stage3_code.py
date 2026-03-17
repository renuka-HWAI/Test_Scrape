# -*- coding: utf-8 -*-
"""
STAGE 3B — EXIT + CLOSURE EXTRACTOR (SEPARATE OUTPUT FILES, RESUMABLE)
HYBRID FETCH VERSION:
- non-finance -> requests
- finance -> Selenium

Reads Stage2 kept DELTA, fetches articles, calls LLM, writes:
- EXIT events
- CLOSURE events
- NO articles
- SKIPPED articles
- ERROR articles
"""

from __future__ import annotations

import json
import time
import re
import datetime
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# ======================
# CONFIG
# ======================
BASE_DIR = Path(__file__).resolve().parent
IN_CSV = BASE_DIR / "OUTPUT_STAGE2" / "stage2_filtered_kept_delta.csv"


OUTPUT_DIR = BASE_DIR / "OUTPUT_STAGE3"
OUTPUT_DIR.mkdir(exist_ok=True)

OUT_EXIT = OUTPUT_DIR / "stage3_exit_events.csv"
OUT_CLOSURE = OUTPUT_DIR / "stage3_closure_events.csv"
OUT_NO = OUTPUT_DIR / "stage3_no.csv"
OUT_SKIPPED = OUTPUT_DIR / "stage3_skipped_run.csv"
OUT_ERROR = OUTPUT_DIR / "stage3_error.csv"

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
if not OPENROUTER_API_KEY:
    raise ValueError("Missing OPENROUTER_API_KEY in environment variables")

MODEL = os.getenv("MODEL")
OPENROUTER_URL = os.getenv("OPENROUTER_URL")

RERUN_SKIPPED_MODE = False
RERUN_ERROR_MODE = False

HEADERS = {
    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
    "Content-Type": "application/json",
    "HTTP-Referer": "https://localhost",
    "X-Title": "Beckers Exit & Closure Extractor Stage3B"
}

MAX_CHARS = 9000
REQUEST_TIMEOUT = 25
SELENIUM_WAIT_SEC = 20

SLEEP_SEC = 0.8
MAX_LLM_RETRIES = 4
BACKOFF_BASE_SEC = 2.0

CHECKPOINT_EVERY_N_ROWS = 25
SAVE_EVERY_NEW_EVENTS = 10


# ======================
# PROMPT
# ======================
SYSTEM = """
You extract structured healthcare event signals from news articles.

There are 2 distinct concepts:

1) Exit events
These include only:
- provider_payer_exit:
  A provider/health system goes out-of-network, terminates, splits, or has a contract end/termination
  with a specific payer/insurer.
- payer_market_exit:
  A payer/insurer exits a market, state, program, or line of business
  (for example Medicare Advantage, ACA marketplace, Medicaid).
- provider_market_exit:
  A provider/health system stops accepting, drops, or exits a line of coverage
  (for example dropping Medicare Advantage plans).

2) Closures
These include:
- hospital closures
- facility closures
- emergency department closures
- unit/service line closures
- office/site closures
- shutdowns or operational closures

Important:
Closures are NOT the same as exit events.
Do not convert closures into exit events unless the article explicitly describes one of the 3 exit event types above.

An article may contain:
- only exit events
- only closures
- both exit events and closures
- neither

Return ONLY valid JSON. No explanations.
Do NOT guess missing data. If not explicitly stated, return null or [].
"""

USER_TEMPLATE = """
Task:
Determine whether this article describes:
1) one or more healthcare exit events, and/or
2) one or more healthcare closures.

Return exactly in this JSON format:

{
  "is_exit": true or false,
  "is_closure": true or false,
  "non_exit_reason": "string or null",
  "events": [
    {
      "record_type": "exit",
      "event_type": "provider_payer_exit | payer_market_exit | provider_market_exit",
      "status": "Active Exit | Resolved | Temporary Disruption | Planned Exit | Regulatory Termination",
      "provider": "Provider/health system name or null",
      "payer": "Payer/insurer name or null",
      "market": "Medicare Advantage | Medicaid | ACA | Commercial | Multiple | Unknown",
      "program": "SNP | D-SNP | C-SNP | I-SNP | Marketplace | null",
      "effective_date": "YYYY-MM-DD or null",
      "states": ["MN","WI"] or [],
      "geography_detail": "Cities/counties/regions if mentioned, or null",
      "reason": "Explicit reason only (no speculation) or null",
      "member_impact": "Explicit member impact only (numbers/disruption) or null",
      "summary": "2–4 sentences: what happened, effective date, geography, reason, member impact if mentioned."
    },
    {
      "record_type": "closure",
      "closure_type": "hospital_closure | facility_closure | unit_closure | service_line_closure | office_closure | other_closure",
      "status": "Active Closure | Planned Closure | Temporary Closure | Resolved",
      "provider": "Provider/health system/facility name or null",
      "payer": null,
      "market": null,
      "program": null,
      "effective_date": "YYYY-MM-DD or null",
      "states": ["MN","WI"] or [],
      "geography_detail": "Cities/counties/regions if mentioned, or null",
      "reason": "Explicit reason only (no speculation) or null",
      "member_impact": "Explicit member impact only (numbers/disruption) or null",
      "summary": "2–4 sentences: what closed, effective date, geography, reason, member impact if mentioned."
    }
  ]
}

Rules:
- Extract ALL distinct events if the article lists multiple exits and/or closures.
- Keep closures separate from exits.
- Use US state 2-letter abbreviations only if clearly mentioned; else [].
- If geography is cities/counties (not states), put it in geography_detail.
- effective_date only if explicitly stated; else null.
- program only if explicitly stated; else null.

Exit event rules:
- provider_payer_exit: provider AND payer must both be specific.
- payer_market_exit: payer must be specific; provider can be null.
- provider_market_exit: provider must be specific; payer can be null.

Closure rules:
- If the article is about a hospital/facility/unit/service/site shutting down or closing, classify as closure.
- Do NOT force closures into exit event categories unless the article explicitly describes both.

Do NOT classify these alone as exit events:
- Drug coverage sunsets or drops (e.g., GLP-1 coverage changes)
- Benefit design changes
- Formulary removals
- Reimbursement policy adjustments
- Coverage reductions within an existing program
- Prior authorization / utilization management changes
- Discontinuing or reducing some plan offerings WITHOUT explicitly exiting a state, market, or line of business
- Portfolio optimization or enrollment decline
- Rate increases or pricing changes

If the article contains none of the allowed exit events and no closures, set:
- "is_exit": false
- "is_closure": false
- "non_exit_reason": a short explanation
- "events": []

Title: <<TITLE>>
Section: <<SECTION>>
Source: <<SOURCE>>
Published: <<PUBLISHED>>

Article:
<<TEXT>>
"""


# ======================
# HELPERS
# ======================
def clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def pick_first_url_from_field(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return ""
    if "," in s:
        return s.split(",")[0].strip()
    return s


def get_url_from_row(row: pd.Series, df_cols: List[str]) -> str:
    if "link" in df_cols:
        return pick_first_url_from_field(row.get("link"))
    if "url" in df_cols:
        return pick_first_url_from_field(row.get("url"))
    if "urls" in df_cols:
        return pick_first_url_from_field(row.get("urls"))
    if "urls_canonical" in df_cols:
        return pick_first_url_from_field(row.get("urls_canonical"))
    return ""


def make_driver():
    options = Options()
    # REQUIRED for GitHub Actions
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def fetch_article_text_selenium(url: str, driver) -> Tuple[str, Optional[str]]:
    try:
        driver.get(url)

        WebDriverWait(driver, SELENIUM_WAIT_SEC).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(5)

        html = driver.page_source
        if "Please enable JS and disable any ad blocker" in html:
            return "", "Blocked by anti-bot challenge"

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        container = (
            soup.select_one("div.entry-main__content.entry-content")
            or soup.select_one("div.entry-content")
            or soup.select_one("article")
        )

        text = container.get_text(" ", strip=True) if container else soup.get_text(" ", strip=True)
        text = clean_ws(text)
        return text[:MAX_CHARS], None

    except Exception as e:
        return "", f"Selenium {type(e).__name__}: {e}"


def fetch_article_text(url: str, driver=None) -> Tuple[str, Optional[str]]:
    if "beckershospitalreview.com/finance/" in url:
        if driver is None:
            return "", "No Selenium driver provided for finance URL"
        return fetch_article_text_selenium(url, driver)

    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return "", f"HTTP {r.status_code}"

        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        container = (
            soup.select_one("div.entry-main__content.entry-content")
            or soup.select_one("div.entry-content")
            or soup.select_one("article")
        )

        text = container.get_text(" ", strip=True) if container else soup.get_text(" ", strip=True)
        text = clean_ws(text)
        return text[:MAX_CHARS], None
    except Exception as e:
        return "", f"{type(e).__name__}: {e}"


def safe_json_load(s: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(s)
    except Exception:
        return None


def call_llm_with_retry(system: str, user: str) -> Dict[str, Any]:
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_LLM_RETRIES + 1):
        try:
            r = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload, timeout=60)

            if r.status_code in (401, 403):
                raise RuntimeError(f"AUTH_ERROR: {r.status_code} {r.text[:200]}")
            if r.status_code >= 500:
                raise RuntimeError(f"SERVER_ERROR: {r.status_code} {r.text[:200]}")

            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]

            parsed = safe_json_load(content)
            if parsed is None:
                m = re.search(r"\{.*\}", content, flags=re.DOTALL)
                if m:
                    parsed = safe_json_load(m.group(0))

            if parsed is None:
                raise ValueError(f"Invalid JSON from model. Raw: {content[:400]}")

            return parsed

        except Exception as e:
            last_err = e
            msg = str(e)

            if msg.startswith("AUTH_ERROR:"):
                raise

            backoff = BACKOFF_BASE_SEC * (2 ** (attempt - 1))
            print(f"   -> LLM retry {attempt}/{MAX_LLM_RETRIES} after {type(e).__name__}: {msg[:120]} | sleep {backoff:.1f}s")
            time.sleep(backoff)

    raise last_err  # type: ignore


def read_links_from_csv(path: str, link_col: str = "link") -> set:
    try:
        df = pd.read_csv(path)
        if link_col in df.columns:
            return set(df[link_col].dropna().astype(str).str.strip().tolist())
        return set()
    except Exception:
        return set()


def checkpoint_save(df_rows: List[Dict[str, Any]], out_path: str):
    if not df_rows:
        return
    pd.DataFrame(df_rows).to_csv(out_path, index=False, encoding="utf-8-sig")


def normalize_states(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return []


def ensure_events_list(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    ev = result.get("events")
    if isinstance(ev, list):
        return [x for x in ev if isinstance(x, dict)]
    return []


# ======================
# MAIN
# ======================
def main():
    start_dt = datetime.datetime.now()
    print("\n==============================")
    print("STAGE 3B EXIT + CLOSURE EXTRACTION START")
    print("Start Time:", start_dt)
    print("==============================\n")

    driver = None
    try:
        driver = make_driver()
        print("[init] Selenium driver started\n")
    except Exception as e:
        print(f"[warn] Selenium driver could not be started: {e}\n")

    df = pd.read_csv(IN_CSV)
    cols = df.columns.tolist()

    print("Columns in input file:", cols)
    print("Total rows to check:", len(df))
    print()

    processed_links = set()
    processed_links |= read_links_from_csv(OUT_EXIT, "link")
    processed_links |= read_links_from_csv(OUT_CLOSURE, "link")
    processed_links |= read_links_from_csv(OUT_NO, "link")

    if not RERUN_ERROR_MODE:
        processed_links |= read_links_from_csv(OUT_ERROR, "link")

    if not RERUN_SKIPPED_MODE:
        processed_links |= read_links_from_csv(OUT_SKIPPED, "link")

    if processed_links:
        print(f"[resume] Found {len(processed_links)} already-processed links across outputs. Will skip them.\n")

    def load_existing(path: str) -> List[Dict[str, Any]]:
        try:
            return pd.read_csv(path).to_dict(orient="records")
        except Exception:
            return []

    exit_rows = load_existing(OUT_EXIT)
    closure_rows = load_existing(OUT_CLOSURE)
    no_rows = load_existing(OUT_NO)
    skipped_rows = load_existing(OUT_SKIPPED)
    error_rows = load_existing(OUT_ERROR)

    counts = {
        "EXIT_events": 0,
        "EXIT_articles": 0,
        "CLOSURE_events": 0,
        "CLOSURE_articles": 0,
        "NO": 0,
        "SKIPPED": 0,
        "ERROR": 0,
    }

    processed = 0
    total = len(df)

    try:
        for idx, row in df.iterrows():
            row_num = idx + 1
            title = str(row.get("title", "")).strip()
            section = str(row.get("sections", row.get("section", "")) or "")
            source = str(row.get("sources", row.get("source", "")) or "")
            published = str(row.get("published_dt", "") or "")
            url = get_url_from_row(row, cols)

            if not title:
                continue

            if not url:
                counts["SKIPPED"] += 1
                skipped_rows.append({
                    "title": title,
                    "published_dt": published,
                    "section": section,
                    "source": source,
                    "link": "",
                    "skip_reason": "No URL found"
                })
                continue

            if url in processed_links:
                continue

            t_article_start = time.time()
            print(f"[{row_num}/{total}] Checking: {title[:90]}")

            text, fetch_err = fetch_article_text(url, driver=driver)
            if fetch_err:
                counts["SKIPPED"] += 1
                skipped_rows.append({
                    "title": title,
                    "published_dt": published,
                    "section": section,
                    "source": source,
                    "link": url,
                    "skip_reason": f"Fetch error: {fetch_err}"
                })
                processed_links.add(url)
                processed += 1
                print(f"   -> SKIPPED (fetch error: {fetch_err})\n")
                continue

            if not text or len(text.strip()) < 50:
                counts["SKIPPED"] += 1
                skipped_rows.append({
                    "title": title,
                    "published_dt": published,
                    "section": section,
                    "source": source,
                    "link": url,
                    "skip_reason": "Empty or blocked article page"
                })
                processed_links.add(url)
                processed += 1
                print("   -> SKIPPED (Empty or blocked page)\n")
                continue

            prompt = (
                USER_TEMPLATE
                .replace("<<TITLE>>", title)
                .replace("<<SECTION>>", section)
                .replace("<<SOURCE>>", source)
                .replace("<<PUBLISHED>>", published)
                .replace("<<TEXT>>", text)
            )

            try:
                result = call_llm_with_retry(SYSTEM, prompt)
                is_exit = bool(result.get("is_exit"))
                is_closure = bool(result.get("is_closure"))
                events = ensure_events_list(result)

                if (not is_exit and not is_closure) or not events:
                    counts["NO"] += 1
                    no_rows.append({
                        "title": title,
                        "published_dt": published,
                        "section": section,
                        "source": source,
                        "link": url,
                        "note": result.get("non_exit_reason") or "Model classified as no exit events and no closures"
                    })
                    processed_links.add(url)
                    processed += 1
                    dt_article = time.time() - t_article_start
                    print(f"   -> NO | {dt_article:.1f}s\n")

                else:
                    exit_events_this_article = 0
                    closure_events_this_article = 0

                    for j, ev in enumerate(events, start=1):
                        states = normalize_states(ev.get("states"))
                        record_type = str(ev.get("record_type") or "").strip().lower()

                        common_row = {
                            "article_title": title,
                            "article_published_dt": published,
                            "article_section": section,
                            "article_source": source,
                            "link": url,
                            "event_index": j,
                            "status": ev.get("status"),
                            "provider": ev.get("provider"),
                            "payer": ev.get("payer"),
                            "market": ev.get("market"),
                            "program": ev.get("program"),
                            "effective_date": ev.get("effective_date"),
                            "states": ",".join(states),
                            "geography_detail": ev.get("geography_detail"),
                            "reason": ev.get("reason"),
                            "member_impact": ev.get("member_impact"),
                            "summary": ev.get("summary"),
                        }

                        if record_type == "exit":
                            exit_row = common_row.copy()
                            exit_row["record_type"] = "exit"
                            exit_row["event_type"] = ev.get("event_type")
                            exit_rows.append(exit_row)
                            counts["EXIT_events"] += 1
                            exit_events_this_article += 1

                        elif record_type == "closure":
                            closure_row = common_row.copy()
                            closure_row["record_type"] = "closure"
                            closure_row["closure_type"] = ev.get("closure_type")
                            closure_rows.append(closure_row)
                            counts["CLOSURE_events"] += 1
                            closure_events_this_article += 1

                    if exit_events_this_article > 0:
                        counts["EXIT_articles"] += 1
                    if closure_events_this_article > 0:
                        counts["CLOSURE_articles"] += 1

                    processed_links.add(url)
                    processed += 1
                    dt_article = time.time() - t_article_start
                    print(
                        f"   -> SAVED | exit_events={exit_events_this_article} "
                        f"| closure_events={closure_events_this_article} | {dt_article:.1f}s\n"
                    )

                    total_new_events = counts["EXIT_events"] + counts["CLOSURE_events"]
                    if total_new_events % SAVE_EVERY_NEW_EVENTS == 0:
                        checkpoint_save(exit_rows, OUT_EXIT)
                        checkpoint_save(closure_rows, OUT_CLOSURE)
                        checkpoint_save(no_rows, OUT_NO)
                        checkpoint_save(skipped_rows, OUT_SKIPPED)
                        checkpoint_save(error_rows, OUT_ERROR)
                        print(f"   -> Checkpoint saved (every {SAVE_EVERY_NEW_EVENTS} total events).\n")

            except Exception as e:
                msg = str(e)
                counts["ERROR"] += 1

                if msg.startswith("AUTH_ERROR:"):
                    error_rows.append({
                        "title": title,
                        "published_dt": published,
                        "section": section,
                        "source": source,
                        "link": url,
                        "error_type": "AUTH_ERROR",
                        "error_message": msg[:300],
                    })
                    checkpoint_save(error_rows, OUT_ERROR)
                    print("\n!!! AUTH ERROR from OpenRouter (401/403). Fix key/credits then rerun.")
                    raise

                error_rows.append({
                    "title": title,
                    "published_dt": published,
                    "section": section,
                    "source": source,
                    "link": url,
                    "error_type": type(e).__name__,
                    "error_message": msg[:500],
                })
                processed_links.add(url)
                processed += 1
                dt_article = time.time() - t_article_start
                print(f"   -> ERROR: {type(e).__name__}: {msg[:160]} | {dt_article:.1f}s\n")

            if processed % CHECKPOINT_EVERY_N_ROWS == 0:
                checkpoint_save(exit_rows, OUT_EXIT)
                checkpoint_save(closure_rows, OUT_CLOSURE)
                checkpoint_save(no_rows, OUT_NO)
                checkpoint_save(skipped_rows, OUT_SKIPPED)
                checkpoint_save(error_rows, OUT_ERROR)
                print(f"   -> Checkpoint saved at processed={processed} (every {CHECKPOINT_EVERY_N_ROWS}).\n")

            time.sleep(SLEEP_SEC)

    except KeyboardInterrupt:
        print("\n[interrupt] KeyboardInterrupt received. Saving progress now...")
        checkpoint_save(exit_rows, OUT_EXIT)
        checkpoint_save(closure_rows, OUT_CLOSURE)
        checkpoint_save(no_rows, OUT_NO)
        checkpoint_save(skipped_rows, OUT_SKIPPED)
        checkpoint_save(error_rows, OUT_ERROR)
        print("[interrupt] Progress saved.\n")

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    checkpoint_save(exit_rows, OUT_EXIT)
    checkpoint_save(closure_rows, OUT_CLOSURE)
    checkpoint_save(no_rows, OUT_NO)
    checkpoint_save(skipped_rows, OUT_SKIPPED)
    checkpoint_save(error_rows, OUT_ERROR)

    end_dt = datetime.datetime.now()
    runtime = end_dt - start_dt

    print("\n==============================")
    print("STAGE 3B COMPLETE")
    print("==============================")
    print("Start:", start_dt)
    print("End:", end_dt)
    print("Total Runtime:", runtime)
    print("------------------------------")
    print("EXIT articles:", counts["EXIT_articles"])
    print("EXIT events:", counts["EXIT_events"])
    print("CLOSURE articles:", counts["CLOSURE_articles"])
    print("CLOSURE events:", counts["CLOSURE_events"])
    print("NO:", counts["NO"])
    print("SKIPPED:", counts["SKIPPED"])
    print("ERROR:", counts["ERROR"])
    print("------------------------------")
    print("EXIT file:", OUT_EXIT)
    print("CLOSURE file:", OUT_CLOSURE)
    print("NO file:", OUT_NO)
    print("SKIPPED file:", OUT_SKIPPED)
    print("ERROR file:", OUT_ERROR)
    print("==============================\n")


if __name__ == "__main__":
    main()
