# -*- coding: utf-8 -*-
"""
Fetch first newsletter post from Becker CFO Report archive page.
Made for GitHub / requests-based use.
"""

import re
import requests
from bs4 import BeautifulSoup

URL = "https://www.beckershospitalreview.com/newsletter-category/beckers-hospital-cfo-report-e-weekly/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding or "utf-8"
    return r.text


def main():
    html = fetch_html(URL)
    soup = BeautifulSoup(html, "html.parser")

    # On this page, each newsletter entry is under h3 with an <a>
    posts = soup.select("h3 a")

    # Keep only newsletter-category post links, not nav/menu links
    cleaned = []
    for a in posts:
        href = a.get("href", "").strip()
        title = a.get_text(" ", strip=True)
        if not href or not title:
            continue
        if "/newsletters/" in href:
            cleaned.append((title, href, a))

    if not cleaned:
        print("No newsletter post links found.")
        return

    first_title, first_url, first_a = cleaned[0]

    # Try to get nearby date/snippet text
    parent = first_a.parent
    block_text = ""
    if parent:
        block = parent.find_parent()
        if block:
            block_text = block.get_text(" ", strip=True)

    # extract a date-like string if present
    date_match = re.search(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday,?\s+[A-Za-z]+\s+\d{1,2}(st|nd|rd|th)?,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},\s+\d{4}|[A-Za-z]+\s+\d{1,2}(st|nd|rd|th),\s+\d{4})",
        block_text,
        flags=re.IGNORECASE,
    )
    date_text = date_match.group(0) if date_match else "Date not found"

    print("First newsletter title:")
    print(first_title)
    print("\nFirst newsletter URL:")
    print(first_url)
    print("\nDate text:")
    print(date_text)
    print("\nSnippet:")
    print(block_text[:500])


if __name__ == "__main__":
    main()
