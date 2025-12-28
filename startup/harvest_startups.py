#!/usr/bin/env python3
"""Harvest startup orgs from configurable sources and enrich with careers pages."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html as html_lib
import json
import re
import time
import urllib.parse
import urllib.robotparser
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

DEFAULT_USER_AGENT = "AthenaHarvestBot/1.0 (+contact@example.com)"
STRONG_CAREER_KEYWORDS = [
    "careers",
    "career",
    "jobs",
    "job",
    "open roles",
    "openings",
    "vacancies",
    "join us",
    "work with",
    "work at",
]
WEAK_CAREER_KEYWORDS = [
    "employment",
    "hiring",
]
CAREERS_PATH_PATTERN = re.compile(
    r"/(careers?|jobs?|open-roles|openings|vacancies|join-us|join|work-with|work-at|employment)",
    re.IGNORECASE,
)
CAREERS_EXCLUDE_PATTERN = re.compile(
    r"/(blog|news|press|media|events|guide|learn|resources|legal|privacy|terms|policy)",
    re.IGNORECASE,
)


@dataclass
class Fetcher:
    user_agent: str
    rate_limit_seconds: float
    timeout_seconds: float
    strict_robots: bool

    def __post_init__(self) -> None:
        self._last_request_at = 0.0
        self._robots_cache: Dict[str, urllib.robotparser.RobotFileParser] = {}
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.user_agent})

    def _sleep_if_needed(self) -> None:
        elapsed = time.time() - self._last_request_at
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)

    def _mark_request(self) -> None:
        self._last_request_at = time.time()

    def _robots_for(self, url: str) -> Optional[urllib.robotparser.RobotFileParser]:
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme.startswith("http"):
            return None
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base in self._robots_cache:
            return self._robots_cache[base]
        robots_url = urllib.parse.urljoin(base, "/robots.txt")
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception:
            if self.strict_robots:
                return None
        self._robots_cache[base] = rp
        return rp

    def allowed(self, url: str) -> bool:
        rp = self._robots_for(url)
        if rp is None:
            return not self.strict_robots
        return rp.can_fetch(self.user_agent, url)

    def get_text(self, url: str) -> Optional[str]:
        if not self.allowed(url):
            return None
        self._sleep_if_needed()
        try:
            resp = self._session.get(url, timeout=self.timeout_seconds)
        except requests.RequestException:
            return None
        finally:
            self._mark_request()
        if resp.status_code >= 400:
            return None
        return resp.text

    def head_ok(self, url: str) -> bool:
        if not self.allowed(url):
            return False
        self._sleep_if_needed()
        try:
            resp = self._session.head(url, allow_redirects=True, timeout=self.timeout_seconds)
        except requests.RequestException:
            return False
        finally:
            self._mark_request()
        return resp.status_code < 400


def make_soup(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def parse_inertia_data_page(html_text: str) -> Optional[Dict[str, Any]]:
    soup = make_soup(html_text)
    root = soup.find(attrs={"data-page": True})
    if not root:
        return None
    raw = root.get("data-page")
    if not raw:
        return None
    try:
        return json.loads(html_lib.unescape(raw))
    except json.JSONDecodeError:
        return None


def set_query_param(url: str, key: str, value: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = dict(urllib.parse.parse_qsl(parsed.query))
    query[key] = value
    new_query = urllib.parse.urlencode(query)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def normalize_url(url: str) -> str:
    if not url:
        return url
    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"
    return url


def select_text(node: Any, selector: str) -> Optional[str]:
    if not selector:
        return None
    target = node.select_one(selector)
    if not target:
        return None
    text = target.get_text(" ", strip=True)
    return text or None


def select_attr(node: Any, selector: str, attr: str) -> Optional[str]:
    if not selector:
        return None
    target = node.select_one(selector)
    if not target:
        return None
    value = target.get(attr)
    if not value:
        return None
    return value.strip()


def parse_directory_source(source: Dict[str, Any], fetcher: Fetcher) -> Iterable[Dict[str, Any]]:
    url = source.get("url")
    item_selector = source.get("item_selector")
    if not url or not item_selector:
        return []

    name_selector = source.get("name_selector", "")
    website_selector = source.get("website_selector", "")
    info_selector = source.get("info_selector", "")
    next_selector = source.get("next_page_selector", "")

    while url:
        html = fetcher.get_text(url)
        if not html:
            break
        soup = make_soup(html)
        for item in soup.select(item_selector):
            name = select_text(item, name_selector)
            website = select_attr(item, website_selector, "href")
            info = select_text(item, info_selector)
            if not (name or website or info):
                continue
            yield {
                "name": name,
                "website": normalize_url(website) if website else None,
                "info": info,
                "source": source.get("name", "directory"),
                "source_url": url,
            }

        if not next_selector:
            break
        next_link = soup.select_one(next_selector)
        if not next_link:
            break
        next_href = next_link.get("href")
        if not next_href:
            break
        url = urllib.parse.urljoin(url, next_href)


def parse_csv_source(source: Dict[str, Any], fetcher: Fetcher) -> Iterable[Dict[str, Any]]:
    path = source.get("path")
    url = source.get("url")
    columns = source.get("columns", {})

    if url:
        csv_text = fetcher.get_text(url)
        if not csv_text:
            return []
        reader = csv.DictReader(csv_text.splitlines())
    elif path:
        with open(path, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
    else:
        return []

    for row in reader:
        name = row.get(columns.get("name", "name"))
        website = row.get(columns.get("website", "website"))
        info = row.get(columns.get("info", "info"))
        yield {
            "name": name.strip() if name else None,
            "website": normalize_url(website.strip()) if website else None,
            "info": info.strip() if info else None,
            "source": source.get("name", "csv"),
            "source_url": url or path,
        }


def parse_json_source(source: Dict[str, Any], fetcher: Fetcher) -> Iterable[Dict[str, Any]]:
    path = source.get("path")
    url = source.get("url")
    fields = source.get("fields", {})

    if url:
        text = fetcher.get_text(url)
        if not text:
            return []
        data = json.loads(text)
    elif path:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    else:
        return []

    if isinstance(data, dict):
        items = data.get(source.get("root", ""), data)
    else:
        items = data

    if not isinstance(items, list):
        return []

    for row in items:
        if not isinstance(row, dict):
            continue
        name = row.get(fields.get("name", "name"))
        website = row.get(fields.get("website", "website"))
        info = row.get(fields.get("info", "info"))
        yield {
            "name": name,
            "website": normalize_url(website) if website else None,
            "info": info,
            "source": source.get("name", "json"),
            "source_url": url or path,
        }


def _yc_extract_website(fetcher: Fetcher, company_url: str) -> Optional[str]:
    html = fetcher.get_text(company_url)
    if not html:
        return None
    soup = make_soup(html)
    for anchor in soup.find_all("a", href=True):
        aria = (anchor.get("aria-label") or "").strip().lower()
        if aria == "website":
            return anchor.get("href")
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True).lower()
        if text == "website":
            return anchor.get("href")

    blacklist = {
        "twitter.com",
        "x.com",
        "linkedin.com",
        "facebook.com",
        "instagram.com",
        "youtube.com",
        "crunchbase.com",
        "angel.co",
        "wellfound.com",
        "medium.com",
        "substack.com",
        "forbes.com",
        "techcrunch.com",
        "cnbc.com",
        "bloomberg.com",
        "wsj.com",
        "nytimes.com",
    }

    candidates: List[str] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href")
        if not href or not href.startswith("http"):
            continue
        parsed = urllib.parse.urlparse(href)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        if domain in blacklist:
            continue
        text = anchor.get_text(" ", strip=True)
        text_lower = text.lower()
        if domain and domain in text_lower:
            return href
        if text.strip() == href:
            return href
        if text and " " not in text and "." in text:
            candidates.append(href)
        else:
            candidates.append(href)

    return candidates[0] if candidates else None


def parse_yc_location_source(source: Dict[str, Any], fetcher: Fetcher) -> Iterable[Dict[str, Any]]:
    base_url = source.get("url")
    if not base_url:
        return []

    max_pages = source.get("max_pages")
    fetch_company_pages = bool(source.get("fetch_company_pages", False))

    page = 1
    total_pages = None
    while True:
        page_url = base_url if page == 1 else set_query_param(base_url, "page", str(page))
        html = fetcher.get_text(page_url)
        if not html:
            break
        data = parse_inertia_data_page(html)
        if not data:
            break

        props = data.get("props", {})
        companies = props.get("companies", [])
        total_pages = props.get("totalPages") or total_pages
        for company in companies:
            if not isinstance(company, dict):
                continue
            slug = company.get("slug")
            company_url = company.get("ycdc_company_url") or (
                f"https://www.ycombinator.com/companies/{slug}" if slug else None
            )
            website = company.get("website")
            if not website and fetch_company_pages and company_url:
                website = _yc_extract_website(fetcher, company_url)
            tags = company.get("tags") or []
            yield {
                "name": company.get("name"),
                "website": normalize_url(website) if website else None,
                "info": company.get("one_liner") or company.get("long_description"),
                "source": source.get("name", "ycombinator"),
                "source_url": page_url,
                "yc_url": company_url,
                "batch": (company.get("batch_name") or "").upper() or None,
                "status": company.get("ycdc_status"),
                "employees": str(company.get("team_size")) if company.get("team_size") else None,
                "location": company.get("location") or company.get("city"),
                "tags": ", ".join(tags) if tags else None,
                "linkedin_url": company.get("linkedin_url"),
                "twitter_url": company.get("twitter_url"),
                "cb_url": company.get("cb_url"),
            }

        if total_pages is None or page >= total_pages:
            break
        page += 1
        if max_pages and page > max_pages:
            break


def is_career_link(text: str, href: str) -> bool:
    if not (text or href):
        return False
    href_lower = (href or "").lower()
    text_lower = (text or "").lower()
    if CAREERS_EXCLUDE_PATTERN.search(href_lower):
        return False
    if CAREERS_PATH_PATTERN.search(href_lower):
        return True
    if any(keyword in text_lower for keyword in STRONG_CAREER_KEYWORDS):
        return True
    if any(keyword in text_lower for keyword in WEAK_CAREER_KEYWORDS):
        return bool(CAREERS_PATH_PATTERN.search(href_lower))
    return False


def _page_looks_like_careers(html: str) -> bool:
    soup = make_soup(html)
    title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(" ", strip=True).lower() if h1 else ""
    blob = f"{title} {h1_text}"
    return any(keyword in blob for keyword in STRONG_CAREER_KEYWORDS)


def find_careers_url(fetcher: Fetcher, website: str) -> Optional[str]:
    if not website:
        return None

    homepage = normalize_url(website)
    html = fetcher.get_text(homepage)
    if not html:
        return None
    soup = make_soup(html)
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True)
        href = anchor.get("href")
        if not href:
            continue
        if is_career_link(text, href):
            return urllib.parse.urljoin(homepage, href)

    for path in ["/careers", "/careers/", "/jobs", "/jobs/", "/join", "/join-us", "/company/careers"]:
        probe = urllib.parse.urljoin(homepage, path)
        html_probe = fetcher.get_text(probe)
        if html_probe and _page_looks_like_careers(html_probe):
            return probe
    return None


def harvest_sources(
    config: Dict[str, Any], fetcher: Fetcher, max_records: Optional[int] = None
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for source in config.get("sources", []):
        source_type = source.get("type")
        if source_type == "directory":
            records = parse_directory_source(source, fetcher)
        elif source_type == "csv":
            records = parse_csv_source(source, fetcher)
        elif source_type == "json":
            records = parse_json_source(source, fetcher)
        elif source_type == "yc_location":
            records = parse_yc_location_source(source, fetcher)
        else:
            print(f"Skipping unknown source type: {source_type}")
            continue

        for record in records:
            results.append(record)
            if max_records is not None and len(results) >= max_records:
                return results
    return results


def dedupe_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[Tuple[Optional[str], Optional[str]]] = set()
    unique: List[Dict[str, Any]] = []
    for record in records:
        key = (record.get("name"), record.get("website"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def write_outputs(records: List[Dict[str, Any]], output_path: str, csv_path: Optional[str]) -> None:
    with open(output_path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=True) + "\n")

    if csv_path:
        base_fields = ["name", "website", "info", "careers_url", "source", "source_url", "collected_at"]
        extra_fields = sorted({key for record in records for key in record.keys()} - set(base_fields))
        fieldnames = base_fields + extra_fields
        with open(csv_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)


def main() -> int:
    parser = argparse.ArgumentParser(description="Harvest startup organizations and careers pages.")
    parser.add_argument("--config", default="startup/config.json", help="Path to config JSON")
    parser.add_argument("--output", default="startup/output/startups.jsonl", help="Output JSONL path")
    parser.add_argument("--csv-output", default="", help="Optional CSV output path")
    parser.add_argument("--max", type=int, default=0, help="Max records to emit (0 = no limit)")
    parser.add_argument("--no-enrich", action="store_true", help="Skip careers page enrichment")
    args = parser.parse_args()

    config = load_config(args.config)
    fetcher = Fetcher(
        user_agent=config.get("user_agent", DEFAULT_USER_AGENT),
        rate_limit_seconds=float(config.get("rate_limit_seconds", 1.0)),
        timeout_seconds=float(config.get("timeout_seconds", 15.0)),
        strict_robots=bool(config.get("strict_robots", True)),
    )

    max_records = args.max if args.max > 0 else None
    records = harvest_sources(config, fetcher, max_records=max_records)
    records = dedupe_records(records)

    if not args.no_enrich and config.get("enrich_careers", True):
        for record in records:
            careers_url = find_careers_url(fetcher, record.get("website"))
            record["careers_url"] = careers_url

    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for record in records:
        record.setdefault("careers_url", None)
        record["collected_at"] = now

    if args.max > 0:
        records = records[: args.max]

    write_outputs(records, args.output, args.csv_output or None)
    print(f"Wrote {len(records)} records to {args.output}")
    if args.csv_output:
        print(f"Wrote CSV to {args.csv_output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
