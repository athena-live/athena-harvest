#!/usr/bin/env python3
"""Harvest startup orgs from configurable sources and enrich with careers pages."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import time
import urllib.parse
import urllib.robotparser
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

DEFAULT_USER_AGENT = "AthenaHarvestBot/1.0 (+contact@example.com)"
CAREER_KEYWORDS = [
    "careers",
    "career",
    "jobs",
    "job",
    "join us",
    "join",
    "work with",
    "work at",
    "open roles",
    "openings",
    "vacancies",
    "employment",
    "hiring",
]


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
        soup = BeautifulSoup(html, "lxml")
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


def is_career_link(text: str, href: str) -> bool:
    if not (text or href):
        return False
    blob = f"{text} {href}".lower()
    return any(keyword in blob for keyword in CAREER_KEYWORDS)


def find_careers_url(fetcher: Fetcher, website: str) -> Optional[str]:
    if not website:
        return None

    homepage = normalize_url(website)
    html = fetcher.get_text(homepage)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True)
        href = anchor.get("href")
        if not href:
            continue
        if is_career_link(text, href):
            return urllib.parse.urljoin(homepage, href)

    for path in ["/careers", "/careers/", "/jobs", "/jobs/", "/join", "/join-us", "/company/careers"]:
        probe = urllib.parse.urljoin(homepage, path)
        if fetcher.head_ok(probe):
            return probe
    return None


def harvest_sources(config: Dict[str, Any], fetcher: Fetcher) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for source in config.get("sources", []):
        source_type = source.get("type")
        if source_type == "directory":
            records = parse_directory_source(source, fetcher)
        elif source_type == "csv":
            records = parse_csv_source(source, fetcher)
        elif source_type == "json":
            records = parse_json_source(source, fetcher)
        else:
            print(f"Skipping unknown source type: {source_type}")
            continue

        for record in records:
            results.append(record)
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
        fieldnames = ["name", "website", "info", "careers_url", "source", "source_url", "collected_at"]
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

    records = harvest_sources(config, fetcher)
    records = dedupe_records(records)

    if not args.no_enrich and config.get("enrich_careers", True):
        for record in records:
            careers_url = find_careers_url(fetcher, record.get("website"))
            record["careers_url"] = careers_url

    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
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
