#!/usr/bin/env python3
"""Generate per-URL Codex command text files for custom job scrapers."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable
from urllib.parse import urlparse


def read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower())
    return text.strip("-") or "unknown"


def make_filename(index: int, url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.replace("www.", "")
    path = slugify(parsed.path)
    return f"{index:04d}_{slugify(host)}_{path}.txt"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Codex command text files per careers URL.")
    parser.add_argument(
        "--input",
        default="data/startups_with_careers_only.jsonl",
        help="Input JSONL with careers_url",
    )
    parser.add_argument(
        "--output-dir",
        default="data/codex_commands",
        help="Directory to write command files",
    )
    parser.add_argument(
        "--command-template",
        default="create custom script for {URL} to scrape all jobs data and load into database; put in backend/manual_scripts folder",
        help="Command template with {URL} placeholder",
    )
    parser.add_argument("--max", type=int, default=0, help="Max records to process (0 = all)")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in read_jsonl(args.input):
        url = record.get("careers_url")
        if not url:
            continue
        count += 1
        filename = make_filename(count, url)
        content = args.command_template.replace("{URL}", url)
        (out_dir / filename).write_text(content + "\n", encoding="utf-8")
        if args.max and count >= args.max:
            break

    print(f"Wrote {count} command files to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
