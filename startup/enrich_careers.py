#!/usr/bin/env python3
"""Enrich existing startup records with careers page URLs."""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Optional

from harvest_startups import Fetcher, DEFAULT_USER_AGENT, find_careers_url, load_config, write_outputs


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich startup JSONL with careers pages.")
    parser.add_argument("--config", default="startup/config.json", help="Path to config JSON")
    parser.add_argument("--input", default="data/startups.jsonl", help="Input JSONL path")
    parser.add_argument("--output", default="data/startups_with_careers.jsonl", help="Output JSONL path")
    parser.add_argument("--csv-output", default="", help="Optional CSV output path")
    parser.add_argument("--max", type=int, default=0, help="Max records to process (0 = all)")
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--only-missing", action="store_true", help="Only fill missing careers_url")
    args = parser.parse_args()

    config = load_config(args.config)
    fetcher = Fetcher(
        user_agent=config.get("user_agent", DEFAULT_USER_AGENT),
        rate_limit_seconds=float(config.get("rate_limit_seconds", 1.0)),
        timeout_seconds=float(config.get("timeout_seconds", 15.0)),
        strict_robots=bool(config.get("strict_robots", True)),
    )

    records = read_jsonl(args.input)
    if args.start:
        records = records[args.start :]
    if args.max > 0:
        records = records[: args.max]

    for record in records:
        website = record.get("website")
        if not website:
            record["careers_url"] = None
            continue
        if args.only_missing and record.get("careers_url"):
            continue
        careers_url = find_careers_url(fetcher, website)
        record["careers_url"] = careers_url

    write_outputs(records, args.output, args.csv_output or None)
    print(f"Wrote {len(records)} records to {args.output}")
    if args.csv_output:
        print(f"Wrote CSV to {args.csv_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
