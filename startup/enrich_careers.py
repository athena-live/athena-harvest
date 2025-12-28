#!/usr/bin/env python3
"""Enrich existing startup records with careers page URLs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from harvest_startups import Fetcher, DEFAULT_USER_AGENT, find_careers_url, load_config


def read_jsonl(path: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def append_jsonl(path: str, records: List[Dict[str, Any]]) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_csv(path: str, records: List[Dict[str, Any]]) -> None:
    import csv

    if not records:
        return
    base_fields = ["name", "website", "info", "careers_url", "source", "source_url", "collected_at"]
    extra_fields = sorted({key for record in records for key in record.keys()} - set(base_fields))
    fieldnames = base_fields + extra_fields
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def load_progress(path: str) -> int:
    file_path = Path(path)
    if not file_path.exists():
        return 0
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return 0
    return int(data.get("next_index", 0))


def save_progress(path: str, next_index: int) -> None:
    payload = {"next_index": next_index}
    Path(path).write_text(json.dumps(payload, ensure_ascii=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich startup JSONL with careers pages.")
    parser.add_argument("--config", default="startup/config.json", help="Path to config JSON")
    parser.add_argument("--input", default="data/startups.jsonl", help="Input JSONL path")
    parser.add_argument("--output", default="data/startups_with_careers.jsonl", help="Output JSONL path")
    parser.add_argument("--csv-output", default="", help="Optional CSV output path")
    parser.add_argument("--max", type=int, default=0, help="Max records to process (0 = all)")
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--only-missing", action="store_true", help="Only fill missing careers_url")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from progress file and append to output JSONL",
    )
    parser.add_argument(
        "--progress-file",
        default="data/enrich_progress.json",
        help="Progress file path",
    )
    parser.add_argument(
        "--build-csv",
        action="store_true",
        help="Build CSV from the output JSONL and exit",
    )
    parser.add_argument(
        "--only-with-careers",
        action="store_true",
        help="Only write records that have a careers_url",
    )
    args = parser.parse_args()

    if args.build_csv:
        records = read_jsonl(args.output)
        if args.csv_output:
            write_csv(args.csv_output, records)
            print(f"Wrote CSV to {args.csv_output}")
        else:
            print("No --csv-output provided; nothing to do.")
        return 0

    config = load_config(args.config)
    fetcher = Fetcher(
        user_agent=config.get("user_agent", DEFAULT_USER_AGENT),
        rate_limit_seconds=float(config.get("rate_limit_seconds", 1.0)),
        timeout_seconds=float(config.get("timeout_seconds", 15.0)),
        strict_robots=bool(config.get("strict_robots", True)),
    )

    records = read_jsonl(args.input)
    start_index = args.start
    if args.resume:
        start_index = load_progress(args.progress_file)
    if start_index:
        records = records[start_index:]
    if args.max > 0:
        records = records[: args.max]

    processed: List[Dict[str, Any]] = []
    for record in records:
        website = record.get("website")
        if not website:
            record["careers_url"] = None
        elif args.only_missing and record.get("careers_url"):
            pass
        else:
            careers_url = find_careers_url(fetcher, website)
            record["careers_url"] = careers_url
        if args.only_with_careers and not record.get("careers_url"):
            continue
        processed.append(record)

    if args.resume:
        append_jsonl(args.output, processed)
        save_progress(args.progress_file, start_index + len(processed))
        print(f"Appended {len(processed)} records to {args.output}")
    else:
        with open(args.output, "w", encoding="utf-8") as fh:
            for record in processed:
                fh.write(json.dumps(record, ensure_ascii=True) + "\n")
        print(f"Wrote {len(processed)} records to {args.output}")

    if args.csv_output:
        write_csv(args.csv_output, read_jsonl(args.output))
        print(f"Wrote CSV to {args.csv_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
