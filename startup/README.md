# Startup Harvester

This folder contains a configurable harvester to collect startup organizations from approved sources and enrich them with career page URLs.

## Quick start
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r startup/requirements.txt
cp startup/config.example.json startup/config.json
python3 startup/harvest_startups.py --config startup/config.json --output startup/output/startups.jsonl
```

## Config overview
Edit `startup/config.json` and add sources you are permitted to access.

Supported source types:
- `csv`: local or hosted CSV with `name`, `website`, `info` columns
- `json`: local or hosted JSON list of objects
- `directory`: HTML directory page with CSS selectors

Example `directory` source fields:
- `url`: page to crawl
- `item_selector`: CSS selector for each company card/row
- `name_selector`: selector for the company name within the card
- `website_selector`: selector for the company website link within the card
- `info_selector`: selector for the description within the card
- `next_page_selector`: selector for pagination link (optional)

## Output
The harvester writes JSONL records with:
- `name`
- `website`
- `info`
- `careers_url`
- `source`, `source_url`
- `collected_at`

## Notes
- Respect `robots.txt` and terms for each source.
- You can disable careers enrichment with `--no-enrich` or set `"enrich_careers": false`.
- If you want an API-based source (Crunchbase, AngelList, etc.), tell me which provider and I will add a connector that reads your API key from env.
