import csv
import json
from datetime import datetime, timezone

from src.config_loader import resolve_repo_path

CSV_SOURCES = {
    "customers_orgs": "customers_orgs.csv",
    "billing_monthly": "billing_monthly.csv",
    "resources": "resources.csv",
    "users": "users.csv",
    "support_tickets": "support_tickets.csv",
    "marketing_touches": "marketing_touches.csv",
    "nps_surveys": "nps_surveys.csv",
}


def ingest_bronze(config):
    """Lee las 7 tablas CSV y los 120 JSONL de data/raw/ agregando _ingest_ts y _source_file."""
    raw_dir = resolve_repo_path(config["paths"]["raw_dir"], config)
    events_raw_dir = resolve_repo_path(config["paths"]["usage_events_raw_dir"], config)
    ingest_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bronze_data = {}
    corrupt_events = []
    usage_events = []

    jsonl_files = sorted(events_raw_dir.glob("events_part_*.jsonl"))
    for fp in jsonl_files:
        rel_source = f"data/raw/usage_events_stream/{fp.name}"
        with fp.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    rec = json.loads(stripped)
                    rec["_ingest_ts"] = ingest_ts
                    rec["_source_file"] = rel_source
                    usage_events.append(rec)
                except json.JSONDecodeError:
                    corrupt_events.append(
                        {
                            "_corrupt_record": stripped,
                            "_source_file": rel_source,
                            "_line_number": line_no,
                            "_ingest_ts": ingest_ts,
                            "quarantine_reason": "CORRUPT_JSON_RECORD",
                        }
                    )

    bronze_data["usage_events"] = usage_events
    bronze_data["usage_events_corrupt"] = corrupt_events
    bronze_data["_jsonl_files_count"] = len(jsonl_files)

    for name, filename in CSV_SOURCES.items():
        fp = raw_dir / filename
        rel_source = f"data/raw/{filename}"
        rows = []
        with fp.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                item = dict(row)
                item["_ingest_ts"] = ingest_ts
                item["_source_file"] = rel_source
                rows.append(item)
        bronze_data[name] = rows

    return bronze_data
