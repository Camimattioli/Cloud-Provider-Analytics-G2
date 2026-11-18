import json
from datetime import datetime, timedelta, timezone

from src.config_loader import load_pipeline_config, resolve_repo_path
from src.quality_rules import evaluate_usage_event_quality


def _parse_utc(ts_str):
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def _fmt_utc(dt):
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_microbatches(jsonl_files, max_files_per_trigger, delay_hours, checkpoint_dir, reset_checkpoint=True):
    """Procesa los archivos JSONL por micro-lotes aplicando la regla de watermark y guardando checkpoints."""
    offsets_dir = checkpoint_dir / "offsets"
    commits_dir = checkpoint_dir / "commits"
    state_file = checkpoint_dir / "state_metadata.json"

    if reset_checkpoint and checkpoint_dir.exists():
        for p in sorted(checkpoint_dir.rglob("*"), reverse=True):
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                p.rmdir()

    offsets_dir.mkdir(parents=True, exist_ok=True)
    commits_dir.mkdir(parents=True, exist_ok=True)

    committed_batches = set()
    for cf in commits_dir.glob("batch_*.json"):
        try:
            committed_batches.add(int(cf.stem.split("_")[1]))
        except (IndexError, ValueError):
            pass

    if state_file.exists() and committed_batches:
        saved = json.loads(state_file.read_text(encoding="utf-8"))
        max_event_ts = _parse_utc(saved["max_event_ts"]) if saved.get("max_event_ts") else None
        current_watermark = _parse_utc(saved["current_watermark"]) if saved.get("current_watermark") else None
        seen_event_ids = set(saved.get("seen_event_ids", []))
        accepted_events = int(saved.get("accepted_events", 0))
        dropped_late_events = int(saved.get("dropped_late_events", 0))
        quarantined_events = int(saved.get("quarantined_events", 0))
        duplicate_events_dropped = int(saved.get("duplicate_events_dropped", 0))
        accepted_cost_usd = float(saved.get("accepted_cost_usd", 0.0))
        accepted_carbon_kg = float(saved.get("accepted_carbon_kg", 0.0))
        accepted_genai_tokens = int(saved.get("accepted_genai_tokens", 0))
        unit_imputed_count = int(saved.get("unit_imputed_count", 0))
        trigger_logs = list(saved.get("trigger_logs", []))
    else:
        max_event_ts = None
        current_watermark = None
        seen_event_ids = set()
        accepted_events = 0
        dropped_late_events = 0
        quarantined_events = 0
        duplicate_events_dropped = 0
        accepted_cost_usd = 0.0
        accepted_carbon_kg = 0.0
        accepted_genai_tokens = 0
        unit_imputed_count = 0
        trigger_logs = []

    delay_td = timedelta(hours=delay_hours)
    batches = [
        jsonl_files[i : i + max_files_per_trigger]
        for i in range(0, len(jsonl_files), max_files_per_trigger)
    ]

    newly_executed_triggers = 0
    skipped_committed_triggers = 0

    for batch_id, batch_files in enumerate(batches):
        if batch_id in committed_batches:
            skipped_committed_triggers += 1
            continue

        newly_executed_triggers += 1
        offset_payload = {
            "batch_id": batch_id,
            "files": [f.name for f in batch_files],
            "watermark_before_batch": _fmt_utc(current_watermark),
        }
        (offsets_dir / f"batch_{batch_id:04d}.json").write_text(
            json.dumps(offset_payload, indent=2), encoding="utf-8"
        )

        batch_input_rows = 0
        batch_accepted = 0
        batch_late_dropped = 0
        batch_quarantined = 0
        batch_duplicates = 0
        batch_cost_usd = 0.0
        batch_min_ts = None
        batch_max_ts = None

        for fp in batch_files:
            with fp.open("r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    batch_input_rows += 1
                    raw_ev = json.loads(s)
                    ev_dt = _parse_utc(raw_ev["timestamp"])
                    if batch_min_ts is None or ev_dt < batch_min_ts:
                        batch_min_ts = ev_dt
                    if batch_max_ts is None or ev_dt > batch_max_ts:
                        batch_max_ts = ev_dt
                    if max_event_ts is None or ev_dt > max_event_ts:
                        max_event_ts = ev_dt

                    is_valid, _q_reason, cleaned = evaluate_usage_event_quality(raw_ev, min_valid_cost=0.0)
                    if not is_valid:
                        quarantined_events += 1
                        batch_quarantined += 1
                        continue

                    if current_watermark is not None and ev_dt < current_watermark:
                        dropped_late_events += 1
                        batch_late_dropped += 1
                        continue

                    ev_id = cleaned["event_id"]
                    if ev_id in seen_event_ids:
                        duplicate_events_dropped += 1
                        batch_duplicates += 1
                        continue

                    seen_event_ids.add(ev_id)
                    accepted_events += 1
                    batch_accepted += 1
                    c_val = float(cleaned["cost_usd_increment"])
                    accepted_cost_usd += c_val
                    batch_cost_usd += c_val
                    accepted_carbon_kg += float(cleaned["carbon_kg"] or 0.0)
                    accepted_genai_tokens += int(cleaned["genai_tokens"] or 0)
                    if cleaned["is_unit_imputed"]:
                        unit_imputed_count += 1

        watermark_before = _fmt_utc(current_watermark)
        if max_event_ts is not None:
            candidate_wm = max_event_ts - delay_td
            if current_watermark is None or candidate_wm > current_watermark:
                current_watermark = candidate_wm
        watermark_after = _fmt_utc(current_watermark)

        trigger_logs.append(
            {
                "batch_id": batch_id,
                "files_processed": f"{batch_files[0].name}..{batch_files[-1].name}",
                "files_count": len(batch_files),
                "input_rows": batch_input_rows,
                "batch_min_event_ts": _fmt_utc(batch_min_ts),
                "batch_max_event_ts": _fmt_utc(batch_max_ts),
                "watermark_before_trigger": watermark_before,
                "watermark_after_trigger": watermark_after,
                "accepted_rows": batch_accepted,
                "quarantined_negative_cost_rows": batch_quarantined,
                "dropped_by_watermark_rows": batch_late_dropped,
                "dropped_duplicates_rows": batch_duplicates,
                "batch_accepted_cost_usd": round(batch_cost_usd, 4),
                "state_store_unique_ids_tracked": len(seen_event_ids),
            }
        )

        (commits_dir / f"batch_{batch_id:04d}.json").write_text(
            json.dumps({"batch_id": batch_id, "status": "COMMITTED"}, indent=2),
            encoding="utf-8",
        )

    state_summary = {
        "max_event_ts": _fmt_utc(max_event_ts),
        "current_watermark": _fmt_utc(current_watermark),
        "seen_event_ids": list(seen_event_ids),
        "accepted_events": accepted_events,
        "dropped_late_events": dropped_late_events,
        "quarantined_events": quarantined_events,
        "duplicate_events_dropped": duplicate_events_dropped,
        "accepted_cost_usd": round(accepted_cost_usd, 4),
        "accepted_carbon_kg": round(accepted_carbon_kg, 4),
        "accepted_genai_tokens": accepted_genai_tokens,
        "unit_imputed_count": unit_imputed_count,
        "trigger_logs": trigger_logs,
    }
    state_file.write_text(json.dumps(state_summary, indent=2), encoding="utf-8")

    total_raw = accepted_events + dropped_late_events + quarantined_events + duplicate_events_dropped
    valid_pool = accepted_events + dropped_late_events
    return {
        "delay_hours": delay_hours,
        "total_triggers": len(batches),
        "newly_executed_triggers": newly_executed_triggers,
        "skipped_committed_triggers": skipped_committed_triggers,
        "total_raw_events": total_raw,
        "quarantined_negative_cost_events": quarantined_events,
        "valid_candidate_events": valid_pool,
        "accepted_events": accepted_events,
        "dropped_by_watermark_events": dropped_late_events,
        "duplicate_events_dropped": duplicate_events_dropped,
        "valid_retention_pct": round(100.0 * accepted_events / valid_pool, 2) if valid_pool else 0.0,
        "accepted_cost_usd": round(accepted_cost_usd, 4),
        "accepted_carbon_kg": round(accepted_carbon_kg, 4),
        "accepted_genai_tokens": accepted_genai_tokens,
        "unit_imputed_count": unit_imputed_count,
        "final_watermark_utc": _fmt_utc(current_watermark),
        "trigger_progression": trigger_logs,
    }


def run_streaming_pipeline(config_path=None):
    """Compara los perfiles de watermark sobre los 120 JSONL y prueba el checkpoint."""
    config = load_pipeline_config(config_path)
    events_raw_dir = resolve_repo_path(config["paths"]["usage_events_raw_dir"], config)
    checkpoints_dir = resolve_repo_path(config["paths"]["checkpoints_dir"], config)
    evidence_dir = resolve_repo_path(config["paths"]["evidence_dir"], config)

    jsonl_files = sorted(events_raw_dir.glob("events_part_*.jsonl"))
    max_files = int(config["streaming"].get("max_files_per_trigger", 10))

    ckpt_replay_full = checkpoints_dir / "replay_full_1440h"
    res_1440h = run_microbatches(
        jsonl_files=jsonl_files,
        max_files_per_trigger=max_files,
        delay_hours=1440.0,
        checkpoint_dir=ckpt_replay_full,
        reset_checkpoint=True,
    )

    res_1440h_rerun = run_microbatches(
        jsonl_files=jsonl_files,
        max_files_per_trigger=max_files,
        delay_hours=1440.0,
        checkpoint_dir=ckpt_replay_full,
        reset_checkpoint=False,
    )

    ckpt_replay_p95 = checkpoints_dir / "replay_p95_1368h"
    res_1368h = run_microbatches(
        jsonl_files=jsonl_files,
        max_files_per_trigger=max_files,
        delay_hours=1368.0,
        checkpoint_dir=ckpt_replay_p95,
        reset_checkpoint=True,
    )

    ckpt_premature_1h = checkpoints_dir / "premature_e1_1h"
    res_1h = run_microbatches(
        jsonl_files=jsonl_files,
        max_files_per_trigger=max_files,
        delay_hours=1.0,
        checkpoint_dir=ckpt_premature_1h,
        reset_checkpoint=True,
    )

    for ckpt in (ckpt_replay_full, ckpt_replay_p95, ckpt_premature_1h):
        sf = ckpt / "state_metadata.json"
        if sf.exists():
            st = json.loads(sf.read_text(encoding="utf-8"))
            st["seen_event_ids_count"] = len(st.pop("seen_event_ids", []))
            sf.write_text(json.dumps(st, indent=2), encoding="utf-8")

    report = {
        "stream_source": {
            "path": "data/raw/usage_events_stream/*.jsonl",
            "total_files": len(jsonl_files),
            "max_files_per_trigger": max_files,
            "total_microbatch_triggers": len(jsonl_files) // max_files,
            "events_per_trigger": (len(jsonl_files) * 360) // (len(jsonl_files) // max_files),
        },
        "watermark_profiles_comparison": {
            "premature_1h_entrega1": {
                "delay_threshold": "1 hour",
                "accepted_valid_events": res_1h["accepted_events"],
                "quarantined_negative_cost_events": res_1h["quarantined_negative_cost_events"],
                "dropped_by_watermark_events": res_1h["dropped_by_watermark_events"],
                "valid_retention_pct": res_1h["valid_retention_pct"],
                "accepted_cost_usd": res_1h["accepted_cost_usd"],
                "cost_lost_by_premature_watermark_usd": round(
                    res_1440h["accepted_cost_usd"] - res_1h["accepted_cost_usd"], 4
                ),
            },
            "calibrated_p95_1368h_57d": {
                "delay_threshold": "1368 hours (57 days)",
                "accepted_valid_events": res_1368h["accepted_events"],
                "quarantined_negative_cost_events": res_1368h["quarantined_negative_cost_events"],
                "dropped_by_watermark_events": res_1368h["dropped_by_watermark_events"],
                "valid_retention_pct": res_1368h["valid_retention_pct"],
                "accepted_cost_usd": res_1368h["accepted_cost_usd"],
            },
            "calibrated_replay_full_1440h_60d": {
                "delay_threshold": "1440 hours (60 days)",
                "accepted_valid_events": res_1440h["accepted_events"],
                "quarantined_negative_cost_events": res_1440h["quarantined_negative_cost_events"],
                "dropped_by_watermark_events": res_1440h["dropped_by_watermark_events"],
                "valid_retention_pct": res_1440h["valid_retention_pct"],
                "accepted_cost_usd": res_1440h["accepted_cost_usd"],
                "accepted_carbon_kg": res_1440h["accepted_carbon_kg"],
                "accepted_genai_tokens": res_1440h["accepted_genai_tokens"],
                "unit_imputed_count": res_1440h["unit_imputed_count"],
            },
        },
        "checkpoint_fault_tolerance_and_idempotency": {
            "checkpoint_location": "data/datalake/checkpoints/replay_full_1440h",
            "initial_run_executed_triggers": res_1440h["newly_executed_triggers"],
            "rerun_executed_triggers": res_1440h_rerun["newly_executed_triggers"],
            "rerun_skipped_committed_triggers": res_1440h_rerun["skipped_committed_triggers"],
            "initial_accepted_events": res_1440h["accepted_events"],
            "rerun_accepted_events": res_1440h_rerun["accepted_events"],
            "initial_accepted_cost_usd": res_1440h["accepted_cost_usd"],
            "rerun_accepted_cost_usd": res_1440h_rerun["accepted_cost_usd"],
            "matches_batch_gold_cost_exactly": abs(res_1440h["accepted_cost_usd"] - 148351.6009) < 1e-4,
            "is_checkpoint_idempotent": (
                res_1440h_rerun["newly_executed_triggers"] == 0
                and res_1440h_rerun["skipped_committed_triggers"] == 12
                and res_1440h["accepted_events"] == res_1440h_rerun["accepted_events"]
            ),
        },
        "replay_full_12_triggers_progression": res_1440h["trigger_progression"],
        "premature_1h_12_triggers_progression": res_1h["trigger_progression"],
    }

    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_file = evidence_dir / "streaming_pipeline_resumen.json"
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


if __name__ == "__main__":
    rep = run_streaming_pipeline()
    print(json.dumps(rep["watermark_profiles_comparison"], indent=2, ensure_ascii=False))
    print(json.dumps(rep["checkpoint_fault_tolerance_and_idempotency"], indent=2, ensure_ascii=False))
