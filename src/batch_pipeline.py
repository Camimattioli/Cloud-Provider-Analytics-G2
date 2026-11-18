import csv
import json

from src.bronze_ingestion import ingest_bronze
from src.config_loader import load_pipeline_config, resolve_repo_path
from src.gold_marts import build_gold_marts
from src.silver_transforms import transform_silver


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            out = {}
            for k in fieldnames:
                v = r.get(k)
                out[k] = "" if v is None else v
            writer.writerow(out)


def _run_pass(config):
    bronze_data = ingest_bronze(config)
    silver_data = transform_silver(bronze_data, config)
    gold_data = build_gold_marts(silver_data, config)
    return {
        "bronze": bronze_data,
        "silver": silver_data,
        "gold": gold_data,
    }


def run_batch_pipeline(config_path=None):
    """Corre las etapas Bronze -> Silver -> Gold + Quarantine y guarda los CSV y el resumen."""
    config = load_pipeline_config(config_path)
    gold_dir = resolve_repo_path(config["paths"]["gold_dir"], config)
    quarantine_dir = resolve_repo_path(config["paths"]["quarantine_dir"], config)
    evidence_dir = resolve_repo_path(config["paths"]["evidence_dir"], config)

    # Corremos dos veces seguidas para comprobar que la salida es idempotente
    pass1 = _run_pass(config)
    pass2 = _run_pass(config)

    gold1 = pass1["gold"]
    gold2 = pass2["gold"]
    silver1 = pass1["silver"]
    quarantine1 = silver1["quarantine"]

    _write_csv(gold_dir / "finops_org_service_daily.csv", gold1["finops_org_service_daily"])
    _write_csv(gold_dir / "finops_org_service_monthly.csv", gold1["finops_org_service_monthly"])
    _write_csv(gold_dir / "support_org_sla_severity.csv", gold1["support_org_sla_severity"])
    _write_csv(gold_dir / "product_genai_adoption_daily.csv", gold1["product_genai_adoption_daily"])
    _write_csv(gold_dir / "cost_anomalies_by_org.csv", gold1["cost_anomalies_by_org"])

    _write_csv(quarantine_dir / "usage_events_quarantine.csv", quarantine1["usage_events"])
    _write_csv(quarantine_dir / "nps_surveys_quarantine.csv", quarantine1["nps_surveys"])
    _write_csv(quarantine_dir / "support_tickets_csat_audit.csv", quarantine1["support_tickets_csat_audit"])
    _write_csv(quarantine_dir / "customers_orgs_nps_audit.csv", quarantine1["customers_orgs_nps_audit"])
    _write_csv(quarantine_dir / "users_login_audit.csv", quarantine1["users_login_audit"])

    total_cost_pass1 = round(sum(float(e["cost_usd_increment"]) for e in silver1["usage_events"]), 4)
    total_cost_pass2 = round(sum(float(e["cost_usd_increment"]) for e in pass2["silver"]["usage_events"]), 4)
    total_carbon_pass1 = round(sum(float(e["carbon_kg"] or 0.0) for e in silver1["usage_events"]), 4)
    total_tokens_pass1 = sum(int(e["genai_tokens"] or 0) for e in silver1["usage_events"])

    mad_spikes_total = sum(1 for e in silver1["usage_events"] if e["is_mad_spike"])
    zscore_spikes_total = sum(1 for e in silver1["usage_events"] if e["is_zscore_spike"])
    both_spikes = sum(1 for e in silver1["usage_events"] if e["is_mad_spike"] and e["is_zscore_spike"])

    partitions_summary = {}
    for ev in silver1["usage_events"]:
        p_key = f"year={ev['year']}/month={ev['month']:02d}"
        if p_key not in partitions_summary:
            partitions_summary[p_key] = {"events_count": 0, "total_cost_usd": 0.0, "total_carbon_kg": 0.0}
        partitions_summary[p_key]["events_count"] += 1
        partitions_summary[p_key]["total_cost_usd"] += float(ev["cost_usd_increment"])
        partitions_summary[p_key]["total_carbon_kg"] += float(ev["carbon_kg"] or 0.0)
    for p_val in partitions_summary.values():
        p_val["total_cost_usd"] = round(p_val["total_cost_usd"], 4)
        p_val["total_carbon_kg"] = round(p_val["total_carbon_kg"], 4)

    cost_by_org = {}
    for r in gold1["finops_org_service_monthly"]:
        oid = r["org_id"]
        if oid not in cost_by_org:
            cost_by_org[oid] = {
                "org_id": oid,
                "org_name": r["org_name"],
                "plan": r["plan"],
                "events_count": 0,
                "total_cost_usd": 0.0,
                "total_carbon_kg": 0.0,
                "mad_spike_count": 0,
            }
        cost_by_org[oid]["events_count"] += r["events_count"]
        cost_by_org[oid]["total_cost_usd"] += r["total_cost_usd"]
        cost_by_org[oid]["total_carbon_kg"] += r["total_carbon_kg"]
        cost_by_org[oid]["mad_spike_count"] += r["mad_spike_count"]

    top_5_orgs = sorted(cost_by_org.values(), key=lambda x: x["total_cost_usd"], reverse=True)[:5]
    for item in top_5_orgs:
        item["total_cost_usd"] = round(item["total_cost_usd"], 4)
        item["total_carbon_kg"] = round(item["total_carbon_kg"], 4)

    summary_report = {
        "bronze_counts": {
            "usage_events_jsonl_files": pass1["bronze"]["_jsonl_files_count"],
            "usage_events_raw_rows": len(pass1["bronze"]["usage_events"]),
            "usage_events_corrupt_json_rows": len(pass1["bronze"]["usage_events_corrupt"]),
            "customers_orgs_raw_rows": len(pass1["bronze"]["customers_orgs"]),
            "billing_monthly_raw_rows": len(pass1["bronze"]["billing_monthly"]),
            "resources_raw_rows": len(pass1["bronze"]["resources"]),
            "users_raw_rows": len(pass1["bronze"]["users"]),
            "support_tickets_raw_rows": len(pass1["bronze"]["support_tickets"]),
            "marketing_touches_raw_rows": len(pass1["bronze"]["marketing_touches"]),
            "nps_surveys_raw_rows": len(pass1["bronze"]["nps_surveys"]),
        },
        "silver_and_quarantine_counts": {
            "usage_events_silver_valid": len(silver1["usage_events"]),
            "usage_events_quarantined_negative_cost": len(quarantine1["usage_events"]),
            "usage_events_unit_imputed_count": sum(1 for e in silver1["usage_events"] if e["is_unit_imputed"]),
            "usage_events_value_null_preserved_count": sum(1 for e in silver1["usage_events"] if e["is_value_null"]),
            "customers_orgs_silver": len(silver1["customers_orgs"]),
            "customers_orgs_invalid_nps_nullified": len(quarantine1["customers_orgs_nps_audit"]),
            "resources_silver": len(silver1["resources"]),
            "billing_monthly_silver": len(silver1["billing_monthly"]),
            "billing_monthly_credits_imputed": sum(1 for b in silver1["billing_monthly"] if b["is_credits_imputed"]),
            "billing_monthly_credit_notes_preserved": sum(1 for b in silver1["billing_monthly"] if b["is_credit_note"]),
            "support_tickets_silver": len(silver1["support_tickets"]),
            "support_tickets_csat_nullified_total": len(quarantine1["support_tickets_csat_audit"]),
            "users_silver": len(silver1["users"]),
            "users_inconsistent_login_nullified": len(quarantine1["users_login_audit"]),
            "marketing_touches_silver": len(silver1["marketing_touches"]),
            "nps_surveys_silver_valid": len(silver1["nps_surveys"]),
            "nps_surveys_quarantined_empty": len(quarantine1["nps_surveys"]),
            "referential_integrity_join_audit": silver1["join_audit"],
            "silver_usage_events_partitions": partitions_summary,
        },
        "anomaly_detection_comparison": {
            "grouping_grain": ["service", "metric"],
            "mad_modified_zscore_threshold": 3.5,
            "classic_zscore_threshold": 3.0,
            "mad_spikes_detected": mad_spikes_total,
            "classic_zscore_spikes_detected": zscore_spikes_total,
            "spikes_detected_by_both": both_spikes,
            "stats_by_service_metric": silver1["anomaly_stats_by_service_metric"],
        },
        "gold_marts_summary": {
            "finops_org_service_daily_rows": len(gold1["finops_org_service_daily"]),
            "finops_org_service_monthly_rows": len(gold1["finops_org_service_monthly"]),
            "support_org_sla_severity_rows": len(gold1["support_org_sla_severity"]),
            "product_genai_adoption_daily_rows": len(gold1["product_genai_adoption_daily"]),
            "cost_anomalies_by_org_rows": len(gold1["cost_anomalies_by_org"]),
            "total_valid_cost_usd": total_cost_pass1,
            "total_carbon_kg": total_carbon_pass1,
            "total_genai_tokens": total_tokens_pass1,
            "top_5_orgs_by_cost_usd": top_5_orgs,
        },
        "idempotency_verification": {
            "consecutive_runs_compared": 2,
            "finops_daily_rows_equal": len(gold1["finops_org_service_daily"])
            == len(gold2["finops_org_service_daily"]),
            "total_cost_usd_pass1": total_cost_pass1,
            "total_cost_usd_pass2": total_cost_pass2,
            "cost_difference_usd": round(abs(total_cost_pass1 - total_cost_pass2), 8),
            "quarantine_rows_equal": len(quarantine1["usage_events"])
            == len(pass2["silver"]["quarantine"]["usage_events"]),
            "is_idempotent": (
                len(gold1["finops_org_service_daily"]) == len(gold2["finops_org_service_daily"])
                and abs(total_cost_pass1 - total_cost_pass2) < 1e-9
            ),
        },
    }

    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_json = evidence_dir / "batch_pipeline_resumen.json"
    out_json.write_text(json.dumps(summary_report, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary_report


if __name__ == "__main__":
    rep = run_batch_pipeline()
    print(json.dumps(rep["gold_marts_summary"], indent=2, ensure_ascii=False))
    print("Idempotencia verificada:", rep["idempotency_verification"]["is_idempotent"])
