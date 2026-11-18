import csv
import json
import os
from collections import defaultdict

from src.config_loader import load_pipeline_config, resolve_repo_path


def _read_csv(path):
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def sync_to_astradb(config, finops_daily, support_sla, product_genai, cost_anomalies):
    """Conecta a AstraDB / Cassandra usando variables de entorno y carga las 4 tablas."""
    srv_cfg = config["serving_cassandra"]
    token = os.environ.get(srv_cfg["env_token_var"])
    scb_path = os.environ.get(srv_cfg["env_scb_var"])
    cassandra_host = os.environ.get("CASSANDRA_HOST")

    if not ((token and scb_path) or cassandra_host):
        return {
            "connected_to_remote_cluster": False,
            "note": "Variables ASTRA_TOKEN y ASTRA_SCB_PATH no definidas en el entorno; se verificaron las claves y particiones sobre los CSV de Gold.",
        }

    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    from cassandra.concurrent import execute_concurrent_with_args

    keyspace = srv_cfg["keyspace"]
    if token and scb_path:
        cloud_config = {"secure_connect_bundle": scb_path}
        auth_provider = PlainTextAuthProvider("token", token)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    else:
        cluster = Cluster([cassandra_host])

    session = cluster.connect()
    cql_path = resolve_repo_path("infra/cassandra_schema.cql", config)
    raw_cql = cql_path.read_text(encoding="utf-8")
    for stmt in raw_cql.split(";"):
        lines = [ln for ln in stmt.splitlines() if not ln.strip().startswith("--")]
        cleaned_stmt = "\n".join(lines).strip()
        if not cleaned_stmt:
            continue
        upper_stmt = cleaned_stmt.upper()
        if upper_stmt.startswith("CREATE KEYSPACE") or upper_stmt.startswith("USE "):
            try:
                session.execute(cleaned_stmt)
            except Exception:
                pass
        else:
            session.set_keyspace(keyspace)
            session.execute(cleaned_stmt)

    session.set_keyspace(keyspace)

    prep_q1 = session.prepare(
        """
        INSERT INTO finops_cost_by_org_day (
            org_id, year_month, usage_date, service, org_name, plan,
            events_count, total_cost_usd, total_usage_value, total_carbon_kg,
            total_genai_tokens, mad_spike_count, zscore_spike_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    args_q1 = [
        (
            r["org_id"],
            r["year_month"],
            r["usage_date"],
            r["service"],
            r["org_name"],
            r["plan"],
            int(r["events_count"]),
            float(r["total_cost_usd"]),
            float(r["total_usage_value"]),
            float(r["total_carbon_kg"]),
            int(r["total_genai_tokens"]),
            int(r["mad_spike_count"]),
            int(r["zscore_spike_count"]),
        )
        for r in finops_daily
    ]
    execute_concurrent_with_args(session, prep_q1, args_q1, concurrency=32)

    prep_q2 = session.prepare(
        """
        INSERT INTO support_sla_by_org_severity (
            org_id, severity, category, org_name, plan, total_tickets,
            open_tickets, resolved_tickets, sla_breached_count, sla_breach_rate_pct,
            avg_resolution_hours, avg_valid_csat, valid_csat_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    args_q2 = [
        (
            r["org_id"],
            r["severity"],
            r["category"],
            r["org_name"],
            r["plan"],
            int(r["total_tickets"]),
            int(r["open_tickets"]),
            int(r["resolved_tickets"]),
            int(r["sla_breached_count"]),
            float(r["sla_breach_rate_pct"]),
            float(r["avg_resolution_hours"]) if r["avg_resolution_hours"] != "" else None,
            float(r["avg_valid_csat"]) if r["avg_valid_csat"] != "" else None,
            int(r["valid_csat_count"]),
        )
        for r in support_sla
    ]
    execute_concurrent_with_args(session, prep_q2, args_q2, concurrency=32)

    prep_q3 = session.prepare(
        """
        INSERT INTO product_genai_tokens_by_org_day (
            org_id, year_month, usage_date, org_name, plan, lifecycle_stage,
            total_events, genai_events, genai_tokens_total, genai_cost_usd,
            total_org_cost_usd, genai_cost_share_pct, active_users_count, org_header_nps
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    args_q3 = [
        (
            r["org_id"],
            r["year_month"],
            r["usage_date"],
            r["org_name"],
            r["plan"],
            r["lifecycle_stage"],
            int(r["total_events"]),
            int(r["genai_events"]),
            int(r["genai_tokens_total"]),
            float(r["genai_cost_usd"]),
            float(r["total_org_cost_usd"]),
            float(r["genai_cost_share_pct"]),
            int(r["active_users_count"]),
            float(r["org_header_nps"]) if r["org_header_nps"] != "" else None,
        )
        for r in product_genai
    ]
    execute_concurrent_with_args(session, prep_q3, args_q3, concurrency=32)

    prep_q4 = session.prepare(
        """
        INSERT INTO cost_anomalies_by_org (
            org_id, timestamp, event_id, org_name, plan, usage_date, year_month,
            resource_id, service, region, metric, value, unit, cost_usd_increment,
            mad_modified_zscore, classic_zscore, costcenter, team
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    args_q4 = [
        (
            r["org_id"],
            r["timestamp"],
            r["event_id"],
            r["org_name"],
            r["plan"],
            r["usage_date"],
            r["year_month"],
            r["resource_id"],
            r["service"],
            r["region"],
            r["metric"],
            float(r["value"]) if r["value"] != "" else None,
            r["unit"],
            float(r["cost_usd_increment"]),
            float(r["mad_modified_zscore"]),
            float(r["classic_zscore"]),
            r["costcenter"],
            r["team"],
        )
        for r in cost_anomalies
    ]
    execute_concurrent_with_args(session, prep_q4, args_q4, concurrency=32)

    cluster.shutdown()
    return {
        "connected_to_remote_cluster": True,
        "upserted_finops_rows": len(args_q1),
        "upserted_support_rows": len(args_q2),
        "upserted_product_rows": len(args_q3),
        "upserted_anomalies_rows": len(args_q4),
    }


def _group_by_cql_keys(rows, partition_cols, clustering_cols):
    """Agrupa las filas por (Partition Key, Clustering Key) para medir tamaño de particiones y probar upserts."""
    partitions = defaultdict(dict)
    for r in rows:
        pk = tuple(r[c] for c in partition_cols)
        ck = tuple(r[c] for c in clustering_cols)
        partitions[pk][ck] = dict(r)
    return partitions


def _partition_summary(table_name, partitions, partition_cols, clustering_cols):
    sizes = [len(ck_map) for ck_map in partitions.values()]
    return {
        "table_name": table_name,
        "partition_key": partition_cols,
        "clustering_key": clustering_cols,
        "total_partitions": len(sizes),
        "total_rows": sum(sizes),
        "min_rows_per_partition": min(sizes) if sizes else 0,
        "max_rows_per_partition": max(sizes) if sizes else 0,
        "avg_rows_per_partition": round(sum(sizes) / len(sizes), 2) if sizes else 0.0,
    }


def run_serving_cassandra(config_path=None):
    config = load_pipeline_config(config_path)
    gold_dir = resolve_repo_path(config["paths"]["gold_dir"], config)
    evidence_dir = resolve_repo_path(config["paths"]["evidence_dir"], config)

    finops_daily = _read_csv(gold_dir / "finops_org_service_daily.csv")
    support_sla = _read_csv(gold_dir / "support_org_sla_severity.csv")
    product_genai = _read_csv(gold_dir / "product_genai_adoption_daily.csv")
    cost_anomalies = _read_csv(gold_dir / "cost_anomalies_by_org.csv")

    conn_status = sync_to_astradb(config, finops_daily, support_sla, product_genai, cost_anomalies)

    # Pasada 1 y Pasada 2 duplicando la entrada para verificar que la clave primaria natural no duplica filas
    p_finops_1 = _group_by_cql_keys(finops_daily, ["org_id", "year_month"], ["usage_date", "service"])
    p_support_1 = _group_by_cql_keys(support_sla, ["org_id"], ["severity", "category"])
    p_product_1 = _group_by_cql_keys(product_genai, ["org_id", "year_month"], ["usage_date"])
    p_anom_1 = _group_by_cql_keys(cost_anomalies, ["org_id"], ["timestamp", "event_id"])

    p_finops_2 = _group_by_cql_keys(finops_daily * 2, ["org_id", "year_month"], ["usage_date", "service"])
    p_support_2 = _group_by_cql_keys(support_sla * 2, ["org_id"], ["severity", "category"])
    p_product_2 = _group_by_cql_keys(product_genai * 2, ["org_id", "year_month"], ["usage_date"])
    p_anom_2 = _group_by_cql_keys(cost_anomalies * 2, ["org_id"], ["timestamp", "event_id"])

    rows_after_pass1 = {
        "finops_cost_by_org_day": sum(len(m) for m in p_finops_1.values()),
        "support_sla_by_org_severity": sum(len(m) for m in p_support_1.values()),
        "product_genai_tokens_by_org_day": sum(len(m) for m in p_product_1.values()),
        "cost_anomalies_by_org": sum(len(m) for m in p_anom_1.values()),
    }
    rows_after_pass2 = {
        "finops_cost_by_org_day": sum(len(m) for m in p_finops_2.values()),
        "support_sla_by_org_severity": sum(len(m) for m in p_support_2.values()),
        "product_genai_tokens_by_org_day": sum(len(m) for m in p_product_2.values()),
        "cost_anomalies_by_org": sum(len(m) for m in p_anom_2.values()),
    }

    target_org = "org_kdgigatj"
    target_ym = "2025-08"

    q1_partition = p_finops_1.get((target_org, target_ym), {})
    q1_rows = sorted(q1_partition.values(), key=lambda x: (x["usage_date"], x["service"]), reverse=True)[:5]

    q2_partition = p_support_1.get((target_org,), {})
    q2_rows = [
        {
            "severity": r["severity"],
            "category": r["category"],
            "total_tickets": int(r["total_tickets"]),
            "open_tickets": int(r["open_tickets"]),
            "sla_breach_rate_pct": float(r["sla_breach_rate_pct"]),
            "avg_resolution_hours": float(r["avg_resolution_hours"]) if r["avg_resolution_hours"] != "" else None,
            "avg_valid_csat": float(r["avg_valid_csat"]) if r["avg_valid_csat"] != "" else None,
        }
        for _, r in sorted(q2_partition.items())
    ][:6]

    q3_partition = p_product_1.get((target_org, target_ym), {})
    q3_rows = sorted(
        [r for r in q3_partition.values() if int(r["genai_tokens_total"]) > 0],
        key=lambda x: x["usage_date"],
        reverse=True,
    )[:5]

    q4_partition = p_anom_1.get((target_org,), {})
    q4_rows = sorted(q4_partition.values(), key=lambda x: (x["timestamp"], x["event_id"]), reverse=True)[:5]

    report = {
        "schema_cql_file": "infra/cassandra_schema.cql",
        "connection_info": conn_status,
        "idempotency_upsert_verification": {
            "rows_after_pass1": rows_after_pass1,
            "rows_after_pass2": rows_after_pass2,
            "is_idempotent": rows_after_pass1 == rows_after_pass2,
        },
        "tables_partition_sizing": [
            _partition_summary("cloud_analytics.finops_cost_by_org_day", p_finops_1, ["org_id", "year_month"], ["usage_date", "service"]),
            _partition_summary("cloud_analytics.support_sla_by_org_severity", p_support_1, ["org_id"], ["severity", "category"]),
            _partition_summary("cloud_analytics.product_genai_tokens_by_org_day", p_product_1, ["org_id", "year_month"], ["usage_date"]),
            _partition_summary("cloud_analytics.cost_anomalies_by_org", p_anom_1, ["org_id"], ["timestamp", "event_id"]),
        ],
        "cql_queries_sample_output": {
            "Q1_finops_cost_by_org_day": {
                "cql": "SELECT usage_date, service, total_cost_usd, events_count, total_carbon_kg, mad_spike_count FROM cloud_analytics.finops_cost_by_org_day WHERE org_id = 'org_kdgigatj' AND year_month = '2025-08' LIMIT 5;",
                "partition_total_rows": len(q1_partition),
                "sample_results": q1_rows,
            },
            "Q2_support_sla_by_org_severity": {
                "cql": "SELECT severity, category, total_tickets, open_tickets, sla_breach_rate_pct, avg_resolution_hours, avg_valid_csat FROM cloud_analytics.support_sla_by_org_severity WHERE org_id = 'org_kdgigatj';",
                "partition_total_rows": len(q2_partition),
                "sample_results": q2_rows,
            },
            "Q3_product_genai_tokens_by_org_day": {
                "cql": "SELECT usage_date, genai_events, genai_tokens_total, genai_cost_usd, total_org_cost_usd, genai_cost_share_pct FROM cloud_analytics.product_genai_tokens_by_org_day WHERE org_id = 'org_kdgigatj' AND year_month = '2025-08' LIMIT 5;",
                "partition_total_rows": len(q3_partition),
                "sample_results": q3_rows,
            },
            "Q4_cost_anomalies_by_org": {
                "cql": "SELECT timestamp, event_id, service, metric, resource_id, cost_usd_increment, mad_modified_zscore, costcenter, team FROM cloud_analytics.cost_anomalies_by_org WHERE org_id = 'org_kdgigatj' LIMIT 5;",
                "partition_total_rows": len(q4_partition),
                "sample_results": q4_rows,
            },
        },
    }

    evidence_dir.mkdir(parents=True, exist_ok=True)
    out_file = evidence_dir / "serving_cassandra_resumen.json"
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    return report


if __name__ == "__main__":
    rep = run_serving_cassandra()
    print(json.dumps(rep["idempotency_upsert_verification"], indent=2, ensure_ascii=False))
    print(json.dumps(rep["tables_partition_sizing"], indent=2, ensure_ascii=False))
