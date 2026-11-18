from src.quality_rules import (
    clean_billing_record,
    clean_customer_org_record,
    clean_nps_survey_record,
    clean_resource_record,
    clean_support_ticket_record,
    clean_user_record,
    compute_cost_anomaly_stats_by_group,
    evaluate_usage_event_quality,
    score_event_cost_anomaly,
)


def transform_silver(bronze_data, config):
    """Aplica las reglas de limpieza de Silver, separa registros a Quarantine y cruza dimensiones."""
    silver_data = {}
    quarantine_data = {
        "usage_events": list(bronze_data.get("usage_events_corrupt", [])),
        "nps_surveys": [],
        "support_tickets_csat_audit": [],
        "customers_orgs_nps_audit": [],
        "users_login_audit": [],
    }

    # 1. customers_orgs
    silver_orgs = []
    orgs_by_id = {}
    for row in bronze_data["customers_orgs"]:
        cleaned, had_invalid_nps = clean_customer_org_record(row)
        silver_orgs.append(cleaned)
        orgs_by_id[cleaned["org_id"]] = cleaned
        if had_invalid_nps:
            quarantine_data["customers_orgs_nps_audit"].append(
                {
                    "org_id": cleaned["org_id"],
                    "raw_nps_score": row.get("nps_score"),
                    "audit_reason": "NPS_OUT_OF_RANGE_100",
                }
            )
    silver_data["customers_orgs"] = silver_orgs

    # 2. resources (conservando res_ y org_)
    silver_resources = []
    res_by_id = {}
    for row in bronze_data["resources"]:
        cleaned = clean_resource_record(row)
        silver_resources.append(cleaned)
        res_by_id[cleaned["resource_id"]] = cleaned
    silver_data["resources"] = silver_resources

    # 3. billing_monthly
    silver_data["billing_monthly"] = [clean_billing_record(row) for row in bronze_data["billing_monthly"]]

    # 4. support_tickets
    silver_tickets = []
    for row in bronze_data["support_tickets"]:
        cleaned, nullified_reason = clean_support_ticket_record(row)
        silver_tickets.append(cleaned)
        if nullified_reason:
            quarantine_data["support_tickets_csat_audit"].append(
                {
                    "ticket_id": cleaned["ticket_id"],
                    "org_id": cleaned["org_id"],
                    "raw_csat": row.get("csat"),
                    "resolved_at": row.get("resolved_at"),
                    "audit_reason": nullified_reason,
                }
            )
    silver_data["support_tickets"] = silver_tickets

    # 5. users
    silver_users = []
    for row in bronze_data["users"]:
        cleaned, inconsistent = clean_user_record(row)
        silver_users.append(cleaned)
        if inconsistent:
            quarantine_data["users_login_audit"].append(
                {
                    "user_id": cleaned["user_id"],
                    "org_id": cleaned["org_id"],
                    "created_at": cleaned["created_at"],
                    "raw_last_login": row.get("last_login"),
                    "audit_reason": "LAST_LOGIN_BEFORE_CREATED_AT",
                }
            )
    silver_data["users"] = silver_users

    # 6. marketing_touches y nps_surveys
    silver_touches = []
    for row in bronze_data["marketing_touches"]:
        ts = str(row.get("timestamp", "")).strip()
        silver_touches.append(
            {
                "touch_id": str(row.get("touch_id", "")).strip(),
                "org_id": str(row.get("org_id", "")).strip(),
                "campaign": str(row.get("campaign", "")).strip(),
                "channel": str(row.get("channel", "")).strip().lower(),
                "timestamp": ts,
                "mes": ts[:7],
                "year": int(ts[:4]) if len(ts) >= 7 else 2025,
                "month": int(ts[5:7]) if len(ts) >= 7 else 1,
                "clicked": str(row.get("clicked", "")).strip().lower() in ("true", "1", "yes"),
                "converted": str(row.get("converted", "")).strip().lower() in ("true", "1", "yes"),
            }
        )
    silver_data["marketing_touches"] = silver_touches

    silver_nps = []
    for row in bronze_data["nps_surveys"]:
        is_valid, q_reason, cleaned = clean_nps_survey_record(row)
        if is_valid:
            silver_nps.append(cleaned)
        else:
            cleaned["quarantine_reason"] = q_reason
            quarantine_data["nps_surveys"].append(cleaned)
    silver_data["nps_surveys"] = silver_nps

    # 7. usage_events: separación a Quarantine, deduplicación por event_id, unit desde metric y MAD > 3,5
    seen_event_ids = set()
    valid_events = []
    duplicate_events_dropped = 0
    for raw_ev in bronze_data["usage_events"]:
        is_valid, q_reason, cleaned = evaluate_usage_event_quality(raw_ev, min_valid_cost=0.0)
        if not is_valid:
            quarantine_data["usage_events"].append(
                {
                    "event_id": raw_ev.get("event_id"),
                    "timestamp": raw_ev.get("timestamp"),
                    "org_id": raw_ev.get("org_id"),
                    "resource_id": raw_ev.get("resource_id"),
                    "service": raw_ev.get("service"),
                    "metric": raw_ev.get("metric"),
                    "value": raw_ev.get("value"),
                    "unit": raw_ev.get("unit"),
                    "cost_usd_increment": raw_ev.get("cost_usd_increment"),
                    "schema_version": raw_ev.get("schema_version"),
                    "quarantine_reason": q_reason,
                    "_source_file": raw_ev.get("_source_file"),
                }
            )
            continue

        ev_id = cleaned["event_id"]
        if ev_id in seen_event_ids:
            duplicate_events_dropped += 1
            continue
        seen_event_ids.add(ev_id)
        valid_events.append(cleaned)

    group_stats = compute_cost_anomaly_stats_by_group(valid_events)

    org_join_matches = 0
    res_join_matches = 0
    for ev in valid_events:
        key = (str(ev["service"]), str(ev["metric"]))
        scores = score_event_cost_anomaly(ev["cost_usd_increment"], group_stats[key])
        ev.update(scores)

        org_info = orgs_by_id.get(ev["org_id"])
        if org_info:
            org_join_matches += 1
            ev["org_name"] = org_info["org_name"]
            ev["plan"] = org_info["plan"]
            ev["industry"] = org_info["industry"]
            ev["lifecycle_stage"] = org_info["lifecycle_stage"]
        else:
            ev["org_name"] = None
            ev["plan"] = None
            ev["industry"] = None
            ev["lifecycle_stage"] = None

        res_info = res_by_id.get(ev["resource_id"])
        if res_info:
            res_join_matches += 1
            ev["resource_state"] = res_info["state"]
            ev["env"] = res_info["env"]
            ev["costcenter"] = res_info["costcenter"]
            ev["team"] = res_info["team"]
            ev["pii"] = res_info["pii"]
            ev["backup"] = res_info["backup"]
        else:
            ev["resource_state"] = None
            ev["env"] = "Sin Especificar"
            ev["costcenter"] = "Sin Asignar"
            ev["team"] = "Sin Asignar"
            ev["pii"] = False
            ev["backup"] = False

    valid_events.sort(
        key=lambda r: (r["year"], r["month"], str(r["service"]), str(r["org_id"]), str(r["timestamp"]))
    )

    silver_data["usage_events"] = valid_events
    silver_data["quarantine"] = quarantine_data
    silver_data["anomaly_stats_by_service_metric"] = {
        f"{s}__{m}": {k: round(v, 6) for k, v in st.items()} for (s, m), st in sorted(group_stats.items())
    }
    silver_data["join_audit"] = {
        "valid_events_total": len(valid_events),
        "duplicate_events_dropped": duplicate_events_dropped,
        "org_join_matches": org_join_matches,
        "org_join_match_pct": round(100.0 * org_join_matches / len(valid_events), 2) if valid_events else 0.0,
        "resource_join_matches": res_join_matches,
        "resource_join_match_pct": round(100.0 * res_join_matches / len(valid_events), 2) if valid_events else 0.0,
    }
    return silver_data
