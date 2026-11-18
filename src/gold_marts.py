from collections import defaultdict


def build_gold_marts(silver_data, config):
    """Agrega las tablas de Silver para armar los marts de FinOps, Soporte, Producto/GenAI y anomalías."""
    events = silver_data["usage_events"]
    tickets = silver_data["support_tickets"]
    billing = silver_data["billing_monthly"]
    orgs = silver_data["customers_orgs"]
    users = silver_data["users"]

    orgs_by_id = {o["org_id"]: o for o in orgs}

    active_users_by_org = defaultdict(int)
    for u in users:
        if u["active"]:
            active_users_by_org[u["org_id"]] += 1

    billing_by_org_month = {}
    for b in billing:
        billing_by_org_month[(b["org_id"], b["year_month"])] = b

    finops_daily_acc = {}
    finops_monthly_acc = {}
    product_daily_acc = {}
    cost_anomalies = []

    for ev in events:
        org_id = ev["org_id"]
        u_date = ev["usage_date"]
        ym = ev["year_month"]
        svc = ev["service"]
        cost = float(ev["cost_usd_increment"])
        val = float(ev["value"]) if ev["value"] is not None else 0.0
        carb = float(ev["carbon_kg"]) if ev["carbon_kg"] is not None else 0.0
        tok = int(ev["genai_tokens"]) if ev["genai_tokens"] is not None else 0
        is_mad = bool(ev["is_mad_spike"])
        is_z = bool(ev["is_zscore_spike"])

        d_key = (org_id, u_date, svc)
        if d_key not in finops_daily_acc:
            finops_daily_acc[d_key] = {
                "org_id": org_id,
                "org_name": ev["org_name"],
                "plan": ev["plan"],
                "year_month": ym,
                "usage_date": u_date,
                "year": ev["year"],
                "month": ev["month"],
                "service": svc,
                "events_count": 0,
                "total_cost_usd": 0.0,
                "total_usage_value": 0.0,
                "total_carbon_kg": 0.0,
                "total_genai_tokens": 0,
                "mad_spike_count": 0,
                "zscore_spike_count": 0,
            }
        d_rec = finops_daily_acc[d_key]
        d_rec["events_count"] += 1
        d_rec["total_cost_usd"] += cost
        d_rec["total_usage_value"] += val
        d_rec["total_carbon_kg"] += carb
        d_rec["total_genai_tokens"] += tok
        if is_mad:
            d_rec["mad_spike_count"] += 1
        if is_z:
            d_rec["zscore_spike_count"] += 1

        m_key = (org_id, ym, svc)
        if m_key not in finops_monthly_acc:
            finops_monthly_acc[m_key] = {
                "org_id": org_id,
                "org_name": ev["org_name"],
                "plan": ev["plan"],
                "year_month": ym,
                "service": svc,
                "events_count": 0,
                "total_cost_usd": 0.0,
                "total_usage_value": 0.0,
                "total_carbon_kg": 0.0,
                "total_genai_tokens": 0,
                "mad_spike_count": 0,
                "zscore_spike_count": 0,
            }
        m_rec = finops_monthly_acc[m_key]
        m_rec["events_count"] += 1
        m_rec["total_cost_usd"] += cost
        m_rec["total_usage_value"] += val
        m_rec["total_carbon_kg"] += carb
        m_rec["total_genai_tokens"] += tok
        if is_mad:
            m_rec["mad_spike_count"] += 1
        if is_z:
            m_rec["zscore_spike_count"] += 1

        p_key = (org_id, u_date)
        if p_key not in product_daily_acc:
            org_meta = orgs_by_id.get(org_id, {})
            product_daily_acc[p_key] = {
                "org_id": org_id,
                "org_name": ev["org_name"],
                "plan": ev["plan"],
                "lifecycle_stage": ev["lifecycle_stage"],
                "year_month": ym,
                "usage_date": u_date,
                "year": ev["year"],
                "month": ev["month"],
                "total_events": 0,
                "genai_events": 0,
                "genai_tokens_total": 0,
                "genai_cost_usd": 0.0,
                "total_org_cost_usd": 0.0,
                "active_users_count": active_users_by_org.get(org_id, 0),
                "org_header_nps": org_meta.get("nps_score"),
            }
        p_rec = product_daily_acc[p_key]
        p_rec["total_events"] += 1
        p_rec["total_org_cost_usd"] += cost
        if svc == "genai":
            p_rec["genai_events"] += 1
            p_rec["genai_tokens_total"] += tok
            p_rec["genai_cost_usd"] += cost

        if is_mad:
            cost_anomalies.append(
                {
                    "org_id": org_id,
                    "org_name": ev["org_name"],
                    "plan": ev["plan"],
                    "event_id": ev["event_id"],
                    "timestamp": ev["timestamp"],
                    "usage_date": u_date,
                    "year_month": ym,
                    "resource_id": ev["resource_id"],
                    "service": svc,
                    "region": ev["region"],
                    "metric": ev["metric"],
                    "value": ev["value"],
                    "unit": ev["unit"],
                    "cost_usd_increment": round(cost, 4),
                    "mad_modified_zscore": ev["mad_modified_zscore"],
                    "classic_zscore": ev["classic_zscore"],
                    "costcenter": ev["costcenter"],
                    "team": ev["team"],
                }
            )

    finops_daily_list = []
    for k in sorted(finops_daily_acc.keys()):
        r = finops_daily_acc[k]
        r["total_cost_usd"] = round(r["total_cost_usd"], 4)
        r["total_usage_value"] = round(r["total_usage_value"], 4)
        r["total_carbon_kg"] = round(r["total_carbon_kg"], 4)
        finops_daily_list.append(r)

    finops_monthly_list = []
    for k in sorted(finops_monthly_acc.keys()):
        r = finops_monthly_acc[k]
        r["total_cost_usd"] = round(r["total_cost_usd"], 4)
        r["total_usage_value"] = round(r["total_usage_value"], 4)
        r["total_carbon_kg"] = round(r["total_carbon_kg"], 4)
        b_info = billing_by_org_month.get((r["org_id"], r["year_month"]))
        r["org_month_net_billed_usd"] = b_info["net_billed_usd"] if b_info else None
        r["org_month_subtotal_usd"] = b_info["subtotal_usd"] if b_info else None
        finops_monthly_list.append(r)

    product_daily_list = []
    for k in sorted(product_daily_acc.keys()):
        r = product_daily_acc[k]
        r["genai_cost_usd"] = round(r["genai_cost_usd"], 4)
        r["total_org_cost_usd"] = round(r["total_org_cost_usd"], 4)
        r["genai_cost_share_pct"] = (
            round(100.0 * r["genai_cost_usd"] / r["total_org_cost_usd"], 2)
            if r["total_org_cost_usd"] > 0
            else 0.0
        )
        product_daily_list.append(r)

    support_acc = {}
    for t in tickets:
        org_id = t["org_id"]
        sev = t["severity"]
        cat = t["category"]
        s_key = (org_id, sev, cat)
        if s_key not in support_acc:
            org_meta = orgs_by_id.get(org_id, {})
            support_acc[s_key] = {
                "org_id": org_id,
                "org_name": org_meta.get("org_name"),
                "plan": org_meta.get("plan"),
                "severity": sev,
                "category": cat,
                "total_tickets": 0,
                "open_tickets": 0,
                "resolved_tickets": 0,
                "sla_breached_count": 0,
                "_res_hours_sum": 0.0,
                "_res_hours_cnt": 0,
                "_csat_sum": 0.0,
                "_csat_cnt": 0,
            }
        s_rec = support_acc[s_key]
        s_rec["total_tickets"] += 1
        if t["is_open"]:
            s_rec["open_tickets"] += 1
        else:
            s_rec["resolved_tickets"] += 1
        if t["sla_breached"]:
            s_rec["sla_breached_count"] += 1
        if t["resolution_hours"] is not None:
            s_rec["_res_hours_sum"] += float(t["resolution_hours"])
            s_rec["_res_hours_cnt"] += 1
        if t["csat"] is not None:
            s_rec["_csat_sum"] += float(t["csat"])
            s_rec["_csat_cnt"] += 1

    support_sla_list = []
    for k in sorted(support_acc.keys()):
        r = support_acc[k]
        total_t = r["total_tickets"]
        res_cnt = r.pop("_res_hours_cnt")
        res_sum = r.pop("_res_hours_sum")
        csat_cnt = r.pop("_csat_cnt")
        csat_sum = r.pop("_csat_sum")
        r["sla_breach_rate_pct"] = round(100.0 * r["sla_breached_count"] / total_t, 2) if total_t else 0.0
        r["avg_resolution_hours"] = round(res_sum / res_cnt, 2) if res_cnt else None
        r["avg_valid_csat"] = round(csat_sum / csat_cnt, 2) if csat_cnt else None
        r["valid_csat_count"] = csat_cnt
        support_sla_list.append(r)

    cost_anomalies.sort(key=lambda r: (r["org_id"], r["timestamp"], r["event_id"]))

    return {
        "finops_org_service_daily": finops_daily_list,
        "finops_org_service_monthly": finops_monthly_list,
        "support_org_sla_severity": support_sla_list,
        "product_genai_adoption_daily": product_daily_list,
        "cost_anomalies_by_org": cost_anomalies,
    }
