import json
import math
from datetime import datetime

UNIT_BY_METRIC = {
    "cpu_hours": "hours",
    "requests": "count",
    "storage_gb_hours": "gb_hours",
}

DEFAULT_RESOURCE_TAGS = {
    "env": "Sin Especificar",
    "pii": False,
    "costcenter": "Sin Asignar",
    "team": "Sin Asignar",
    "backup": False,
}


def impute_event_unit(metric, current_unit):
    """Si unit viene vacío, lo completa según metric."""
    if current_unit is not None and str(current_unit).strip() != "":
        return str(current_unit).strip(), False
    if metric is None:
        return None, False
    imputed = UNIT_BY_METRIC.get(str(metric).strip())
    return imputed, imputed is not None


def evaluate_usage_event_quality(event, min_valid_cost=0.0):
    """Valida un evento de uso y devuelve (es_valido, motivo_rechazo, evento_limpio)."""
    cleaned = dict(event)

    if cleaned.get("_corrupt_record"):
        return False, "CORRUPT_JSON_RECORD", cleaned

    event_id = cleaned.get("event_id")
    org_id = cleaned.get("org_id")
    resource_id = cleaned.get("resource_id")
    ts_str = cleaned.get("timestamp")
    if not event_id or not org_id or not resource_id or not ts_str:
        return False, "MISSING_PRIMARY_IDENTIFIERS", cleaned

    try:
        cost = float(cleaned.get("cost_usd_increment"))
    except (TypeError, ValueError):
        return False, "INVALID_COST_NUMBER", cleaned

    if cost < min_valid_cost:
        cleaned["cost_usd_increment"] = cost
        return False, "NEGATIVE_COST_INCREMENT", cleaned

    cleaned["cost_usd_increment"] = cost
    raw_val = cleaned.get("value")
    if raw_val is None or str(raw_val).strip() == "":
        cleaned["value"] = None
        cleaned["is_value_null"] = True
    else:
        cleaned["value"] = float(raw_val)
        cleaned["is_value_null"] = False

    unit_val, is_imputed = impute_event_unit(cleaned.get("metric"), cleaned.get("unit"))
    cleaned["unit"] = unit_val
    cleaned["is_unit_imputed"] = is_imputed

    cleaned["schema_version"] = int(cleaned.get("schema_version", 1))

    carbon_raw = cleaned.get("carbon_kg")
    cleaned["carbon_kg"] = float(carbon_raw) if carbon_raw not in (None, "") else None

    tokens_raw = cleaned.get("genai_tokens")
    cleaned["genai_tokens"] = int(float(tokens_raw)) if tokens_raw not in (None, "") else None

    usage_date = str(ts_str)[:10]
    cleaned["usage_date"] = usage_date
    cleaned["year"] = int(usage_date[:4])
    cleaned["month"] = int(usage_date[5:7])
    cleaned["year_month"] = usage_date[:7]

    return True, None, cleaned


def parse_resource_tags(tags_json_str):
    """Abre tags_json de resources.csv y completa faltantes."""
    result = dict(DEFAULT_RESOURCE_TAGS)
    if not tags_json_str or not str(tags_json_str).strip():
        return result

    try:
        parsed = json.loads(str(tags_json_str))
    except json.JSONDecodeError:
        return result

    if not isinstance(parsed, dict):
        return result

    env_val = parsed.get("env")
    if env_val is not None and str(env_val).strip() != "":
        result["env"] = str(env_val).strip()

    pii_val = parsed.get("pii")
    if isinstance(pii_val, bool):
        result["pii"] = pii_val
    elif isinstance(pii_val, str) and pii_val.strip().lower() in ("true", "1", "yes"):
        result["pii"] = True

    cc_val = parsed.get("cost_center") or parsed.get("costcenter")
    if cc_val is not None and str(cc_val).strip() != "":
        result["costcenter"] = str(cc_val).strip()

    team_val = parsed.get("team")
    if team_val is not None and str(team_val).strip() != "":
        result["team"] = str(team_val).strip()

    backup_val = parsed.get("backup")
    if isinstance(backup_val, bool):
        result["backup"] = backup_val
    elif isinstance(backup_val, str):
        b_norm = backup_val.strip().lower()
        result["backup"] = b_norm in ("true", "1", "yes", "daily", "weekly", "enabled")

    return result


def clean_resource_record(row):
    """Limpia una fila de resources.csv conservando res_ y org_."""
    tags = parse_resource_tags(row.get("tags_json"))
    created_raw = str(row.get("created_at", "")).strip()
    created_date = created_raw[:10] if created_raw else ""
    return {
        "resource_id": str(row.get("resource_id", "")).strip(),
        "org_id": str(row.get("org_id", "")).strip(),
        "service": str(row.get("service", "")).strip().lower(),
        "region": str(row.get("region", "")).strip().lower(),
        "created_at": created_date,
        "state": str(row.get("state", "")).strip().lower(),
        "env": tags["env"],
        "pii": tags["pii"],
        "costcenter": tags["costcenter"],
        "team": tags["team"],
        "backup": tags["backup"],
    }


def clean_customer_org_record(row, nps_min=-100.0, nps_max=100.0):
    """Limpia una fila de customers_orgs.csv y anula NPS fuera de [-100, 100]."""
    raw_nps = row.get("nps_score")
    nps_val = None
    had_invalid_nps = False
    if raw_nps is not None and str(raw_nps).strip() != "":
        try:
            candidate = float(raw_nps)
            if nps_min <= candidate <= nps_max:
                nps_val = candidate
            else:
                had_invalid_nps = True
        except ValueError:
            had_invalid_nps = True

    plan_val = row.get("plan_tier") or row.get("plan") or ""
    region_val = row.get("hq_region") or row.get("region") or ""
    ent_raw = row.get("is_enterprise")
    is_ent = ent_raw if isinstance(ent_raw, bool) else str(ent_raw).strip().lower() in ("true", "1", "yes")

    return {
        "org_id": str(row.get("org_id", "")).strip(),
        "org_name": str(row.get("org_name", "")).strip(),
        "industry": str(row.get("industry", "")).strip().lower(),
        "hq_region": str(region_val).strip().lower(),
        "plan_tier": str(plan_val).strip().lower(),
        "plan": str(plan_val).strip().lower(),
        "is_enterprise": is_ent,
        "signup_date": str(row.get("signup_date", "")).strip()[:10],
        "sales_rep": str(row.get("sales_rep", "")).strip(),
        "lifecycle_stage": str(row.get("lifecycle_stage", "")).strip().lower(),
        "marketing_source": str(row.get("marketing_source", "")).strip().lower(),
        "nps_score": nps_val,
    }, had_invalid_nps


def clean_billing_record(row):
    """Completa credits vacíos con 0 y pasa los montos a USD."""
    subtotal = float(row.get("subtotal") or 0.0)
    raw_credits = row.get("credits")
    is_credits_imputed = raw_credits is None or str(raw_credits).strip() == ""
    credits = 0.0 if is_credits_imputed else float(raw_credits)
    taxes = float(row.get("taxes") or 0.0)
    fx = float(row.get("exchange_rate_to_usd") or 1.0)

    month_raw = str(row.get("month", "")).strip()
    year_month = month_raw[:7]
    month_date = f"{year_month}-01" if len(year_month) == 7 else month_raw

    subtotal_usd = round(subtotal * fx, 2)
    credits_usd = round(credits * fx, 2)
    taxes_usd = round(taxes * fx, 2)
    net_billed_usd = round(subtotal_usd - credits_usd + taxes_usd, 2)

    return {
        "invoice_id": str(row.get("invoice_id", "")).strip(),
        "org_id": str(row.get("org_id", "")).strip(),
        "month": month_date,
        "year_month": year_month,
        "subtotal": subtotal,
        "credits": credits,
        "taxes": taxes,
        "currency": str(row.get("currency", "")).strip().upper(),
        "exchange_rate_to_usd": fx,
        "subtotal_usd": subtotal_usd,
        "credits_usd": credits_usd,
        "taxes_usd": taxes_usd,
        "net_billed_usd": net_billed_usd,
        "is_credits_imputed": is_credits_imputed,
        "is_credit_note": subtotal < 0.0,
    }


def clean_support_ticket_record(row, csat_min=1.0, csat_max=5.0):
    """Limpia un ticket y anula CSAT si está fuera de 1..5 o si el ticket sigue abierto."""
    created_at = str(row.get("created_at", "")).strip()
    resolved_raw = row.get("resolved_at")
    resolved_at = str(resolved_raw).strip() if resolved_raw is not None and str(resolved_raw).strip() != "" else None

    raw_csat = row.get("csat")
    csat_val = None
    nullified_reason = None
    if raw_csat is not None and str(raw_csat).strip() != "":
        try:
            candidate = float(raw_csat)
            if candidate < csat_min or candidate > csat_max:
                nullified_reason = "CSAT_OUT_OF_RANGE"
            elif resolved_at is None:
                nullified_reason = "CSAT_ON_UNRESOLVED_TICKET"
            else:
                csat_val = candidate
        except ValueError:
            nullified_reason = "CSAT_INVALID_NUMBER"

    sla_raw = row.get("sla_breached")
    if isinstance(sla_raw, bool):
        sla_breached = sla_raw
    else:
        sla_breached = str(sla_raw).strip().lower() in ("true", "1", "yes")

    resolution_hours = None
    if created_at and resolved_at:
        try:
            dt_c = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            dt_r = datetime.fromisoformat(resolved_at.replace("Z", "+00:00"))
            diff_h = (dt_r - dt_c).total_seconds() / 3600.0
            if diff_h >= 0:
                resolution_hours = round(diff_h, 2)
        except ValueError:
            resolution_hours = None

    year = int(created_at[:4]) if len(created_at) >= 7 else 2025
    month = int(created_at[5:7]) if len(created_at) >= 7 else 1

    return {
        "ticket_id": str(row.get("ticket_id", "")).strip(),
        "org_id": str(row.get("org_id", "")).strip(),
        "category": str(row.get("category", "")).strip().lower(),
        "severity": str(row.get("severity", "")).strip().lower(),
        "created_at": created_at,
        "resolved_at": resolved_at,
        "is_open": resolved_at is None,
        "resolution_hours": resolution_hours,
        "csat": csat_val,
        "sla_breached": sla_breached,
        "year": year,
        "month": month,
    }, nullified_reason


def clean_user_record(row):
    """Limpia un usuario y anula last_login cuando es anterior a created_at."""
    created_at = str(row.get("created_at", "")).strip()
    raw_login = row.get("last_login")
    last_login = str(raw_login).strip() if raw_login is not None and str(raw_login).strip() != "" else None

    inconsistent_login = False
    if created_at and last_login:
        if last_login[:19] < created_at[:19]:
            inconsistent_login = True
            last_login = None

    active_raw = row.get("active")
    active = active_raw if isinstance(active_raw, bool) else str(active_raw).strip().lower() in ("true", "1", "yes")

    return {
        "user_id": str(row.get("user_id", "")).strip(),
        "org_id": str(row.get("org_id", "")).strip(),
        "email": str(row.get("email", "")).strip().lower(),
        "role": str(row.get("role", "")).strip().lower(),
        "active": active,
        "created_at": created_at,
        "last_login": last_login,
        "is_login_inconsistent": inconsistent_login,
    }, inconsistent_login


def clean_nps_survey_record(row, low_outlier=-20.0, high_outlier=80.0):
    """Separa a cuarentena la encuesta vacía (sin puntaje ni comentario) y marca outliers."""
    raw_score = row.get("nps_score")
    raw_comment = row.get("comment")
    has_score = raw_score is not None and str(raw_score).strip() != ""
    has_comment = raw_comment is not None and str(raw_comment).strip() != ""

    if not has_score and not has_comment:
        return False, "EMPTY_SURVEY_SCORE_AND_COMMENT", dict(row)

    score_val = float(raw_score) if has_score else None
    comment_val = str(raw_comment).strip() if has_comment else None
    is_outlier = bool(score_val is not None and (score_val <= low_outlier or score_val >= high_outlier))

    return True, None, {
        "org_id": str(row.get("org_id", "")).strip(),
        "survey_date": str(row.get("survey_date", "")).strip()[:10],
        "nps_score": score_val,
        "comment": comment_val,
        "is_outlier": is_outlier,
    }


def _median_sorted(sorted_vals):
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return sorted_vals[mid]
    return 0.5 * (sorted_vals[mid - 1] + sorted_vals[mid])


def compute_cost_anomaly_stats_by_group(events):
    """Calcula mediana, MAD, media y desvío estándar de costo por (service, metric)."""
    grouped = {}
    for ev in events:
        key = (str(ev.get("service", "")), str(ev.get("metric", "")))
        grouped.setdefault(key, []).append(float(ev.get("cost_usd_increment", 0.0)))

    stats = {}
    for key, values in grouped.items():
        s_vals = sorted(values)
        n = len(s_vals)
        med = _median_sorted(s_vals)
        abs_devs = sorted(abs(x - med) for x in s_vals)
        mad = _median_sorted(abs_devs)
        mean = sum(s_vals) / n if n else 0.0
        variance = sum((x - mean) ** 2 for x in s_vals) / n if n else 0.0
        std = math.sqrt(variance)
        stats[key] = {
            "count": float(n),
            "median": med,
            "mad": mad,
            "mean": mean,
            "std": std,
        }
    return stats


def score_event_cost_anomaly(cost_usd, group_stats, mad_threshold=3.5, zscore_threshold=3.0):
    """Calcula Modified Z-Score (MAD) y Z-Score clásico para el costo de un evento."""
    med = group_stats.get("median", 0.0)
    mad = group_stats.get("mad", 0.0)
    mean = group_stats.get("mean", 0.0)
    std = group_stats.get("std", 0.0)

    mod_z = (0.6745 * abs(cost_usd - med) / mad) if mad > 1e-12 else 0.0
    classic_z = ((cost_usd - mean) / std) if std > 1e-12 else 0.0

    return {
        "mad_modified_zscore": round(mod_z, 4),
        "classic_zscore": round(classic_z, 4),
        "is_mad_spike": mod_z > mad_threshold,
        "is_zscore_spike": classic_z > zscore_threshold,
    }
