-- ============================================================================
-- ISTEA - Minería de Datos II (2C 2026)
-- Proyecto Integrador: Cloud Provider Analytics G2
-- Esquema CQL Query-First para Apache Cassandra / DataStax AstraDB
-- ============================================================================
-- Principios de modelado (Clase Cassandra & AstraDB):
-- 1. Diseño Query-First: una tabla desnormalizada por cada patrón de consulta.
-- 2. Partition Key acotada: en series diarias se incluye `year_month` junto a
--    `org_id` para evitar que la partición crezca sin límite con el paso del tiempo.
-- 3. Clustering Keys con orden explícito (`CLUSTERING ORDER BY`) para soportar
--    filtros de rango y devolver los registros más recientes primero sin usar
--    `ALLOW FILTERING`.
-- 4. Idempotencia nativa: `INSERT` en Cassandra actúa como UPSERT por clave
--    primaria natural, garantizando convergencia ante re-ejecuciones.
-- ============================================================================

CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE cloud_analytics;

-- ----------------------------------------------------------------------------
-- Tabla 1 (Q1 - FinOps): Costo, uso, carbono, tokens GenAI y spikes por
-- organización, mes, fecha y servicio.
-- Consulta Q1:
--   SELECT usage_date, service, total_cost_usd, events_count, total_carbon_kg, mad_spike_count
--   FROM cloud_analytics.finops_cost_by_org_day
--   WHERE org_id = ? AND year_month = ? AND usage_date >= ? AND usage_date <= ?;
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cloud_analytics.finops_cost_by_org_day (
    org_id text,
    year_month text,
    usage_date date,
    service text,
    org_name text,
    plan text,
    events_count int,
    total_cost_usd double,
    total_usage_value double,
    total_carbon_kg double,
    total_genai_tokens bigint,
    mad_spike_count int,
    zscore_spike_count int,
    PRIMARY KEY ((org_id, year_month), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC)
  AND comment = 'Q1 FinOps: Costo diario, carbono y spikes por organizacion y servicio';

-- ----------------------------------------------------------------------------
-- Tabla 2 (Q2 - Soporte): SLA vencido, horas promedio de resolución y CSAT
-- válido promedio por organización, severidad y categoría.
-- Consulta Q2:
--   SELECT severity, category, total_tickets, open_tickets, sla_breach_rate_pct,
--          avg_resolution_hours, avg_valid_csat
--   FROM cloud_analytics.support_sla_by_org_severity
--   WHERE org_id = ?;
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cloud_analytics.support_sla_by_org_severity (
    org_id text,
    severity text,
    category text,
    org_name text,
    plan text,
    total_tickets int,
    open_tickets int,
    resolved_tickets int,
    sla_breached_count int,
    sla_breach_rate_pct double,
    avg_resolution_hours double,
    avg_valid_csat double,
    valid_csat_count int,
    PRIMARY KEY ((org_id), severity, category)
) WITH CLUSTERING ORDER BY (severity ASC, category ASC)
  AND comment = 'Q2 Soporte: Metricas de SLA y CSAT valido por organizacion, severidad y categoria';

-- ----------------------------------------------------------------------------
-- Tabla 3 (Q3 - Producto / GenAI): Evolución diaria de adopción de GenAI
-- (tokens, eventos, costo GenAI y % sobre el costo total) por organización.
-- Consulta Q3:
--   SELECT usage_date, genai_events, genai_tokens_total, genai_cost_usd,
--          total_org_cost_usd, genai_cost_share_pct, active_users_count
--   FROM cloud_analytics.product_genai_tokens_by_org_day
--   WHERE org_id = ? AND year_month = ?;
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cloud_analytics.product_genai_tokens_by_org_day (
    org_id text,
    year_month text,
    usage_date date,
    org_name text,
    plan text,
    lifecycle_stage text,
    total_events int,
    genai_events int,
    genai_tokens_total bigint,
    genai_cost_usd double,
    total_org_cost_usd double,
    genai_cost_share_pct double,
    active_users_count int,
    org_header_nps double,
    PRIMARY KEY ((org_id, year_month), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC)
  AND comment = 'Q3 Producto: Adopcion diaria de GenAI, tokens y share de costo por organizacion';

-- ----------------------------------------------------------------------------
-- Tabla 4 (Q4 - Alertas FinOps): Spikes de costo detectados por Modified
-- Z-Score (MAD > 3.5) por organización, ordenados desde el más reciente.
-- Consulta Q4:
--   SELECT timestamp, event_id, service, metric, resource_id, cost_usd_increment,
--          mad_modified_zscore, costcenter, team
--   FROM cloud_analytics.cost_anomalies_by_org
--   WHERE org_id = ? LIMIT 20;
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cloud_analytics.cost_anomalies_by_org (
    org_id text,
    timestamp text,
    event_id text,
    org_name text,
    plan text,
    usage_date date,
    year_month text,
    resource_id text,
    service text,
    region text,
    metric text,
    value double,
    unit text,
    cost_usd_increment double,
    mad_modified_zscore double,
    classic_zscore double,
    costcenter text,
    team text,
    PRIMARY KEY ((org_id), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC)
  AND comment = 'Q4 Alertas FinOps: Eventos anomalos (MAD > 3.5) por organizacion';
