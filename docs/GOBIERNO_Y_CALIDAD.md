**INSTITUTO SUPERIOR TECNOLÓGICO EMPRESARIAL ARGENTINO (ISTEA)**
MINERÍA DE DATOS II - 2C 2026
Proyecto Integrador Obligatorio: Cloud Provider Analytics

# Gobierno del dato, linaje, seguridad y controles de calidad

Este documento reúne la matriz de gobierno (responsables, clasificación, acceso, retención y monitoreo), el linaje de las ocho fuentes a través de las capas del Data Lake hasta Cassandra, el criterio sobre datos personales (PII) y los controles de calidad aplicados en el código.

## 1. Matriz de gobierno por activo de datos

| Activo | Ubicación en el repo | Propietario (Data Owner) | Responsable técnico (Data Steward) | Sensibilidad | Acceso | Retención | Control y evidencia |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `usage_events_stream` (120 JSONL, 43.200 eventos) | `data/raw/usage_events_stream/` → `Silver` (`year=YYYY/month=MM`) | FinOps y Plataforma | Rol de ingesta streaming | Interno operativo (sin PII) | Lectura por pipeline Spark; `raw/` inmutable | Landing: permanente; Bronze: 1 año; Silver: 2 años | 216 costos negativos a `data/quarantine/usage_events_quarantine.csv`; 2.075 `unit` completados según `metric` (`evidence/perfilado_usage_events_resumen.json`) |
| `customers_orgs` (80 clientes) | `data/raw/customers_orgs.csv` → `data/clean/customers_orgs_limpio.csv` | Comercial / Customer Success | Rol de ingesta batch | Confidencial comercial (plan y estado del cliente) | FinOps, Soporte y Producto | Mientras la cuenta esté activa + 5 años | 0 `org_id` repetidos; el único `nps_score` de 101 pasa a nulo y se anota en `data/quarantine/customers_orgs_nps_audit.csv` |
| `resources` (400 recursos) | `data/raw/resources.csv` → `data/clean/resources_limpio.csv` | Infraestructura Cloud | Rol de ingesta batch | Interno (incluye etiqueta `pii` del recurso) | FinOps e Ingeniería | Snapshot activo + histórico por 2 años | Conserva prefijos `res_` y `org_`; 0 nulos tras abrir `tags_json`; 100 % de cruce con eventos (`evidence/integridad_referencial_resumen.json`) |
| `billing_monthly` (240 facturas) | `data/raw/billing_monthly.csv` → `data/clean/billing_monthly_limpio.csv` | Finanzas / FinOps | Rol de ingesta batch | Confidencial financiero | FinOps y Finanzas | 5 a 10 años (contable) | 137 créditos vacíos en 0; conversión a USD con `exchange_rate_to_usd`; 13 subtotales negativos marcados como notas de crédito |
| `users` (800 usuarios) | `data/raw/users.csv` → `data/clean/users_limpio.csv` | Seguridad / Producto | Rol de calidad y Silver | **PII directo** (`email` del usuario) | Restringido; el `email` no pasa a Gold ni a Cassandra | Mientras el usuario esté activo (baja a los 180 días) | 0 mails repetidos; 232 casos con `last_login < created_at` anulados y anotados en `data/quarantine/users_login_audit.csv` |
| `support_tickets` (1.000 tickets) | `data/raw/support_tickets.csv` → `data/clean/support_tickets_clean.csv` | Soporte Técnico | Rol de calidad y Silver | Interno operativo | Soporte y Customer Success | 2 años en Silver, 3 años en Gold | 40 CSAT fuera de 1 a 5 y 163 CSAT de tickets abiertos pasan a nulo (203 filas en `data/quarantine/support_tickets_csat_audit.csv`) |
| `marketing_touches` (1.500) y `nps_surveys` (92) | `data/raw/` → `data/clean/` | Marketing y Producto | Rol de ingesta batch | Interno comercial | Marketing y Producto | 2 años | En NPS no se repite `(org_id, survey_date)`; la única fila vacía (`org_064` del `2025-07-09`) va a `data/quarantine/nps_surveys_quarantine.csv` |
| Marts de Gold y tablas de Cassandra | `data/gold/*.csv` y keyspace `cloud_analytics` | FinOps, Soporte y Producto | Rol de Gold y Serving | Interno analítico (sin PII) | Tableros y consultas CQL por rol | Gold: 2 años; Cassandra: ventana operativa de 90 a 180 días | Idempotencia probada en 2 corridas seguidas; particiones de Cassandra con máximo de 147 filas (`evidence/serving_cassandra_resumen.json`) |

## 2. Linaje de punta a punta

El recorrido de cada fuente desde que entra a `data/raw/` hasta los marts de `data/gold/` y las cuatro tablas de Cassandra (`infra/cassandra_schema.cql`) es el siguiente:

1. **`usage_events_stream/*.jsonl` (43.200 eventos)**
   - En Bronze se lee con el esquema explícito que une `v1` y `v2`, agregando `_ingest_ts`, `_source_file`, `year` y `month`.
   - En Silver se separan los 216 eventos con costo negativo a `data/quarantine/usage_events_quarantine.csv`, se completa `unit` desde `metric` en los 2.075 vacíos, se cruza por `org_id` con `customers_orgs` y por `resource_id` con `resources` (usando `broadcast` en PySpark), y se calculan los picos de costo con MAD por `(service, metric)`.
   - En Gold alimenta `finops_org_service_daily.csv` (11.050 filas), `finops_org_service_monthly.csv` (524 filas), `product_genai_adoption_daily.csv` (4.580 filas) y `cost_anomalies_by_org.csv` (191 filas).
   - En Cassandra alimenta `finops_cost_by_org_day`, `product_genai_tokens_by_org_day` y `cost_anomalies_by_org`.
2. **`customers_orgs.csv` (80 organizaciones) y `resources.csv` (400 recursos)**
   - Se limpian en `data/clean/customers_orgs_limpio.csv` y `data/clean/resources_limpio.csv` (manteniendo los prefijos `org_` y `res_`) y se usan como dimensiones para enriquecer los eventos, los tickets y los marts con `org_name`, `plan`, `lifecycle_stage`, `costcenter` y `team`.
3. **`billing_monthly.csv` (240 facturas)**
   - Se limpia en `data/clean/billing_monthly_limpio.csv` convirtiendo todo a USD y se cruza por `(org_id, year_month)` en `data/gold/finops_org_service_monthly.csv` para comparar el gasto por eventos frente a lo facturado en el mes.
4. **`support_tickets.csv` (1.000 tickets)**
   - Se limpia en `data/clean/support_tickets_clean.csv` (dejando solo los 543 CSAT válidos de tickets cerrados) y se agrupa por `(org_id, severity, category)` para armar `data/gold/support_org_sla_severity.csv` (709 filas), que se carga en la tabla `support_sla_by_org_severity` de Cassandra.
5. **`users.csv` (800 usuarios)**
   - Se limpia en `data/clean/users_limpio.csv`. Para no exponer datos personales en las capas de consulta, a Gold y a Cassandra solo pasa la cantidad de usuarios activos por organización (`active_users_count` en `product_genai_tokens_by_org_day`).

## 3. Datos personales (PII) y manejo de credenciales

- **`users.email`:** es el único dato personal directo del dataset. Queda en `data/clean/users_limpio.csv` para validar que no haya usuarios duplicados, pero no se incluye en ningún archivo de `data/gold/` ni en las tablas de Cassandra.
- **Etiqueta `pii` en `resources`:** al abrir `tags_json` en `resources_limpio.csv` se conserva la columna booleana `pii`, que sirve para auditar qué recursos productivos guardan información sensible y verificar si tienen `backup = True`.
- **Credenciales fuera del repositorio:** no hay tokens de GitHub ni claves de AstraDB en el código ni en los notebooks. `src/serving_cassandra.py` lee `ASTRA_TOKEN` y `ASTRA_SCB_PATH` desde las variables de entorno del sistema, y `.gitignore` excluye `.env`, `*.pem` y `credentials.json`.

## 4. Reproceso e idempotencia

- **En Batch (`src/batch_pipeline.py` y `src/pyspark_etl.py`):** la escritura en Gold y en Parquet por partición (`year`, `month`) pisa el resultado del período reprocesado sin duplicar filas. El script verifica que correr dos pasadas seguidas da exactamente las mismas 11.050 filas diarias y el mismo costo total (`148.351,6009 USD`, diferencia `0.0`).
- **En Streaming (`src/streaming_pipeline.py`):** cada consulta guarda sus lotes procesados en `data/datalake/checkpoints/` (`offsets/` y `commits/`) y deduplica por `event_id` dentro del watermark. Si el proceso se vuelve a lanzar sobre el mismo checkpoint, salta los 12 lotes ya confirmados.
- **En Cassandra (`src/serving_cassandra.py`):** las tablas tienen claves primarias naturales (`((org_id, year_month), usage_date, service)`, etc.), por lo que volver a insertar el mismo lote actualiza la fila en su lugar (*upsert*) sin generar duplicados.
