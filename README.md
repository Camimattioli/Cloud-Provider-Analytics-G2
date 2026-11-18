# Cloud Provider Analytics G2

Análisis y pipeline de datos para un proveedor cloud a partir de siete fuentes CSV (clientes, facturación, recursos, usuarios, soporte, marketing y NPS) y 120 archivos JSONL de eventos de uso (`43.200` eventos).

## Estructura del repositorio

| Carpeta / archivo | Propósito |
| --- | --- |
| `README.md` | Objetivo, estructura, requisitos, ejecución y pruebas. |
| `DECISIONS.md` | Decisiones de arquitectura, limpieza, particionamiento y watermark, con las alternativas descartadas. |
| `config/pipeline_config.json` | Rutas relativas, parámetros de Spark, reglas de calidad y valores de watermark. Sin credenciales. |
| `docs/` | Diseño arquitectónico, perfilado de los JSONL, limpieza de recursos, matriz requisito-componente, gobierno y riesgos. |
| `data/raw/` | Datos originales tal como se recibieron (7 CSV y 120 JSONL). No se editan a mano. |
| `data/clean/` | Salida de la limpieza de los siete CSV, con `org_id` y `resource_id` consistentes en todas las tablas. |
| `data/gold/` | Marts agregados de FinOps, Soporte, Producto/GenAI y tabla de anomalías de costo. |
| `data/quarantine/` | Registros rechazados con motivo (costos negativos, encuesta vacía) y auditorías de CSAT y logins. |
| `notebooks/` | Notebooks de limpieza, perfilado de eventos JSONL y ejecución en PySpark (Batch y Structured Streaming). |
| `src/` | Módulos de esquemas, reglas de calidad, pipeline PySpark (`pyspark_etl.py`), ejecución batch/streaming y carga a Cassandra. |
| `infra/cassandra_schema.cql` | Creación del keyspace `cloud_analytics` y las cuatro tablas en Cassandra / AstraDB. |
| `tests/` | Pruebas automáticas de las reglas de limpieza, esquemas, integridad referencial y conteos del pipeline. |
| `evidence/` | Resultados en JSON de las corridas (perfilado, integridad referencial, MapReduce, batch, streaming y consultas CQL). |

El detalle de cada fuente, qué se limpió y qué se encontró está en [data/README.md](data/README.md).

## Requisitos

Para correr los scripts locales y la suite de pruebas alcanza con Python 3.10+. Para abrir los notebooks o correr PySpark y la carga a AstraDB:

```bash
python -m pip install pandas numpy jupyter pyspark cassandra-driver
```

## Ejecución

Todos los notebooks y scripts usan rutas relativas a la raíz del repositorio (`data/raw/...`, `data/clean/...`, `data/gold/...`). Hay que correrlos parándose en esa raíz:

```bash
cd Cloud-Provider-Analytics-G2
```

### Scripts principales (`src/`)

```bash
# 1. Pipeline batch (genera data/gold/, data/quarantine/ y evidence/batch_pipeline_resumen.json)
python -m src.batch_pipeline

# 2. Simulación y comparación de micro-lotes y watermarks sobre los 120 JSONL
python -m src.streaming_pipeline

# 3. Validación y carga a Cassandra / AstraDB (usa ASTRA_TOKEN y ASTRA_SCB_PATH si están definidas)
python -m src.serving_cassandra
```

Para correr el job distribuido directamente con `pyspark` (en Google Colab o en un entorno con Java + Spark instalado):

```bash
python -m src.pyspark_etl
```

### Notebooks (`notebooks/`)

| Notebook | Entrada | Salida |
| --- | --- | --- |
| `notebooks/perfilado_usage_events_jsonl.ipynb` | `data/raw/usage_events_stream/*.jsonl` (120 archivos) | `evidence/perfilado_usage_events_resumen.json` |
| `notebooks/pipeline_batch_pyspark.ipynb` | `data/raw/*.csv` y `usage_events_stream/*.jsonl` | `data/gold/*.csv`, `data/quarantine/*.csv` |
| `notebooks/pipeline_streaming_pyspark.ipynb` | `data/raw/usage_events_stream/*.jsonl` | `evidence/streaming_pipeline_resumen.json` |
| `notebooks/limpieza_billing_monthly.ipynb` | `data/raw/billing_monthly.csv` | `data/clean/billing_monthly_limpio.csv` |
| `notebooks/limpieza_customers_orgs.ipynb` | `data/raw/customers_orgs.csv` | `data/clean/customers_orgs_limpio.csv` |
| `notebooks/LimpiezaResources.ipynb` | `data/raw/resources.csv` | `data/clean/resources_limpio.csv`, `evidence/integridad_referencial_resumen.json` |
| `notebooks/limpieza_support_ticketsCSV_TP.ipynb` | `data/raw/support_tickets.csv` | `data/clean/support_tickets_clean.csv` |
| `notebooks/user_limpieza.ipynb` | `data/raw/users.csv` | `data/clean/users_limpio.csv` |
| `notebooks/Limpieza_análisis_Marketing_touches_csv_y_nps_surveys_csv.ipynb` | `data/raw/marketing_touches.csv`, `data/raw/nps_surveys.csv` | `data/clean/marketing_touches_limpio.csv`, `data/clean/nps_surveys_clean.csv` |

## Pruebas

La suite de pruebas está en `tests/test_quality_and_pipeline.py` y valida los esquemas v1/v2, la imputación de `unit`, el envío de costos negativos a cuarentena, la conservación de `res_` y `org_`, las reglas de facturación, tickets y usuarios, la detección de spikes con MAD y la integridad referencial entre las 8 fuentes:

```bash
python -m unittest discover -s tests -v
```

## Notas y limitaciones

- En máquinas locales sin Java/JVM instalado, `src/batch_pipeline.py` y `src/streaming_pipeline.py` ejecutan las reglas de `src/quality_rules.py` en Python estándar para regenerar `data/gold/`, `data/quarantine/` y los resúmenes en `evidence/`, mientras que `src/pyspark_etl.py` concentra la ejecución en PySpark para Colab o clúster Spark.
- Para impactar en una base real de DataStax AstraDB hay que definir las variables de entorno `ASTRA_TOKEN` y `ASTRA_SCB_PATH` antes de correr `python -m src.serving_cassandra`. No se suben tokens ni archivos `.zip` de conexión al repositorio.
