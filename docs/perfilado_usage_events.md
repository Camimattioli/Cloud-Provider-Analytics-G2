# Perfilado de `usage_events_stream` (120 archivos JSONL)

Este documento resume lo que encontramos al explorar los 120 archivos de `data/raw/usage_events_stream/` (`events_part_0000.jsonl` a `events_part_0119.jsonl`). El código y las tablas completas están en `notebooks/perfilado_usage_events_jsonl.ipynb` y los números de salida en `evidence/perfilado_usage_events_resumen.json`.

El objetivo de este perfilado fue responder la observación principal de la primera entrega: antes de fijar el watermark e implementar Structured Streaming, medir la distribución temporal, la evolución de esquema v1/v2, los nulos, los duplicados y los costos negativos o atípicos sobre los datos reales.

## 1. Volumen, archivos y rango de fechas

- **Cantidad de archivos:** 120 archivos JSONL.
- **Filas por archivo:** exactamente 360 líneas en cada archivo, lo que suma **43.200 eventos** (12.970.715 bytes, **12,37 MB**).
- **Líneas corruptas:** 0 (todos los JSON se parsean sin error).
- **Rango temporal:** del `2025-07-03T00:02:00Z` al `2025-08-31T23:58:00Z` (**60 días exactos**).
- **Volumen diario y mensual:** hay exactamente **720 eventos por día** durante los 60 días (`2025-07` tiene 20.880 eventos en 29 días; `2025-08` tiene 22.320 eventos en 31 días).
- **Cobertura de organizaciones y recursos:** aparecen las **80 organizaciones** (`org_...`) y los **400 recursos** (`res_...`), coincidiendo al 100 % con `customers_orgs_limpio.csv` y `resources_limpio.csv`.

## 2. Evolución de esquema (`schema_version` 1 y 2)

El cambio de versión tiene un corte limpio por fecha el **`2025-07-18T00:00:00Z`**:

| Versión | Cantidad de eventos | % del total | Rango de fechas | Qué cambia en las columnas |
| --- | --- | --- | --- | --- |
| `schema_version = 1` | 10.800 | 25,0 % | `2025-07-03` al `2025-07-17` (15 días) | Trae 11 campos base. No incluye las claves `carbon_kg` ni `genai_tokens`. |
| `schema_version = 2` | 32.400 | 75,0 % | `2025-07-18` al `2025-08-31` (45 días) | Suma `carbon_kg` en el 100 % de los eventos v2 (32.400 filas, 0 nulos) y `genai_tokens` en el 100 % de los eventos v2 donde `service == 'genai'` (3.132 filas; en los otros 5 servicios la clave no viene). |

**Decisión para la lectura en Spark:** no se puede usar `inferSchema` sobre los primeros archivos porque si toma un lote con eventos v1 ignora `carbon_kg` y `genai_tokens`. Definimos en `src/schemas.py` un único `StructType` con las 13 columnas de negocio (dejando `carbon_kg` y `genai_tokens` como `nullable=True`) más `_corrupt_record`.

## 3. Nulos y cómo se tratan

| Columna | Nulos en los 43.200 eventos | % | Criterio aplicado en Silver |
| --- | --- | --- | --- |
| `event_id`, `timestamp`, `org_id`, `resource_id`, `service`, `region`, `metric`, `cost_usd_increment`, `schema_version` | 0 | 0,0 % | Campos obligatorios completos. |
| `unit` | 2.075 | 4,80 % | En los 41.125 eventos donde `unit` sí viene, la relación con `metric` es 1 a 1 sin excepciones: `cpu_hours` → `hours`, `requests` → `count`, `storage_gb_hours` → `gb_hours`. Por eso los 2.075 vacíos se completan a partir de `metric` y se marca `is_unit_imputed = True`. |
| `value` | 877 | 2,03 % | Esos 877 eventos tienen `cost_usd_increment` válido. No se rellenan con 0 ni con la media para no distorsionar tasas de consumo; quedan en `NULL` con el flag `is_value_null = True`. |
| `carbon_kg` | 10.800 | 25,0 % | Corresponden todos a los eventos `v1`. En `v2` no hay nulos. |
| `genai_tokens` | 40.068 | 92,75 % | Solo aplica a los eventos `v2` del servicio `genai` (3.132 eventos, todos con valor). |

## 4. Duplicados, costos negativos y picos de costo (spikes)

- **Duplicados de `event_id`:** 0 (hay 43.200 `event_id` distintos). Si miramos la combinación `(org_id, resource_id, timestamp, metric)` sin el `event_id`, hay 13 combinaciones que se repiten (27 filas en total con distinto `event_id` y distinto costo). Se mantiene la deduplicación por `event_id`.
- **Costos negativos (`cost_usd_increment < 0`):** hay **216 eventos (0,50 %)** con incremento de costo negativo (llegan hasta `-154,46 USD`, y 211 de ellos son menores a `-0,01 USD`). A diferencia de `billing_monthly` (donde el subtotal mensual puede ser negativo por una nota de crédito), en un evento puntual de telemetría de consumo un costo negativo es un error de origen. Esos 216 eventos se desvían a `data/quarantine/usage_events_quarantine.csv` con motivo `NEGATIVE_COST_INCREMENT`, quedando **42.984 eventos válidos** para Silver y Gold (que suman **148.351,60 USD** y **365,56 kg de CO2**).
- **Detección de spikes (`MAD` vs. `Z-Score`):** dentro de cada servicio conviven tres métricas (`requests`, `cpu_hours`, `storage_gb_hours`) con escalas de costo muy diferentes. Por eso calculamos la mediana, la desviación absoluta mediana (MAD), la media y el desvío estándar agrupando por **`(service, metric)`** sobre los 42.984 eventos válidos:
  - Con **Modified Z-Score (`0.6745 * |x - mediana| / MAD > 3,5`)** se detectan **191 spikes**.
  - Con **Z-Score clásico (`(x - media) / std > 3,0`)** se detectan **158 spikes** (los 158 están incluidos dentro de los 191 de MAD, porque los valores extremos inflan el desvío estándar y tapan otros picos).
  - Nos quedamos con MAD por `(service, metric)` como método principal y guardamos los 191 eventos en `data/gold/cost_anomalies_by_org.csv`.

## 5. Desorden temporal entre archivos y calibración del watermark

Al revisar el mínimo y máximo `timestamp` dentro de cada uno de los 120 archivos (`events_part_0000.jsonl` a `0119.jsonl`) encontramos por qué un watermark de 1 hora no sirve para correr este dataset:

- Cada archivo individual contiene eventos repartidos a lo largo de casi los 60 días completos: la mediana de amplitud dentro de un mismo archivo es de **59,76 días** (el archivo con menor amplitud abarca 59,08 días).
- Ya en el primer archivo (`events_part_0000.jsonl`), el `timestamp` mínimo es `2025-07-03T01:41:00Z` y el máximo llega a **`2025-08-31T16:32:00Z`**.
- En Spark Structured Streaming, al terminar de procesar el lote 0 el motor actualiza el watermark a `max_event_time_visto - tolerancia`. Si la tolerancia es de `1 hour`, el watermark queda en `2025-08-31T15:32:00Z` desde el primer lote y casi todos los eventos de julio y agosto que llegan en los archivos `0001` a `0119` caen por debajo del watermark y se descartan.

Medimos los percentiles de retraso (`max_ts_visto_hasta_lote_anterior - ts_evento`) en los lotes 1 al 119:

- **Mediana (p50):** 720,08 horas (30,00 días)
- **Percentil 90 (p90):** 1.296,13 horas (54,01 días)
- **Percentil 95 (p95):** 1.368,00 horas (57,00 días)
- **Percentil 99 (p99):** 1.425,38 horas (59,39 días)
- **Máximo (p100):** 1.439,90 horas (60,00 días)

### Simulación de pérdida según el valor del watermark

| Umbral de watermark | Eventos retenidos (1 archivo/lote) | Eventos descartados | % retenido | Observación |
| --- | --- | --- | --- | --- |
| `1 hour` (valor teórico de la entrega 1) | 399 | 42.801 | **0,92 %** | Pierde el 99,08 % de los eventos (en lotes de 10 archivos retiene solo el 8,42 % de los válidos y pierde 134.853,01 USD). |
| `24 hours` (1 día) | 1.076 | 42.124 | **2,49 %** | Sigue perdiendo el 97,51 % por la mezcla de 60 días entre archivos. |
| `720 hours` (30 días - p50) | 21.842 | 21.358 | **50,56 %** | Retiene la mitad de la muestra. |
| `1368 hours` (57 días - p95) | 41.058 | 2.142 | **95,04 %** | Cubre el percentil 95 de retraso medido (en lotes de 10 archivos retiene el 95,41 % de los eventos válidos). |
| `1440 hours` (60 días - p100) | 43.200 | 0 | **100,00 %** | Retiene el 100 % de los eventos de la muestra (42.984 válidos + 216 enviados a Quarantine). |

Con esta evidencia dejamos el watermark parametrizado en `config/pipeline_config.json`: usamos `1440 hours` como perfil por defecto para procesar la muestra completa sin pérdida (`replay_full`), `1368 hours` como perfil del percentil 95 (`replay_p95`), y `2 hours` como perfil para un entorno productivo donde los eventos llegan en tiempo real ordenados.
