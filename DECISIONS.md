# Decisiones

## Estructura del repositorio

Se mantiene el esquema `docs/`, `data/`, `notebooks/`, `src/`, `tests/`, `config/`, `infra/` y `evidence/`.

`data/raw/` guarda los archivos originales y `data/clean/` los CSV limpios que salen de los notebooks. La nota de limpieza de recursos queda en `docs/limpieza_resources.md` y el perfilado de los 120 archivos JSONL en `docs/perfilado_usage_events.md`.

En esta segunda etapa las reglas de limpieza y agregación pasan a módulos en `src/`, la configuración de rutas y umbrales va en `config/pipeline_config.json`, el esquema de Cassandra en `infra/cassandra_schema.cql`, las pruebas en `tests/` y los resúmenes de corrida en `evidence/`. En `data/gold/` quedan los marts agregados y en `data/quarantine/` los registros rechazados con su motivo.

## Datos y reproducibilidad

`data/raw/` no se modifica. Los notebooks y scripts leen desde ahí con rutas relativas a la raíz del repositorio (`data/raw/...`) y escriben en `data/clean/`, `data/gold/`, `data/quarantine/` o `evidence/`.

Se sacaron de los notebooks todas las celdas que pedían tokens con `getpass`, clonaban el repositorio o usaban `google.colab` para descargar archivos. Tampoco se guardan credenciales ni el bundle de AstraDB en el repositorio: la conexión a Cassandra lee `ASTRA_TOKEN` y `ASTRA_SCB_PATH` desde variables de entorno.

## Correcciones sobre el feedback de la primera entrega

**Lo medido vs. lo proyectado (5V).** En `docs/DISENO_ARQUITECTONICO.md` se separó lo que mide la muestra del trabajo práctico (47.212 filas en total y 12,73 MB sumando los 7 CSV y los 120 JSONL) de lo que proyectamos para un entorno productivo (más de 10.000 organizaciones y ~160 GB/mes). La muestra entra en memoria local, pero usamos Spark, Parquet y Cassandra para resolver sobre el caso de prueba los mismos problemas de esquema, datos tardíos, particionamiento y consultas por clave que aparecen a escala real.

**Perfilado de `usage_events_stream` y valor del watermark.** Antes de implementar el streaming se perfilaron los 120 archivos JSONL (360 líneas por archivo, 43.200 eventos, 0 líneas rotas). Vimos que los eventos cubren 60 días (`2025-07-03` a `2025-08-31`), pero dentro de cada archivo vienen mezclados a lo largo de casi todo ese rango (la mediana del rango dentro de un mismo archivo es de 59,76 días). Ya en el primer archivo (`events_part_0000.jsonl`) aparece un evento del `2025-08-31 16:32:00`. Por eso, el `withWatermark('timestamp', '1 hour')` que habíamos propuesto en la primera entrega descartaba 42.801 de los 43.200 eventos (el 99,08 %) si se leía archivo por archivo, o 39.366 eventos válidos si se leía de a 10 archivos por lote. Con los datos medidos, parametrizamos el watermark en `config/pipeline_config.json`: para correr la muestra completa sin perder datos por el desorden entre archivos usamos `1440 hours` (60 días, retiene el 100 %), para cortar en el percentil 95 de retraso medido usamos `1368 hours` (57 días, retiene el 95,41 %), y dejamos `2 hours` como valor para operación en vivo con llegada ordenada.

**Particionamiento único por dataset en el Data Lake.** Se corrigió la inconsistencia entre secciones del documento de arquitectura. Queda una sola estrategia por tipo de tabla: las series de tiempo (`usage_events`, `support_tickets`, `marketing_touches` y los marts diarios de Gold) se particionan únicamente por `year=YYYY/month=MM`. Con 12,37 MB de eventos en dos meses (`2025-07` y `2025-08`), abrir también por `service` o por `org_id` generaba cientos de archivos de pocos kilobytes. Las tablas chicas de referencia (`customers_orgs`, `resources`, `users`, `nps_surveys` y `billing_monthly`) van sin partición de carpetas.

**Identificadores en `resources` e integridad referencial.** En la primera entrega habíamos sacado los prefijos `res_` y `org_` y pasado las columnas a `PascalCase` en `resources_limpio.csv`. Como el resto de las tablas y los 43.200 eventos JSONL usan `res_XXXX` y `org_XXX`, ese cambio rompía todos los joins. Volvimos a generar `data/clean/resources_limpio.csv` desde `notebooks/LimpiezaResources.ipynb` conservando los prefijos `res_` y `org_` y los nombres en `snake_case`. Con eso verificamos que los 400 recursos y las 80 organizaciones cruzan al 100 % con los eventos y las demás tablas (`evidence/integridad_referencial_resumen.json`).

**MapReduce con grano consistente.** En la primera entrega el ejemplo mezclaba `billing_monthly` y los eventos para armar la clave `(org_id, year_month, service)`, cuando `billing_monthly` no tiene la columna `service`. Lo rehícimos usando únicamente `usage_events_stream` para responder cuánto gastó y cuánto carbono emitió cada organización por mes y servicio. El Combiner local baja los pares de 42.984 a 29.457 (31,47 % menos de tráfico al shuffle) y el Reducer consolida las 524 combinaciones `(org_id, year_month, service)` (`evidence/mapreduce_finops_resumen.json`).

## Limpieza y reglas aplicadas por fuente

**Eventos de uso (`usage_events_stream`).** Hay 10.800 eventos con `schema_version = 1` (del 3 al 17 de julio) que no traen `carbon_kg` ni `genai_tokens`, y 32.400 eventos con `schema_version = 2` (del 18 de julio al 31 de agosto) donde `carbon_kg` viene siempre y `genai_tokens` viene en los 3.132 eventos del servicio `genai`. Se leen con un único esquema explícito donde ambos campos son opcionales. Los 216 eventos con `cost_usd_increment` negativo (0,5 %) se desvían a `data/quarantine/usage_events_quarantine.csv`. En los 42.984 eventos válidos, los 2.075 vacíos de `unit` (4,8 %) se completan según `metric` (`cpu_hours` → `hours`, `requests` → `count`, `storage_gb_hours` → `gb_hours`), y los 877 vacíos de `value` se dejan nulos para no inventar consumo. Para detectar picos de costo comparamos MAD (Modified Z-Score > 3,5) contra Z-Score clásico (> 3,0) agrupando por `(service, metric)`, porque cada métrica tiene otra escala de precio; MAD detecta 191 spikes frente a 158 del Z-Score.

**Facturación mensual (`billing_monthly`).** Los 137 créditos nulos se completan con 0: representan facturas sin crédito, y usar la media inventaría descuentos. `month` pasa a fecha. Los montos se convierten a USD con `exchange_rate_to_usd`. Las 13 filas con `subtotal` negativo se conservan y se marcan como notas de crédito o ajustes.

**Organizaciones (`customers_orgs`).** `signup_date` pasa a fecha. El único `nps_score` de 101 queda en nulo porque el rango válido es de −100 a 100. Los otros 11 NPS vacíos se conservan nulos.

**Recursos (`resources`).** Se conservan los prefijos `res_` y `org_` y los nombres en `snake_case`. `created_at` pasa a fecha. `tags_json` se abre en `env`, `pii`, `costcenter`, `team` y `backup`, completando los faltantes con `Sin Especificar`, `Sin Asignar` y `False`. El detalle está en `docs/limpieza_resources.md`.

**Tickets (`support_tickets`).** Fechas a datetime, `category` y `severity` en minúsculas y sin espacios. Los 40 tickets con CSAT fuera de 1 a 5 y los 163 tickets todavía abiertos (`resolved_at` vacío) que traían CSAT en rango pasan a CSAT nulo (203 casos registrados en `data/quarantine/support_tickets_csat_audit.csv`).

**Usuarios (`users`).** Fechas a datetime, mail y rol en minúsculas y sin espacios. No hay usuarios ni mails repetidos. En los 232 casos donde `last_login` es anterior a `created_at`, se anula `last_login` y se deja registro en `data/quarantine/users_login_audit.csv`.

**Marketing y NPS (`marketing_touches` y `nps_surveys`).** En marketing (`1.500` toques de las 80 organizaciones) `timestamp` pasa a fecha y se agrega `mes`. En NPS, `org_id` + `survey_date` no se repite; la única fila que venía sin puntaje y sin comentario (`org_064` del `2025-07-09`) pasa a `data/quarantine/nps_surveys_quarantine.csv`, quedando 91 encuestas válidas en `data/clean/nps_surveys_clean.csv` (se eliminó el archivo duplicado `nps_surveys_clean_limpio.csv`).
