**INSTITUTO SUPERIOR TECNOLÓGICO EMPRESARIAL ARGENTINO (ISTEA)**
MINERÍA DE DATOS II - 2C 2026
Proyecto Integrador Obligatorio: Cloud Provider Analytics

# Supuestos, riesgos, mitigaciones, esfuerzo y próximos pasos

Este documento complementa el [diseño arquitectónico](DISENO_ARQUITECTONICO.md). Ahí queda la arquitectura. Acá queda lo que asumimos, lo que puede fallar, cuánto esfuerzo lleva y qué decisiones cerramos para esta segunda entrega.

## Supuestos

- Los 120 archivos JSONL de eventos (`data/raw/usage_events_stream/`) simulan un flujo que llega por micro-lotes a la carpeta Landing, pero traen un desorden de fechas de hasta 60 días entre archivos. Por eso distinguimos el watermark para reprocesar la muestra (`1440 hours` / `1368 hours`) del watermark para operación en vivo con llegada ordenada (`2 hours`).
- El entorno de Google Colab o la máquina local tiene conexión a Internet cuando se quiera impactar en la base Cloud de AstraDB.
- La capa Landing (`data/raw/`) no se modifica de acuerdo al principio de inmutabilidad.

## Riesgos y mitigaciones

| **Riesgo** | **Nivel** | **Impacto** | **Mitigación** |
| --- | --- | --- | --- |
| Desorden temporal entre los 120 JSONL y pérdida por watermark corto | Alto | El lote 0 ya tiene eventos del 31/08/2025; un watermark de 1 hora descarta el 99,08 % de los eventos (42.801 de 43.200) | Perfilado previo de los 120 archivos (`docs/perfilado_usage_events.md`); watermark parametrizado en `config/pipeline_config.json` (`1440 hours` para correr la muestra completa sin pérdida, `1368 hours` para cubrir el percentil 95, y `2 hours` para producción) |
| Evolución de esquema v1 → v2 (corte el 18/07/2025) | Alto | Falla la lectura o se pierden columnas entre los 10.800 eventos v1 y los 32.400 eventos v2 | Esquema explícito (`StructType`) en PySpark con `carbon_kg` y `genai_tokens` opcionales (`nullable=True`), sin usar `inferSchema` |
| Ruptura de joins por sacar prefijos en `resources` | Alto | Si se quitan `res_` y `org_` solo en `resources_limpio.csv`, no cruza ningún recurso con los eventos ni con organizaciones | Se conservan los prefijos `res_` y `org_` y los nombres en `snake_case`; verificado con 100 % de coincidencia en las 8 fuentes (`evidence/integridad_referencial_resumen.json`) |
| Tipos ambiguos, costos negativos y spikes en eventos | Alto | Métricas de costo y facturación alteradas | Los 216 eventos con `cost_usd_increment < 0` se separan a `data/quarantine/`; los 2.075 `unit` vacíos se completan según `metric`; los spikes se marcan con MAD (Modified Z-Score > 3,5) agrupado por `(service, metric)` (191 spikes vs. 158 con Z-Score > 3,0) |
| CSAT fuera de rango o en tickets abiertos (203 casos), fechas de login incoherentes (232) y subtotales negativos (13) | Medio | Métricas de Soporte, Usuarios y FinOps distorsionadas | Los 40 CSAT fuera de 1 a 5 y los 163 CSAT en tickets abiertos pasan a nulo y quedan registrados en `data/quarantine/support_tickets_csat_audit.csv`; los 232 `last_login < created_at` se anulan; los 13 subtotales negativos se conservan como notas de crédito |
| Falta de coincidencia entre divisas (160 USD, 51 ARS y 29 EUR) | Medio | Informes financieros de FinOps distorsionados | Imputación de los 137 créditos vacíos en 0 y conversión a USD en Silver con `exchange_rate_to_usd` |
| Muchos archivos chicos (*small files*) por sobre-particionar | Medio | Lectura lenta en Spark por exceso de metadatos si se divide un dataset de 12,37 MB por servicio y cliente | Estrategia única de partición: `year=YYYY/month=MM` para series de tiempo y sin partición de carpetas para tablas maestras chicas |

## Estimación de esfuerzo

### Roles

| **Rol** | **Responsabilidades** |
| --- | --- |
| Arquitectura e integración | Diagramas, patrón Lambda, `DECISIONS.md`, revisión final y coordinación |
| Ingesta batch | Bronze y Silver de maestros, tickets, encuestas y facturación |
| Ingesta streaming | Perfilado de los 120 JSONL, watermark, deduplicación por `event_id` y checkpoint |
| Calidad y Silver | Reglas de limpieza, quarantine, joins con dimensiones y anomalías (MAD vs. z-score) |
| Gold y Serving | Marts de negocio, modelo de Cassandra/AstraDB, carga y consultas CQL |

La documentación y el README son responsabilidad compartida, coordinada por el rol de arquitectura.

### Esfuerzo por etapa

| **Entrega** | **Tareas principales** | **Horas totales (aprox.)** |
| --- | --- | --- |
| **1** (hasta 07/10) | Problema y 5V (6), inventario y perfil de fuentes (8), arquitectura, diagrama y patrón (8), Data Lake (6), matriz (4), MapReduce (3), riesgos y plan (4), repositorio y README (4), revisión (4) | **30** |
| **2** (hasta 18/11) | Perfilado de los 120 JSONL y correcciones de la entrega 1 (14), Bronze batch (12), streaming y watermark (16), Silver y quarantine (18), Gold (10), Serving en Cassandra/AstraDB (16), anomalías MAD (10), gobierno preliminar (6), README y tests en `tests/` (12), backlog (6) | **120** |
| **Final** (hasta 09/12) | Carga y capturas en vivo sobre AstraDB (10), visualización / tablero sobre Gold y Serving (16), pruebas integrales y ensayo de demo remota (12), documentación final y presentación de defensa (16), reserva (10) | **64** |

### Recursos

| **Recurso** | **Uso** |
| --- | --- |
| Entorno local / Google Colab | Ejecución de notebooks, PySpark y suite de pruebas (`tests/`) |
| Almacenamiento local / Drive | Zonas del Data Lake (`data/raw`, `data/clean`, `data/gold`, `data/quarantine`) |
| GitHub | Repositorio versionado |
| AstraDB (Cassandra) | Serving; plan gratuito |

## Decisiones que estaban abiertas en la entrega 1 y cómo quedaron

En la primera entrega habíamos dejado esta tabla con posturas provisorias. Después de perfilar los 120 JSONL y armar el pipeline de la segunda entrega, quedaron cerradas así:

| **Decisión** | **Opciones que evaluamos** | **Evidencia medida en el dataset** | **Decisión adoptada** |
| --- | --- | --- | --- |
| Partición de los eventos | Solo por fecha, o abrir también por servicio / cliente | Los 43.200 eventos pesan 12,37 MB en dos meses (`2025-07` y `2025-08`). Abrir por servicio u `org_id` genera cientos de archivos de pocos KB | Una sola estrategia: `year=YYYY/month=MM` en Bronze, Silver y Gold para series de tiempo, y sin partición para tablas maestras |
| Duración del watermark | Corto (1 hora) o calibrado según el retraso de los archivos | Los eventos están mezclados a lo largo de los 60 días en los 120 archivos (mediana 59,76 días; p95 = 57 días). Con 1 hora se pierde el 99,08 % (1 archivo/lote) o el 91,58 % (10 archivos/lote) | Parametrizado en `config/pipeline_config.json`: `1440 hours` (60 días) para correr la muestra completa sin pérdida, `1368 hours` (57 días) para p95, y `2 hours` para producción ordenada |
| Identificadores en `resources` | Sacar los prefijos `res_` y `org_` o conservarlos | Sacarlos dejó en 0 % el cruce con `usage_events_stream` y `customers_orgs` | Se conservan `res_` y `org_` y nombres en `snake_case` en todas las tablas (100 % de cruce verificado) |
| Método de anomalías de costo | MAD o z-score, y por qué columnas agrupar | Dentro de un mismo servicio, `requests`, `cpu_hours` y `storage_gb_hours` tienen escalas de costo muy distintas | MAD (Modified Z-Score > 3,5) agrupado por `(service, metric)` como método principal (191 spikes), dejando z-score clásico (> 3,0, 158 spikes) como comparación |
| Claves de las tablas de Cassandra | Partición solo por `org_id`, o sumar `year_month` en tablas diarias | En `finops_cost_by_org_day` hay 11.050 filas en dos meses | `((org_id, year_month), usage_date, service)` en las tablas diarias para que cada partición tenga como máximo 147 filas (promedio 69) y no crezca sin límite |
| Alcance de la deduplicación en streaming | Dentro del watermark, más consolidación en Silver y Serving | Los 43.200 `event_id` son únicos en la muestra | `dropDuplicatesWithinWatermark(["event_id"])` en streaming, deduplicación por `event_id` en Silver y upsert por clave natural en Cassandra |

## Backlog priorizado para el cierre del MVP final (09/12)

| **Prioridad** | **Tarea** | **Estado** | **Qué falta para la defensa final** |
| --- | --- | --- | --- |
| **Obligatorio (Must)** | Pipeline Batch y Streaming reproducible con reglas de calidad, quarantine, marts Gold y suite de tests | Hecho en `src/`, `data/gold/`, `data/quarantine/` y `tests/` | Mantener congelado el alcance y usar los mismos comandos del README en la demo |
| **Obligatorio (Must)** | Esquema CQL query-first y script de carga y consulta para Cassandra / AstraDB | Hecho en `infra/cassandra_schema.cql` y `src/serving_cassandra.py` | Correr la carga contra la instancia gratuita de AstraDB con las variables de entorno y guardar capturas del CQL Console |
| **Deseable (Should)** | Gráficos de resumen y guion de demostración remota de 10 minutos | En armado | Preparar la presentación corta mostrando el flujo desde `data/raw/` hasta las 4 consultas de negocio |
| **Fuera de alcance (Won't)** | Modelo predictivo de pronóstico de costos o clúster Kafka externo | Descartado | Se dejó fuera para no sumar complejidad que ponga en riesgo la demo de punta a punta |
