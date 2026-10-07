**INSTITUTO SUPERIOR TECNOLÓGICO EMPRESARIAL ARGENTINO (ISTEA)**
MINERÍA DE DATOS II - 2C 2026
Proyecto Integrador Obligatorio: Cloud Provider Analytics

# Matriz requisito-componente

Cada fila sale de un objetivo medible o de una pregunta de negocio del [diseño arquitectónico](DISENO_ARQUITECTONICO.md). La última columna dice qué componente lo cubre.

| Requisito | De dónde sale | Componente | Qué cubre |
| --- | --- | --- | --- |
| Costo diario por organización y servicio, en USD | FinOps | PySpark Structured Streaming y el mart `finops_org_daily_usage` | El stream aporta el consumo del día. Gold lo agrega por organización y servicio. |
| Desvíos y anomalías de costo | FinOps | Reglas de calidad y el método de anomalías (MAD, con z-score de comparación) | Lo imposible va a Quarantine. El spike se marca, no se borra. |
| Facturas en USD, EUR y ARS comparables | FinOps, y el hallazgo de las 160 facturas USD con tasa distinta de 1 | Conversión en Silver con `exchange_rate_to_usd` | El monto original se conserva. El informe suma solo la columna en USD. |
| Tasa de SLA y CSAT promedio por región | Soporte | Batch de `support_tickets` y el mart `support_sla_metrics` | El ticket se queda en Silver. El CSAT inválido no entra al promedio. |
| Evolución de compute, base de datos, tokens de GenAI y carbono | Producto | Silver de eventos v1/v2 y el mart `product_genai_adoption` | `carbon_kg` y `genai_tokens` existen para las dos versiones. En v1 quedan nulos. |
| El 100 % de los registros inválidos se desvía, sin frenar el pipeline, y cada corrida informa el porcentaje | Objetivo de calidad | Quarantine y el rules engine | El rechazo lleva motivo. El pipeline sigue. |
| Consulta por clave de partición en menos de 100 ms | Objetivo de latencia | Cassandra / AstraDB, modelado query-first | La lectura del tablero no escanea el Data Lake. |
| El 100 % de los eventos v1 y v2 se leen sin error | Objetivo de evolución de esquema | PySpark Schema Enforcement | Un solo esquema. Las columnas nuevas de v2 son opcionales. |
| Muchos eventos diarios, más los maestros | Volumen | Parquet (Snappy) en Bronze, Silver y Gold | Los CSV maestros no justifican Spark. El stream sí. |
| Alertas de consumo en el día y cierre mensual exacto | Velocidad, y el patrón Lambda | Speed layer para el día. Batch layer para el cierre contra `billing_monthly` | El batch corrige lo que el stream todavía no vio. |
| CSV, JSON de tags y JSONL v1/v2 en el mismo lago | Variedad | Landing sin transformar, y Silver como formato único | Landing no se toca. Silver aplana y tipa. |
| Rentabilidad, churn y huella de carbono en un tablero | Valor | Marts de Gold cargados a Cassandra | FinOps, Soporte y Producto leen una tabla cada uno, no las fuentes crudas. |
