**INSTITUTO SUPERIOR TECNOLÓGICO EMPRESARIAL ARGENTINO (ISTEA)**
MINERÍA DE DATOS II - 2C 2026
Proyecto Integrador Obligatorio: Cloud Provider Analytics

# Supuestos, riesgos, mitigaciones, esfuerzo y próximos pasos

Este documento complementa el [diseño arquitectónico](DISENO_ARQUITECTONICO.md). Ahí queda la arquitectura. Acá queda lo que asumimos, lo que puede fallar, cuánto esfuerzo lleva y qué sigue abierto.

## Supuestos

- Los archivos JSONL de eventos simulan un flujo continuo que llega por micro-lotes en la carpeta Landing.
- La infraestructura de Google Colab o entorno local posee conectividad a Internet para interactuar con la instancia Cloud de AstraDB.
- La capa Landing permanece inalterada de acuerdo al principio de inmutabilidad.

## Riesgos y mitigaciones

| **Riesgo** | **Nivel** | **Impacto** | **Mitigación** |
| --- | --- | --- | --- |
| Evolución de esquema v1 → v2 | Alto | Falla la lectura de eventos sin carbon_kg o genai_tokens | Esquema explícito en PySpark con esos campos opcionales; prueba con ambas versiones |
| Duplicados, datos tardíos y re-ejecución del pipeline | Alto | Costos, consumo e ingresos duplicados o distorsionados en Serving | Watermark, deduplicación por event_id y checkpoint; upserts con clave primaria natural (org_id, usage_date, service) en Cassandra |
| Tipos ambiguos, costos negativos y spikes | Alto | Métricas de costo y facturación alteradas | Cast con fallback controlado; regla cost_usd_increment >= -0.01 con flag; inválidos a quarantine/; anomalías por z-score, MAD o percentiles |
| CSAT fuera de rango y con tickets abiertos (172 de 240), fechas inconsistentes y subtotales negativos | Medio | Métricas de Soporte y FinOps distorsionadas | Reglas de rango y coherencia; CSAT inválido a quarantine; fechas inconsistentes solo con flag en Silver (no se eliminan); criterio de subtotales negativos en DECISIONS.md |
| Falta de coincidencia entre divisas (EUR/ARS y 160 facturas en USD con tasa ≠ 1) | Medio | Informes financieros de FinOps distorsionados | Conversión obligatoria a USD en Silver con exchange_rate_to_usd; criterio documentado y validado con el docente |
| Resultados inconsistentes entre batch y streaming | Medio | Cifras distintas para la misma métrica | Reglas de calidad compartidas y upserts por clave natural |

## Estimación de esfuerzo

### Roles

| **Rol** | **Responsabilidades** |
| --- | --- |
| Arquitectura e integración | Diagrama, patrón, DECISIONS.md, revisión final y coordinación |
| Ingesta batch | Bronze de maestros, tickets, encuestas y facturación |
| Ingesta streaming | Bronze de eventos: watermark, deduplicación, checkpoint |
| Calidad y Silver | Reglas, quarantine, joins, features y anomalías |
| Gold y Serving | Marts, modelo de Cassandra/AstraDB, carga y consultas CQL |

La documentación y el README son responsabilidad compartida, coordinada por el rol de arquitectura.

### Esfuerzo por etapa

| **Entrega** | **Tareas principales** | **Horas totales (aprox.)** |
| --- | --- | --- |
| **1** (hasta 07/10) | Problema y 5V (6), inventario y perfil de fuentes (8), arquitectura, diagrama y patrón (8), Data Lake (6), matriz (4), MapReduce (3), riesgos y plan (4), repositorio y README (4), revisión (4) | **30** |
| **2** (hasta 18/11) | Bronze batch (12), streaming a Bronze (16), Silver (16), calidad y quarantine (12), Gold (8), Serving en AstraDB (16), analítica (10), idempotencia (6), gobierno preliminar (6), README y pruebas (8), backlog (4) | **120** |
| **Final** (hasta 09/12) | Marts restantes (20), analítica/ML completo (14), gobierno, seguridad y observabilidad (10), pruebas e idempotencia (10), documentación técnica y diccionario (12), presentación y video (12), ensayo de defensa (6), reserva (10) | **90** |

### Recursos

| **Recurso** | **Uso** |
| --- | --- |
| Google Colab | Ejecución de PySpark y Structured Streaming |
| Google Drive o almacenamiento del entorno | Zonas del Data Lake |
| GitHub | Repositorio versionado |
| AstraDB (Cassandra) | Serving; plan gratuito |
| draw.io | Diagramas |

## Próximos pasos

Estas decisiones quedan abiertas para la entrega 2. La postura de la columna de la derecha es la que seguimos mientras no haya evidencia para cambiarla.

| **Decisión** | **Opciones** | **Criterio para decidir** | **Postura provisoria** |
| --- | --- | --- | --- |
| Partición de los eventos | Solo por fecha, o fecha y servicio | Tamaño y cantidad de archivos generados; evitar muchos archivos chicos | Fecha en Bronze, fecha y servicio en Silver; se valida con tamaños reales en la entrega 2 |
| Claves de las tablas de Cassandra | Partición por (org_id, service) con clustering por fecha, o sumar el mes a la clave de partición | Que ninguna partición crezca sin límite y que cada consulta lea una sola partición | (org_id, service) + fecha como clustering; se revisa si las particiones crecen |
| Carga a AstraDB | Conector Spark–Cassandra, o driver con foreachBatch | Compatibilidad con Colab, facilidad de hacer upserts y límites del plan gratuito | Conector; el driver queda como alternativa si falla |
| Duración del watermark | Corto (baja latencia, pierde datos tardíos) o largo (más completo, más lento y más estado en memoria) | Retraso real de los eventos observado en los JSONL | ajustado tras medir |
| Alcance de la deduplicación en streaming | Solo dentro de la ventana del watermark, o también contra todo el histórico de Bronze | Costo de memoria frente al riesgo de duplicados que lleguen muy tarde | Dentro del watermark, más upsert en Silver como segunda barrera |
| Método de anomalías de costo | z-score, MAD o percentiles, por organización y servicio | Robustez ante spikes (la media y el desvío se distorsionan con ellos) y cantidad de falsos positivos | MAD como método principal, con z-score como comparación |
| Alcance del componente analítico | Solo detección de anomalías, o sumar pronóstico de costos | Que no ponga en riesgo el pipeline de punta a punta | Solo anomalías; el pronóstico queda en el backlog |
| Historial de los maestros | Guardar solo el estado actual, o conservar el historial de cambios (SCD) | Si hace falta saber, por ejemplo, qué plan tenía una organización cuando se emitió cada factura | Estado actual con ingest_date; SCD solo si surge la necesidad |
| Estrategia de reproceso | Reprocesar todo desde Landing, o solo las particiones afectadas | Tiempo de ejecución y costo | Por partición de fecha, con reproceso completo como último recurso |
| Entorno de ejecución | Solo Google Colab, o sumar Docker | Reproducibilidad desde un entorno limpio | Colab con guía de instalación; Docker en infra/ si sobra tiempo |
| Conexión de la herramienta de visualización con AstraDB | Conexión directa, o una capa intermedia (por ejemplo, exportar desde Gold) | Que la herramienta elegida se pueda conectar a la base; hay que validarlo | Se define al elegir la herramienta |
