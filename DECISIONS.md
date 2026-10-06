# Decisiones

## Estructura del repositorio

Se adopta el esquema `docs/`, `data/`, `notebooks/`, `src/`, `tests/`, `config/`, `infra/` y `evidence/`.

`datasets_raw/` pasa a `data/raw/` y `datasets_clean/` a `data/clean/`. La nota de limpieza de recursos sale de la carpeta de datos y queda en `docs/limpieza_resources.md`, porque describe el pipeline y no es un dataset.

Se mantiene la limpieza dentro de `notebooks/` en lugar de moverla ahora a `src/`. El código de cada dataset todavía está mezclado con exploración, chequeos y celdas de Colab. Extraerlo antes de tener las reglas cerradas duplicaría la lógica.

`src/`, `tests/`, `config/`, `infra/` y `evidence/` quedan creadas y vacías para la próxima etapa. No se inventan scripts, tests ni Docker que el proyecto todavía no usa.

## Datos

`data/raw/` no se modifica. Cada notebook lee desde ahí y escribe su resultado en `data/clean/`. Los CSV limpios se versionan para que el análisis no dependa de volver a ejecutar Colab.

No se suben secretos. Los tokens de GitHub se piden en el momento y no quedan en el repositorio.

## Limpieza ya aplicada

**Facturación mensual.** Los créditos nulos se completan con 0: representan facturas sin crédito, y usar la media inventaría descuentos. `month` pasa a fecha. Los montos se convierten a USD con `exchange_rate_to_usd`. Hay 12 filas con `subtotal` negativo (5 %). Se conservan, porque el dataset incluye créditos y ajustes, y no se tratan como error de carga.

**Organizaciones.** `signup_date` pasa a fecha. Un `nps_score` de 101 queda en nulo: el rango válido es -100 a 100. El resto de los NPS nulos se conserva; no corresponden a clientes recién ingresados y se evalúan más adelante.

**Recursos.** Identificadores sin el prefijo `res_` / `org_`. `CreatedDate` a datetime. `Service`, `Region` y `State` a categóricas por baja cardinalidad. `tags_json` se abre en `env`, `pii`, `costcenter`, `team` y `backup`. El detalle está en `docs/limpieza_resources.md`.

**Tickets.** Fechas a datetime. `category` y `severity` en minúsculas y sin espacios. CSAT fuera de 1.0 a 5.0 pasa a nulo. Un ticket sin `resolved_at` no puede tener CSAT.

**Usuarios.** Fechas a datetime, duplicados de usuario y de mail revisados, y corregido el caso en que `last_login` es anterior a `created_at`.

**Marketing y NPS.** `timestamp` a datetime. Hay 1.500 toques de 80 organizaciones. En NPS, `org_id` + `survey_date` no se repite. La encuesta sin NPS y sin comentario no aporta. Los valores de NPS fuera de un rango razonable se marcan como atípicos para el análisis, sin borrarlos solo por ser outliers.
