# Limpieza de `resources.csv`

Este documento resume el trabajo hecho en `notebooks/LimpiezaResources.ipynb` sobre `data/raw/resources.csv` y explica la corrección aplicada después del feedback de la primera entrega.

## Qué tenía el archivo original

`data/raw/resources.csv` trae 400 filas y 7 columnas: `resource_id`, `org_id`, `service`, `region`, `created_at`, `state` y `tags_json`.

- No hay filas sin `resource_id` ni `org_id`, y los 400 `resource_id` son únicos.
- `tags_json` viene vacío en 83 de los 400 recursos (20,75 %). En los otros 317 trae un string JSON con etiquetas como `env`, `pii`, `cost_center`, `team` y `backup`.
- Los recursos se reparten en las 80 organizaciones (`org_id`) y en seis servicios: `compute` (116), `storage` (104), `database` (65), `networking` (44), `analytics` (40) y `genai` (31).

## Corrección de identificadores respecto de la primera entrega

En la primera entrega habíamos sacado los prefijos fijos `res_` y `org_` y habíamos cambiado los nombres de las columnas a `PascalCase` (`CreatedDate`, `Service`, `Region`, `State`).

El problema marcado en la devolución fue que tanto `customers_orgs.csv` como las demás tablas CSV y los 43.200 eventos de `usage_events_stream` mantienen los identificadores completos con `org_` y `res_`. Sacarlos solo en `resources` hacía que ningún recurso cruzara en los joins (0 % de coincidencia).

Para corregirlo:

1. Se conservan intactos los valores originales de `resource_id` (`res_...`) y `org_id` (`org_...`).
2. Se mantienen todos los nombres de columna en `snake_case`: `resource_id`, `org_id`, `service`, `region`, `created_at`, `state`, `env`, `pii`, `costcenter`, `team` y `backup`.

## Transformaciones aplicadas

1. **Fechas y texto:** `created_at` se pasa a formato fecha (`YYYY-MM-DD`), y `service`, `region` y `state` se normalizan en minúsculas y sin espacios laterales.
2. **Apertura de `tags_json`:** se parsea el JSON de cada fila y se extraen cinco columnas (`env`, `pii`, `costcenter`, `team`, `backup`). Luego se elimina `tags_json`.
3. **Imputación de etiquetas vacías:** cuando un recurso no traía `tags_json` o le faltaba alguna clave, se completa con un valor explícito para no dejar nulos:
   - `env` → `Sin Especificar`
   - `costcenter` → `Sin Asignar`
   - `team` → `Sin Asignar`
   - `pii` → `False`
   - `backup` → `False` (los valores como `daily` pasan a `True`)

## Verificación de integridad referencial

Con `data/clean/resources_limpio.csv` regenerado (400 filas, 11 columnas, 0 nulos):

- Los 80 `org_id` de `resources_limpio.csv` coinciden al **100 %** con `data/clean/customers_orgs_limpio.csv`.
- Los 400 `resource_id` que aparecen en los 43.200 eventos de `data/raw/usage_events_stream/*.jsonl` coinciden al **100 %** con `data/clean/resources_limpio.csv` (0 recursos huérfanos).

El detalle numérico de todos los cruces quedó guardado en `evidence/integridad_referencial_resumen.json`.
