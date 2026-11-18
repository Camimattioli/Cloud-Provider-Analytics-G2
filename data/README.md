# Datos: 7 CSV y 120 JSONL

El trabajo tiene siete tablas CSV y una carpeta con 120 archivos JSONL de eventos de uso. Todas corresponden a las mismas 80 organizaciones y se cruzan por `org_id` (y por `resource_id` entre recursos y eventos). `raw/` tiene los archivos originales sin tocar y `clean/` la salida de los notebooks de limpieza.

La encuesta NPS es la única que no cubre el total: tiene respuestas de 60 organizaciones, no de las 80.

La limpieza sigue siempre la misma idea:

1. Los identificadores (`org_id`, `resource_id`, `user_id`, `ticket_id`, `invoice_id`, `touch_id`, `event_id`) conservan su prefijo para no romper los cruces entre tablas.
2. Las fechas que venían como texto pasan a fecha, para poder ordenar y agrupar por período.
3. Un vacío se completa solo cuando significa "no hubo", "no tiene etiqueta" o cuando sale directo de otra columna (como `unit` a partir de `metric`). Si puede ser un dato que falta de verdad, se deja nulo.
4. Un valor imposible pasa a nulo o se separa a `data/quarantine/` con el motivo. Un valor posible pero incómodo, como una factura con subtotal negativo por ajuste, se conserva.
5. El texto de categorías se normaliza (minúsculas y sin espacios de más).
6. Los montos en otra moneda se calculan en USD sin pisar la moneda original.

## `customers_orgs`

Ficha del cliente: industria, plan (`plan_tier`), región, etapa comercial y un NPS de cabecera. Sirve de tabla madre: el resto de las fuentes cuelga de su `org_id`.

Qué se limpió:

- `signup_date` pasó de texto a fecha.
- El NPS se revisó contra la escala válida, de −100 a 100. El único valor imposible, un 101, pasó a nulo.
- Los NPS que ya venían vacíos se dejaron vacíos. No se rellenaron con un promedio.

Qué se encontró: 80 clientes. El plan más común es standard (41), después pro (21), enterprise (10) y free (8). Hay 54 activos, 11 en riesgo, 6 dados de baja, 6 en prueba y 3 leads. El NPS de cabecera venía vacío en 11 fichas; con el 101 anulado, el limpio queda con 12 nulos y valores entre −38 y 81.

## `billing_monthly`

Factura mensual de cada organización: subtotal, créditos, impuestos y moneda. Permite ver cuánto paga cada cliente y comparar meses.

Qué se limpió:

- 137 créditos vacíos se completaron con 0. Vacío acá quiere decir que esa factura no tuvo crédito, no que el importe se haya perdido. Usar la media habría inventado descuentos.
- `month` pasó de texto a fecha.
- Se agregaron `subtotal_usd`, `taxes_usd` y `credits_usd`, multiplicando cada monto por `exchange_rate_to_usd`. Así se pueden sumar facturas en USD, ARS y EUR.
- La moneda original y el tipo de cambio se conservan.
- Los subtotales negativos no se corrigieron ni se borraron.

Qué se encontró: 240 facturas, tres meses por organización (junio, julio y agosto de 2025), en USD (160), ARS (51) y EUR (29). Trece subtotales son negativos (doce al pasarlos a USD). Se leen como ajustes o notas de crédito, que el propio dataset dice incluir, y no como filas rotas.

## `resources`

Recursos cloud de cada cliente: servicio, región, estado y etiquetas. Sirve para ver qué tiene contratado cada organización y cruzarlo con los eventos de uso.

Qué se limpió:

- Se conservaron los prefijos `res_` en `resource_id` y `org_` en `org_id`, y los nombres de columna en `snake_case` (`resource_id`, `org_id`, `service`, `region`, `created_at`, `state`, `env`, `pii`, `costcenter`, `team`, `backup`). En la primera entrega habíamos sacado los prefijos y eso impedía cruzar esta tabla con los eventos y con `customers_orgs`.
- `created_at` pasó a fecha.
- `service`, `region` y `state` se normalizaron en minúsculas.
- `tags_json` venía como texto JSON, a veces vacío. Cada clave se abrió en una columna (`env`, `pii`, `costcenter`, `team` y `backup`) y la columna original `tags_json` se sacó.
- Lo que seguía vacío después de abrir los tags se completó con un valor explícito: `Sin Especificar` en entorno, `Sin Asignar` en centro de costo y equipo, y `False` en `pii` y `backup` (`backup: daily` se leyó como verdadero).

Qué se encontró: 400 recursos y ninguno sin identificador. Todos cruzan con las 80 organizaciones y con los recursos que aparecen en los eventos JSONL. Compute es el servicio más frecuente (116), seguido de storage (104), database (65), networking (44), analytics (40) y genai (31). Hay 242 en ejecución, 119 detenidos y 39 dados de baja. 83 recursos llegaron sin tags. El archivo limpio (`data/clean/resources_limpio.csv`) no tiene nulos.

## `users`

Personas de cada organización: mail, rol, si está activa, fecha de alta y último login. Sirve para ver adopción y cuentas inactivas.

Qué se limpió:

- `created_at` y `last_login` pasaron a fecha.
- El mail quedó en minúsculas y sin espacios. El rol quedó sin espacios.
- Se buscaron `user_id` y mails repetidos. No había.
- Si `last_login` era anterior a `created_at`, el login se anuló. La fecha de alta se dejó como estaba: el dato incoherente es el login, no el alta.

Qué se encontró: 800 usuarios, todos con id y mail distintos, y 728 activos. 139 no tenían último login. Otros 232 lo tenían antes de la fecha de alta; en el limpio esos login quedaron vacíos (371 nulos en total) y quedaron anotados en `data/quarantine/users_login_audit.csv`.

## `support_tickets`

Tickets de soporte: categoría, severidad, apertura, cierre, CSAT y si se venció el SLA. Sirve para ver carga de soporte y satisfacción de quienes ya tuvieron respuesta.

Qué se limpió:

- `created_at` y `resolved_at` pasaron a fecha.
- `category` y `severity` quedaron en minúsculas y sin espacios.
- Un CSAT menor a 1 o mayor a 5 pasó a nulo. La escala válida es de 1 a 5; los 40 casos con 0, 6 o 7 no se pueden tomar como nota.
- Si el ticket no tiene `resolved_at`, el CSAT también pasó a nulo. Un ticket abierto todavía no tiene cierre ni calificación válida.

Qué se encontró: 1.000 tickets y 240 siguen abiertos. De esos abiertos, 172 traían un CSAT que no correspondía (9 ya estaban fuera de rango y 163 tenían valores entre 1 y 5) y se anuló. 95 tienen el SLA vencido. La severidad baja es la más común (412) y la crítica la menos (56). Los CSAT nulos pasan de 254 a 457 después de las dos reglas (203 anulados), quedando 543 notas válidas en `data/clean/support_tickets_clean.csv`.

## `marketing_touches`

Cada contacto de una campaña: canal, si hubo click y si convirtió. Sirve para ver qué campañas llegan a cada cliente y cuáles terminan en conversión.

Qué se limpió:

- `timestamp` pasó de texto a fecha.
- Se agregó `mes`, con el año y el mes de ese timestamp, para agrupar sin recalcular la fecha.
- No había nulos ni ids repetidos, así que no se imputó ni se borró nada.

Qué se encontró: 1.500 toques, entre mayo y agosto de 2025, de las 80 organizaciones. Hay 6 campañas y 4 canales. Hubo 453 clicks y 144 conversiones.

## `nps_surveys`

Respuesta de la encuesta de NPS. Es distinta del NPS de cabecera que viene en `customers_orgs`: acá hay una fila por respuesta, con fecha y comentario.

Qué se limpió:

- `survey_date` pasó de texto a fecha.
- Se revisó que no hubiera dos encuestas de la misma organización en el mismo día. No las hay, así que `org_id` más `survey_date` identifica cada respuesta.
- La única fila que venía sin puntaje y sin comentario (`org_064` del `2025-07-09`) se pasó a `data/quarantine/nps_surveys_quarantine.csv`, quedando 91 filas en `data/clean/nps_surveys_clean.csv`.
- No se completaron los puntajes ni los comentarios vacíos cuando la otra columna sí tenía dato. Los puntajes extremos se marcaron con la columna `is_outlier` para mirarlos, sin borrarlos.

Qué se encontró: 92 filas en crudo (91 válidas) de 60 organizaciones. 20 clientes no contestaron. Los puntajes van de −16 a 68 y los comentarios se repiten en seis frases: faltan funciones, les gusta genAI, facturación compleja, estable pero lento, muy caro y buen soporte.

## `usage_events_stream` (120 archivos JSONL)

Flujo de eventos de consumo e incremento de costo en `data/raw/usage_events_stream/events_part_0000.jsonl` a `0119.jsonl`. El análisis completo está en `notebooks/perfilado_usage_events_jsonl.ipynb` y `docs/perfilado_usage_events.md`.

Qué se limpió y definió para el pipeline:

- Se lee con un esquema explícito que incluye las columnas de la versión 1 y las dos columnas que se sumaron en la versión 2 (`carbon_kg` y `genai_tokens`, nulas cuando no vienen).
- Los 216 eventos con `cost_usd_increment` negativo se separan a `data/quarantine/usage_events_quarantine.csv`.
- Los 2.075 eventos que traen `unit` vacío se completan según `metric`: `hours` para `cpu_hours`, `count` para `requests` y `gb_hours` para `storage_gb_hours`.
- Los 877 eventos con `value` vacío pero con costo válido se conservan con `value` nulo para no distorsionar promedios de uso.
- Los picos de costo se marcan con `is_mad_spike` calculando MAD (Modified Z-Score > 3,5) agrupado por `(service, metric)`.

Qué se encontró: 120 archivos de 360 líneas cada uno (43.200 eventos, 12,37 MB, ningún JSON roto), entre el 3 de julio y el 31 de agosto de 2025 (720 eventos por día). Todos los `event_id` son únicos y todos los `org_id` (80) y `resource_id` (400) existen en las tablas maestras. Del 3 al 17 de julio hay 10.800 eventos con `schema_version = 1`; el 18 de julio a las 00:00 UTC empieza `schema_version = 2` (32.400 eventos), donde `carbon_kg` viene siempre y `genai_tokens` viene en los 3.132 eventos del servicio `genai` (2.545.481 tokens en eventos válidos). Los 42.984 eventos válidos suman 148.351,60 USD y 365,56 kg de CO2, con 191 spikes de costo detectados por MAD (frente a 158 con Z-Score clásico). Además, los eventos vienen muy desordenados entre archivos (cada archivo trae fechas de casi los 60 días), por lo que un watermark de 1 hora descarta más del 90 % de los datos; para correr la muestra completa sin perder eventos tardíos se necesita un watermark de 60 días (`1440 hours`, o `1368 hours` para cubrir el percentil 95).
