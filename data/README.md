# CSV

Hay siete tablas de las mismas 80 organizaciones. Se cruzan por `org_id`. `raw/` es el archivo original y `clean/` es la salida del notebook. No se borraron filas: un dato raro se corrige, se marca o se deja documentado.

La encuesta NPS es la excepción de cobertura. Tiene respuestas de 60 organizaciones, no de las 80.

La limpieza sigue siempre la misma idea.

1. Las fechas que venían como texto pasan a fecha, para poder ordenar y restar períodos.
2. Un vacío se completa solo cuando significa "no hubo" o "no aplica". Si puede ser un dato que falta de verdad, se deja nulo.
3. Un valor imposible pasa a nulo. Un valor posible pero incómodo, como una factura negativa, se conserva.
4. El texto de categorías se normaliza (minúsculas, sin espacios) cuando hacía falta para que el mismo valor no aparezca escrito de dos formas.
5. Los montos en otra moneda se duplican en USD. La moneda original no se pisa.

## `customers_orgs`

Ficha del cliente: industria, plan, región, etapa comercial y un NPS de cabecera. Sirve de tabla madre: el resto de los CSV cuelga de su `org_id`.

Qué se limpió:

- `signup_date` pasó de texto a fecha.
- El NPS se revisó contra la escala válida, de −100 a 100. El único valor imposible, un 101, pasó a nulo.
- Los NPS que ya venían vacíos se dejaron vacíos. No se rellenaron con un promedio.

Qué se encontró: 80 clientes. El plan más común es standard (41), después pro (21), enterprise (10) y free (8). Hay 54 activos, 11 en riesgo, 6 dados de baja, 6 prospectos y 3 leads. El NPS de cabecera venía vacío en 11 fichas; con el 101 anulado, el limpio queda entre −38 y 81.

## `billing_monthly`

Factura mensual de cada organización: subtotal, créditos, impuestos y moneda. Permite ver cuánto paga cada cliente y comparar meses.

Qué se limpió:

- 137 créditos vacíos se completaron con 0. Vacío acá quiere decir que esa factura no tuvo crédito, no que el importe se haya perdido. Usar la media habría inventado descuentos.
- `month` pasó de texto a fecha.
- Se agregaron `subtotal_usd`, `taxes_usd` y `credits_usd`, multiplicando cada monto por `exchange_rate_to_usd`. Así se pueden sumar facturas en USD, ARS y EUR.
- La moneda original y el tipo de cambio se conservan.
- Los subtotales negativos no se corrigieron ni se borraron.

Qué se encontró: 240 facturas, tres meses por organización, en USD (160), ARS (51) y EUR (29). Trece subtotales son negativos. Se leen como ajustes o notas de crédito, que el propio dataset dice incluir, y no como filas rotas.

## `resources`

Recursos cloud de cada cliente: servicio, región, estado y etiquetas. Sirve para ver qué tiene contratado cada organización.

Qué se limpió:

- A `resource_id` y `org_id` se les sacó el prefijo fijo `res_` y `org_`. El identificador queda solo con la parte que cambia.
- `created_at` pasó a fecha, con el nombre `CreatedDate`.
- `Service`, `Region` y `State` pasaron a categóricas: tienen pocos valores distintos y se repiten en las 400 filas.
- `tags_json` venía como texto, a veces vacío y a veces con una lista del estilo `env:prod`. Los vacíos se reemplazaron por una lista vacía para poder leer el resto. Cada par clave-valor se abrió en una columna: `env`, `pii`, `costcenter`, `team` y `backup`. La columna original de tags se eliminó.
- Lo que seguía vacío después de abrir los tags se imputó con un valor explícito: `Sin Especificar` en entorno, `Sin Asignar` en centro de costo y equipo, y `False` en `pii` y `backup`. `backup: daily` se leyó como verdadero.

Qué se encontró: 400 recursos y ninguno sin identificador. Compute es el servicio más frecuente (116). Hay 242 en ejecución, 119 detenidos y 39 dados de baja. 83 recursos llegaron sin tags: por eso, en el limpio, la mayoría queda sin entorno, centro de costo ni equipo asignado. El archivo limpio no tiene nulos.

## `users`

Personas de cada organización: mail, rol, si está activa, fecha de alta y último login. Sirve para ver adopción y cuentas inactivas.

Qué se limpió:

- `created_at` y `last_login` pasaron a fecha.
- El mail quedó en minúsculas y sin espacios. El rol quedó sin espacios.
- Se buscaron `user_id` y mails repetidos. No había.
- Si `last_login` era anterior a `created_at`, el login se anuló. La fecha de alta se dejó como estaba: el dato incoherente es el login, no el alta.

Qué se encontró: 800 usuarios, todos con id y mail distintos, y 728 activos. Los roles van de data engineer (201) a admin (89). 139 no tenían último login. Otros 232 lo tenían antes de la fecha de alta; en el limpio esos login quedaron vacíos.

## `support_tickets`

Tickets de soporte: categoría, severidad, apertura, cierre, CSAT y si se venció el SLA. Sirve para ver carga de soporte y satisfacción de quienes ya tuvieron una respuesta.

Qué se limpió:

- `created_at` y `resolved_at` pasaron a fecha.
- `category` y `severity` quedaron en minúsculas y sin espacios, para que una misma categoría no se parta por cómo estaba escrita.
- Un CSAT menor a 1 o mayor a 5 pasó a nulo. La escala válida de esta encuesta es de 1 a 5; 0, 6 y 7 no se pueden leer como nota.
- Si el ticket no tiene `resolved_at`, el CSAT también pasó a nulo. Un ticket abierto todavía no tiene cierre ni una calificación válida.

Qué se encontró: 1.000 tickets y 240 siguen abiertos. De esos abiertos, 172 traían un CSAT que no correspondía y se anuló. 95 tienen el SLA vencido. Las categorías están bastante parejas: integración, facturación, disponibilidad, usabilidad, performance y seguridad. La severidad baja es la más común (412) y la crítica la menos (56). Los CSAT nulos pasan de 254 a 457 después de las dos reglas.

## `marketing_touches`

Cada contacto de una campaña: canal, si hubo click y si convirtió. Sirve para ver qué campañas llegan a cada cliente y cuáles terminan en conversión.

Qué se limpió:

- `timestamp` pasó de texto a fecha.
- Se agregó `mes`, con el año y el mes de ese timestamp, para poder agrupar sin recalcular la fecha.
- No había nulos ni ids repetidos, así que no se imputó ni se borró nada. Click y conversión se dejaron como venían.

Qué se encontró: 1.500 toques, entre mayo y agosto de 2025, de las 80 organizaciones. Hay 6 campañas y 4 canales: evento, email, ads e in-app. Hubo 453 clicks y 144 conversiones.

## `nps_surveys`

Respuesta de la encuesta de NPS. Es distinta del NPS de cabecera que viene en `customers_orgs`: acá hay una fila por respuesta, con fecha y comentario. Sirve para ver cómo evoluciona la satisfacción y qué frase repite cada cliente.

Qué se limpió:

- `survey_date` pasó de texto a fecha.
- Se revisó que no hubiera dos encuestas de la misma organización en el mismo día. No las hay, así que `org_id` más `survey_date` identifica cada respuesta.
- No se completaron los puntajes ni los comentarios vacíos, y no se borró la fila que no tiene ninguno de los dos. Un NPS inventado o un comentario inventado cambiaría el resultado.
- Los puntajes extremos se anotaron como atípicos para mirarlos, pero se conservaron: entran en la escala de −100 a 100.

Qué se encontró: 92 respuestas de 60 organizaciones. 20 clientes no contestaron. 19 respuestas no tienen puntaje, 10 no tienen comentario y una no tiene ninguna de las dos. Los puntajes van de −16 a 68. Los comentarios caen en seis frases: faltan funciones, les gusta genAI, facturación compleja, estable pero lento, muy caro y buen soporte.
