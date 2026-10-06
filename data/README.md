# Datos

Los CSV de esta carpeta son datos de muestra del trabajo. No agregues credenciales, exports privados ni tokens.

`raw/` es la entrada original. No se edita a mano.

`clean/` es la salida de los notebooks. Si cambiás una regla de limpieza, volvé a correr el notebook desde la raíz del repositorio y reemplazá el archivo correspondiente.

| Archivo crudo | Filas | Limpio | Qué cambia |
| --- | ---: | --- | --- |
| `raw/billing_monthly.csv` | 240 | `clean/billing_monthly_limpio.csv` | Fecha, créditos nulos en 0 y montos en USD. |
| `raw/customers_orgs.csv` | 80 | `clean/customers_orgs_limpio.csv` | Fecha de alta y NPS fuera de rango anulado. |
| `raw/resources.csv` | 400 | `clean/resources_limpio.csv` | Ids, tipos y tags expandidos. Ver `docs/limpieza_resources.md`. |
| `raw/support_tickets.csv` | 1000 | `clean/support_tickets_clean.csv` | Fechas, texto normalizado y CSAT inválido en nulo. |
| `raw/users.csv` | 800 | `clean/users_limpio.csv` | Fechas y coherencia entre alta y último login. |
| `raw/marketing_touches.csv` | 1500 | `clean/marketing_touches_clean.csv` | Incluye la columna `mes`. |
| `raw/nps_surveys.csv` | 92 | `clean/nps_surveys_clean.csv` | Encuesta limpia para el análisis de NPS. |
