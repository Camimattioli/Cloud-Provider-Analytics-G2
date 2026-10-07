# Cloud Provider Analytics G2

Análisis de un proveedor cloud a partir de datos de clientes, facturación, recursos, usuarios, soporte, marketing y NPS. El trabajo de limpieza está en notebooks y los CSV resultantes quedan versionados para poder analizarlos sin volver a correr todo el pipeline.

## Arquitectura

| Carpeta / archivo | Propósito |
| --- | --- |
| `README.md` | Objetivo, arquitectura, requisitos, ejecución, pruebas y limitaciones. |
| `DECISIONS.md` | Decisiones de limpieza y de estructura, con la alternativa que se descartó. |
| `docs/` | Documentación de pipelines y material de defensa. Hoy: limpieza de recursos. |
| `data/raw/` | Datos de muestra tal como se recibieron. No se editan a mano. |
| `data/clean/` | Salida de los notebooks de limpieza. |
| `notebooks/` | Exploración y limpieza. Todavía no hay código productivo equivalente. |
| `src/` | Reservado para ingesta, procesamiento, ML o serving cuando salga de los notebooks. |
| `tests/` | Reservado para pruebas de transformaciones y calidad. |
| `config/` | Reservado para configuración externalizada. Sin credenciales. |
| `infra/` | Reservado para Docker, scripts o manifiestos de ejecución. |
| `evidence/` | Reservado para logs, capturas y resultados de las entregas. |

La explicación de los siete CSV, la limpieza y lo que se encontró está en [data/README.md](data/README.md).

## Requisitos

- Python 3
- `pandas` y `jupyter`
- `numpy`, usado en la limpieza de tickets

```bash
python -m pip install pandas jupyter numpy
```

## Ejecución

Los notebooks leen y escriben rutas relativas a la raíz del repositorio (`data/raw/...`, `data/clean/...`). Hay que abrirlos desde esa raíz, no desde `notebooks/`.

```bash
cd Cloud-Provider-Analytics-G2
jupyter lab
```

| Notebook | Entrada | Salida |
| --- | --- | --- |
| `notebooks/limpieza_billing_monthly.ipynb` | `data/raw/billing_monthly.csv` | `data/clean/billing_monthly_limpio.csv` |
| `notebooks/limpieza_customers_orgs.ipynb` | `data/raw/customers_orgs.csv` | `data/clean/customers_orgs_limpio.csv` |
| `notebooks/LimpiezaResources.ipynb` | `data/raw/resources.csv` | `data/clean/resources_limpio.csv` |
| `notebooks/limpieza_support_ticketsCSV_TP.ipynb` | `data/raw/support_tickets.csv` | `data/clean/support_tickets_clean.csv` |
| `notebooks/user_limpieza.ipynb` | `data/raw/users.csv` | `data/clean/users_limpio.csv` |
| `notebooks/Análisis_Marketing_touches_csv_y_nps_surveys_csv.ipynb` | `data/raw/marketing_touches.csv`, `data/raw/nps_surveys.csv` | `data/clean/nps_surveys_clean.csv` |

`data/clean/marketing_touches_clean.csv` ya está en el repositorio. El notebook de marketing y NPS hoy solo vuelve a escribir la encuesta limpia.

## Pruebas

No hay suite automática en `tests/`. La revisión de nulos, duplicados, tipos y rangos está dentro de cada notebook. Cuando una transformación pase a `src/`, la prueba correspondiente va en `tests/`.

## Limitaciones

- La limpieza no está extraída a módulos reutilizables.
- Algunos notebooks todavía incluyen celdas de Colab para clonar el repo o descargar el CSV. No guardan tokens: el token se pide con `getpass` y no debe pegarse en el archivo.
- Los CSV son datos de muestra del trabajo. No agregues credenciales, tokens ni archivos de entorno.
