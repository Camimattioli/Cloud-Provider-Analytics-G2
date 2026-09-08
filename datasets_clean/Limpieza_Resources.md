# Documentación del Pipeline de Limpieza: `resources.csv`

Este documento describe el pipeline de limpieza, normalización y transformación aplicado sobre el archivo `resources.csv` (400 filas originales) utilizando **Pandas**.

---

## 1. Estandarización y Recorte de Identificadores
* **Renombrado de Columnas:** Se renombraron las columnas originales al estándar `PascalCase` (`ResourceId`, `OrgId`, `Service`, `Region`, `CreatedDate`, `State`, `Tags`) para mayor consistencia de lectura.
* **Limpieza de Prefijos:** Las columnas `ResourceId` y `OrgId` venían antecedidas por prefijos fijos redundantes (`res_` y `org_`). Se eliminaron los primeros 4 caracteres (`.str[4:]`), dejando únicamente el identificador alfanumérico limpio.

---

## 2. Tipado de Datos y Optimización de Memoria
* **Conversión Temporal (`CreatedDate`):** 
  * Originalmente leída como `object` (string).
  * Se transformó a tipo nativo `datetime64[ns]` mediante `pd.to_datetime()` con `errors='coerce'`. Esto habilita operaciones cronológicas directas, ordenamientos correctos y extracción de métricas temporales (mínimos y máximos por cliente).
* **Conversión a Tipo Categórico (`category`):**
  * Se detectó baja cardinalidad en tres variables de texto:
    * `Service`: 6 valores únicos (`compute`, `database`, `storage`, `networking`, `analytics`, `genai`).
    * `Region`: 7 valores únicos (`sa-east`, `ap-south`, `eu-central`, `us-west`, etc.).
    * `State`: 3 estados únicos (`running`, `stopped`, `terminated`).
  * Al mutar de `object` a `category`, Pandas indexa internamente las palabras como enteros, reduciendo el consumo de memoria en más del 90% para esas columnas.

---

## 3. Análisis de Duplicados y Cardinalidad
* **`ResourceId`:** 0 duplicados (garantiza unicidad a nivel de fila y clave primaria de recurso).
* **`OrgId`:** 320 repeticiones sobre 400 registros, confirmando que existen 80 organizaciones únicas con una relación **1 a N** (múltiples recursos por organización).

---

## 4. Desanidado y Normalización de la Columna `Tags`
La columna original `Tags` contenía texto JSON serializado (`object`) con 83 registros nulos (`NaN`) y estructuras anidadas del tipo `'["clave:valor", ...]'`.

1. **Tratamiento de Nulos:** Se completaron los nulos con `'[]'` mediante `.fillna('[]')` para evitar fallos de deserialización.
2. **Conversión a Listas Nativas:** Se aplicó `json.loads` para convertir cada celda de texto a una lista Python real.
3. **Parseo Clave-Valor Dinámico:** A través de una función split (`:`), cada elemento de la lista se separó en un diccionario Python (`{clave: valor}`).
4. **Expansión a Columnas:** Mediante `.apply(pd.Series)` y `pd.concat()`, las claves de los diccionarios se transformaron automáticamente en 5 columnas tabulares independientes:
   * `env`
   * `pii`
   * `costcenter`
   * `team`
   * `backup`

---

## 5. Imputación de Nulos Post-Expansión y Limpieza Final
Dado que no todos los recursos poseen todas las etiquetas, se imputaron los valores faltantes según la semántica de cada variable:

* **Columnas Booleanas (`pii`, `backup`):**
  * Se imputaron los `NaN` con `False`.
  * Se homologaron los valores existentes (`'true'` -> `True`, `'daily'` -> `True`).
* **Columnas Categóricas/Texto (`env`, `costcenter`, `team`):**
  * `env`: Se imputaron los faltantes como `'Sin Especificar'`.
  * `costcenter`: Se imputaron los faltantes como `'Sin Asignar'`.
  * `team`: Se imputaron los faltantes como `'Sin Asignar'`.
* **Eliminación de Redundancia:** Se ejecutó `df.drop(columns=['Tags'])` para remover la lista original y evitar duplicación de información en memoria.

---

## Estructura Final del Dataset
El DataFrame final consta de **400 filas y 11 columnas**, libre de valores nulos y con tipos de datos nativos listos para consultas y modelado:

| Columna | Tipo de Dato | Ejemplo / Valores |
| :--- | :--- | :--- |
| `ResourceId` | `object` | `eubfn9kr` |
| `OrgId` | `object` | `pnsm43d8` |
| `Service` | `category` | `compute`, `database`, `storage` |
| `Region` | `category` | `sa-east`, `us-west`, etc. |
| `CreatedDate` | `datetime64[ns]` | `2025-08-14` |
| `State` | `category` | `running`, `stopped`, `terminated` |
| `env` | `object` | `prod`, `dev`, `Sin Especificar` |
| `pii` | `bool` | `True`, `False` |
| `costcenter` | `object` | `alpha`, `beta`, `Sin Asignar` |
| `team` | `object` | `core`, `ml`, `Sin Asignar` |
| `backup` | `bool` | `True`, `False` |