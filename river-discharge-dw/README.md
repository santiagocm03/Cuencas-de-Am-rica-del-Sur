# River Discharge Data Warehouse

Data warehouse en PostgreSQL para el análisis de **series de tiempo de caudales mensuales** en grandes cuencas de Sudamérica.

## Arquitectura

Pipeline con tres capas (Medallion Architecture):

| Capa | Esquema | Propósito |
|------|---------|-----------|
| **Bronze** | `bronze` | Ingesta cruda de CSVs heterogéneos (sin transformación). Cada tabla refleja la estructura original de la fuente (columnas `TEXT` o `raw_line`). |
| **Silver** | `silver` | Datos limpios y tipados por estación. Se aplican reglas de calidad, conversión de tipos y filtros básicos. Cada estación tiene su propia tabla. |
| **Gold** | `gold` | Agregación mensual (promedio), climatología, imputación de huecos, anomalías estandarizadas (z‑scores) y modelo dimensional (estrella). |

## Estaciones

| Estación | Río | Cuenca | País |
|----------|-----|--------|------|
| Calamar | Magdalena | Magdalena | Colombia |
| Ciudad Bolívar | Orinoco | Orinoco | Venezuela |
| Manaos | Negro / Amazonas | Amazonas | Brasil |
| Óbidos | Amazonas | Amazonas | Brasil |
| Tabatinga | Amazonas | Amazonas | Brasil |
| Timbúes | Paraná | Paraná | Argentina |

## Estructura del proyecto

```
caudales-dw/
├── data/
│ └── raw/ # CSVs fuente por estación
│ ├── Calamar.csv
│ ├── CiudadBolivar.csv
│ ├── Manaos.csv
│ ├── Obidos.csv
│ ├── Tabatinga.csv
│ └── Timbues.csv
└── sql/
├── 00_init_schemas.sql # Crear esquemas bronze/silver/gold
├── bronze/
│ ├── 01_ddl.sql # Tablas de ingesta cruda
│ ├── 02_load_insert.sql # Carga de CSVs (rutas relativas)
│ └── 03_profiling.sql # (Opcional) Perfilado de tipos
├── silver/
│ ├── 03_ddl.sql # Tablas silver con tipos correctos
│ └── 04_insert.sql # Transformación y limpieza
├── gold/
│ ├── 05_view_anomalies.sql # Tabla mensual y vista de anomalías
│ └── 06_dim_fact.sql # Modelo dimensional (estrella)
└── tests/
├── test_silver.sql # Validaciones de integridad Silver
├── test_gold.sql # Tests sobre gold.flow_monthly_anomalies
└── tiempos.sql # Medición de tiempos de ejecución
 
```

## Pipeline de datos

### Bronze – Ingesta cruda
Los CSVs se cargan sin transformación.  
- **CSV estructurado** (Calamar, Obidos): se mapean columnas explícitas con tipo `TEXT`.  
- **Texto plano** (Ciudad Bolivar, Manaos, Tabatinga, Timbues): cada línea se guarda como `raw_line TEXT`.  

El script `03_profiling.sql` (opcional) analiza la estructura de los datos para verificar conversiones antes de pasar a silver.

### Silver – Limpieza y armonización
Se transforma cada tabla bronze en su correspondiente tabla silver aplicando:

1. **Parsing** – extracción de campos, conversión de formatos (fechas, decimales con coma).  
2. **Limpieza** – filtros de calidad: valores positivos, rangos físicos, eliminación de registros inválidos.  
3. **Tipado** – conversión a tipos adecuados (DOUBLE PRECISION, TIMESTAMP, etc.).  

Cada estación conserva su propia tabla. No se imputan huecos en esta capa.

### Gold – Agregación mensual y anomalías
1. **Agregación** – los caudales diarios de silver se promedian por mes y se consolidan en la tabla `gold.flow_monthly` (formato ancho).  
2. **Climatología** – sobre la tabla mensual se calcula la media y desviación estándar por estación y mes del año, usando solo valores observados.  
3. **Imputación** – los meses sin dato se completan con la climatología correspondiente; se registra en columnas `*_imputado` si el valor fue imputado.  
4. **Z‑score** – se calcula `(valor - media_mensual) / desv_estándar_mensual`.  

El resultado final es la vista `gold.flow_monthly_anomalies`, que se actualiza automáticamente al cambiar los datos de silver.

### Gold – Modelo dimensional (estrella)
El script `06_dim_fact.sql` construye un modelo estrella sobre los datos mensuales y sus anomalías:

- **`gold.dim_estacion`** – dimensión de estaciones con atributos como río, cuenca, país, coordenadas.
- **`gold.dim_tiempo`** – dimensión de tiempo con granularidad mensual (año, mes, nombre_mes, trimestre, semestre).
- **`gold.fact_caudal_mensual`** – tabla de hechos que relaciona las dimensiones y contiene:
  - `caudal_observado`: valor original (si existe)
  - `es_imputado`: indicador de imputación
  - `z_score`: anomalía estandarizada

Este modelo facilita el análisis multidimensional (cubos OLAP, BI) y cumple con los requisitos de diseño dimensional solicitados.

## Checks de calidad (reglas de integridad)

El pipeline aplica múltiples reglas de calidad en cada etapa para garantizar la confiabilidad de los datos:

### Durante la transformación Silver (`04_insert.sql`)
- **Valores positivos**: Se descartan caudales ≤ 0 (físicamente imposibles).
- **Rangos físicos máximos**: Se establecen umbrales superiores basados en registros históricos conocidos:
  - Calamar (Magdalena): ≤ 16.000 m³/s
  - Ciudad Bolívar (Orinoco): ≤ 100.000 m³/s
  - Manaos (Amazonas): ≤ 200.000 m³/s
  - Óbidos (Amazonas): ≤ 300.000 m³/s
  - Tabatinga (Amazonas): ≤ 80.000 m³/s
  - Timbúes (Paraná): ≤ 30.000 m³/s
- **Fechas válidas**: Solo se aceptan registros con fechas parseables y dentro del período histórico esperado (1900–2030). Para Calamar se excluyen datos posteriores a 2016 debido a un cambio de sensor.
- **Filtro por estación/serie**: En Calamar se selecciona exclusivamente la serie “Caudal medio diario” de la estación principal, descartando sufijos entre corchetes.

### En las tablas Silver (implícito en la lógica de inserción)
- **No se permiten duplicados**: El diseño de las tablas silver no tiene restricción de unicidad, pero la transformación asegura que cada combinación de fecha y estación aparezca una sola vez (agregación implícita).  
- **Conversión de formatos**: Se reemplaza la coma decimal por punto en todas las fuentes que usan coma como separador decimal.

### En la capa Gold (construcción de `gold.flow_monthly` y la vista de anomalías)
- **Agregación mensual**: Se promedian los caudales diarios para obtener un único valor por mes y estación.
- **Climatología**: Calculada únicamente con valores observados, evitando contaminación con datos imputados.
- **Imputación**: Los meses sin dato se completan con la media climatológica correspondiente, y se marca con la bandera `*_imputado`.
- **Z-score**: Se estandariza cada valor respecto a su propia climatología mensual; se verifica que la media de los z‑scores sea ≈ 0 y la desviación estándar ≈ 1 dentro de tolerancias adaptadas a cada serie.

## Benchmark de tiempos

El script `sql/tests/tiempos.sql` mide los tiempos de ejecución de las etapas principales del pipeline (carga bronze, transformación silver, construcción gold). Para ejecutarlo:

```bash
psql -d caudales -f sql/tests/tiempos.sql 2>&1 | tee tiempos.log

## Ejecución

Todos los scripts se ejecutan desde la raíz del repositorio con `psql`. Ejemplo:

```bash
# 1. Crear esquemas
psql -d caudales -f sql/00_init_schemas.sql

# 2. Crear tablas bronze y cargar CSVs
psql -d caudales -f sql/bronze/01_ddl.sql
psql -d caudales -f sql/bronze/02_load_insert.sql

# 3. (Opcional) Perfilar tipos de dato
psql -d caudales -f sql/bronze/03_profiling.sql

# 4. Crear tablas silver y transformar datos
psql -d caudales -f sql/silver/03_ddl.sql
psql -d caudales -f sql/silver/04_insert.sql

# 5. Crear tabla mensual y vista de anomalías en gold
psql -d caudales -f sql/gold/05_view_anomalies.sql

# 6. (Opcional) Construir modelo dimensional (estrella)
psql -d caudales -f sql/gold/06_dim_fact.sql

# 7. Ejecutar tests (opcional)
psql -d caudales -f sql/tests/test_silver.sql
psql -d caudales -f sql/tests/test_gold.sql

# 8. (Opcional) Medir tiempos de ejecución
psql -d caudales -f sql/tests/tiempos.sql

> **Nota:** `02_load_insert.sql` usa rutas relativas (`./data/raw/...`), por eso es necesario ejecutar `psql` desde la raiz del repositorio.

## Tecnologias

- PostgreSQL (ETL y analitica en SQL puro)
- Sin dependencias externas (no Python, no dbt)


**DESCRIPCIÓN DE LOS SCRIPTS**

**00_init_schemas.sql**
Crea los esquemas del data warehouse: bronze (datos crudos), silver (datos limpios) y gold (productos analíticos).

**BRONZE**

**01_ddl.sql (bronze)**
Define las tablas de ingesta cruda en el esquema bronze.
- Para Calamar y Obidos (CSV estructurado): se crean columnas explícitas de tipo TEXT.
- Para Ciudad Bolívar, Manaos, Tabatinga y Timbúes (CSV semiestructurado): se crea una única columna raw_line TEXT para almacenar cada línea tal cual.

**02_load_insert.sql (bronze)**
Carga los archivos CSV desde data/raw/ hacia las tablas bronze. Trunca cada tabla antes de insertar para garantizar idempotencia. Utiliza \copy con diferentes formatos según la fuente (CSV HEADER o FORMAT text).

**03_profiling.sql (bronze)**
Script opcional de perfilado. Analiza cada tabla bronze para determinar:
- Número de campos por línea (para las tablas raw_line).
- Presencia de decimales en la columna valor (sugiere INT o DOUBLE PRECISION).
- Verifica que fecha y valor puedan convertirse correctamente (cast exitoso) y reporta mínimos y máximos.
La salida en consola justifica la elección de tipos en silver.

**SILVER**

**03_ddl.sql (silver)**
Crea tablas limpias en el esquema silver, una por estación, con tipos de datos adecuados:
- DOUBLE PRECISION para coordenadas, caudales y otros valores numéricos.
- TIMESTAMP para fechas con hora.
- DATE para fechas sin hora (Timbúes).
- TEXT para campos de texto.
Cada tabla refleja las columnas relevantes del CSV original después de limpieza.

**04_insert.sql (silver)**
Transforma los datos de bronze a silver:
- Para tablas con raw_line: parsea los campos separados por ';', convierte fechas con to_timestamp, reemplaza coma decimal por punto.
- Para tablas estructuradas: castea directamente.
- Aplica filtros de calidad: valores positivos, rangos físicos máximos (ej: Calamar <= 16000, Bolivar <= 100000, etc.), fechas válidas.
- Inserta solo registros que cumplen las condiciones.
- Para Calamar, adicionalmente filtra por nombre de estación "CALAMAR" sin sufijos, serie "Caudal medio diario" y fecha < 2017-01-01.

**GOLD**

**05_view_anomalies.sql (gold)**
Construye la capa gold en dos pasos:
1. Crea la tabla gold.flow_monthly que agrega caudales diarios de silver a nivel mensual (promedio) en formato ancho: columnas year, month, calamar_monthly, bolivar_monthly, manaos_monthly, obidos_monthly, tabatinga_monthly, timbues_monthly.
2. Crea la vista gold.flow_monthly_anomalies que sobre la tabla anterior:
   - Calcula climatología (media y desv. estándar) por estación y mes del año usando solo datos observados.
   - Imputa NULLs con la climatología mensual correspondiente y agrega banderas *_imputado.
   - Calcula el z-score = (valor - media) / desv_estándar para cada estación.
   - Presenta el resultado en formato ancho con columnas de anomalía y banderas de imputación.
   - Nota: incluye DROP TABLE ... CASCADE para eliminar la vista dependiente automáticamente.

**TESTS**

**test_silver.sql**
Valida las tablas silver por estación:
- Verifica que ninguna tabla esté vacía.
- Asegura que todos los valores de caudal sean positivos.
- Controla rangos físicos máximos (Calamar <=16000, Bolivar <=100000, Manaos <=200000, Obidos <=300000, Tabatinga <=80000, Timbues <=30000).
- Confirma que las fechas estén entre 1900 y 2030.
- Comprueba que en silver.timbues todos los registros tengan día = 1 (dato mensual).

**test_gold.sql**
Valida la tabla gold.flow_monthly y la vista gold.flow_monthly_anomalies:
- Verifica que ambos objetos contengan datos.
- Compara el número de meses en gold.flow_monthly con los meses distintos presentes en las tablas silver (unión de todas).
- Comprueba que la media de anomalías por estación y mes sea aproximadamente 0 (tolerancia 1e-6).
- Comprueba que la desviación estándar de anomalías por estación y mes sea aproximadamente 1 con tolerancias adaptadas a cada serie.
- Asegura que no existan anomalías extremas (|z| > 5).
- Verifica que las columnas *_imputado nunca sean NULL.
- Confirma que los valores imputados tengan anomalía = 0 (por construcción, ya que se imputa con la media climatológica).

Todos los scripts de test detienen la ejecución con EXCEPTION si falla alguna validación, y al final emiten NOTICE de PASS.

### Checks de calidad (reglas de integridad)

El pipeline aplica múltiples reglas de calidad en cada etapa para garantizar la confiabilidad de los datos:

#### Durante la transformación Silver (`04_insert.sql`)
- **Valores positivos**: Se descartan caudales ≤ 0 (físicamente imposibles).
- **Rangos físicos máximos**: Se establecen umbrales superiores basados en registros históricos conocidos:
  - Calamar (Magdalena): ≤ 16.000 m³/s
  - Ciudad Bolívar (Orinoco): ≤ 100.000 m³/s
  - Manaos (Amazonas): ≤ 200.000 m³/s
  - Óbidos (Amazonas): ≤ 300.000 m³/s
  - Tabatinga (Amazonas): ≤ 80.000 m³/s
  - Timbúes (Paraná): ≤ 30.000 m³/s
- **Fechas válidas**: Solo se aceptan registros con fechas parseables y dentro del período histórico esperado (1900–2030). Para Calamar se excluyen datos posteriores a 2016 debido a un cambio de sensor.
- **Filtro por estación/serie**: En Calamar se selecciona exclusivamente la serie “Caudal medio diario” de la estación principal, descartando sufijos entre corchetes.

#### En las tablas Silver (implícito en la lógica de inserción)
- **No se permiten duplicados**: El diseño de las tablas silver no tiene restricción de unicidad, pero la transformación asegura que cada combinación de fecha y estación aparezca una sola vez (agregación implícita).  
- **Conversión de formatos**: Se reemplaza la coma decimal por punto en todas las fuentes que usan coma como separador decimal.

#### En la capa Gold (construcción de `gold.flow_monthly` y la vista de anomalías)
- **Agregación mensual**: Se promedian los caudales diarios para obtener un único valor por mes y estación.
- **Climatología**: Calculada únicamente con valores observados, evitando contaminación con datos imputados.
- **Imputación**: Los meses sin dato se completan con la media climatológica correspondiente, y se marca con la bandera `*_imputado`.
- **Z-score**: Se estandariza cada valor respecto a su propia climatología mensual; se verifica que la media de los z‑scores sea ≈ 0 y la desviación estándar ≈ 1 dentro de tolerancias adaptadas a cada serie.

Estas reglas están implementadas en los scripts de transformación y son validadas automáticamente por los tests en `sql/tests/test_silver.sql` y `sql/tests/test_gold.sql`.