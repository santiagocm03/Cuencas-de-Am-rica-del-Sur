# River Discharge Data Warehouse

Data warehouse en PostgreSQL para el analisis de **series de tiempo de caudales mensuales** en grandes cuencas de Sudamerica.

## Arquitectura

Pipeline con tres capas (Medallion Architecture):

| Capa | Esquema | Proposito |
|------|---------|-----------|
| **Bronze** | `bronze` | Ingesta cruda de CSVs heterogeneos (sin transformacion) |
| **Silver** | `silver` | Datos diarios limpios y armonizados. NULL indica dia sin observacion. |
| **Gold** | `gold` | Agregacion mensual, climatologia, imputacion de huecos y anomalias estandarizadas (z-scores) |

## Estaciones

| Estacion | Rio | Cuenca | Pais |
|----------|-----|--------|------|
| Calamar | Magdalena | Magdalena | Colombia |
| Ciudad Bolivar | Orinoco | Orinoco | Venezuela |
| Manaos | Negro / Amazonas | Amazonas | Brasil |
| Obidos | Amazonas | Amazonas | Brasil |
| Tabatinga | Amazonas | Amazonas | Brasil |
| Timbues | Parana | Parana | Argentina |

## Estructura del proyecto


```
caudales-dw/
├── data/
│   └── raw/                          # CSVs fuente por estacion
│       ├── Calamar.csv
│       ├── CiudadBolivar.csv
│       ├── Manaos.csv
│       ├── Obidos.csv
│       ├── Tabatinga.csv
│       └── Timbues.csv
└── sql/
    ├── 00_init_schemas.sql           # Crear esquemas bronze/silver/gold
    ├── bronze/
    │   ├── 01_ddl.sql                # Tablas de ingesta cruda
    │   └── 02_load_insert.sql        # Carga de CSVs (rutas relativas)
    ├── silver/
    │   ├── 03_ddl.sql                # Tabla de caudales mensuales limpios
    │   └── 04_insert.sql             # Limpieza, agregacion e imputacion
    ├── gold/
    │   └── 05_view_anomalies.sql     # VIEW de anomalias (z-scores)
    └── tests/
        ├── test_silver.sql           # Validaciones de integridad Silver
        └── test_gold.sql             # Tests sobre gold.flow_monthly_anomalies

 
```

## Ejecucion

Todos los scripts se ejecutan desde la raiz del repositorio con `psql`:

```bash
# 1. Crear esquemas
psql -d <base> -f sql/00_init_schemas.sql

# 2. Crear tablas bronze y cargar CSVs
psql -d <base> -f sql/bronze/01_ddl.sql
psql -d <base> -f sql/bronze/02_load_insert.sql

# 3. Verificar tipos de dato antes de transformar
psql -d <base> -f sql/bronze/03_profiling.sql

# 4. Crear tabla silver y transformar datos
psql -d <base> -f sql/silver/03_ddl.sql
psql -d <base> -f sql/silver/04_insert.sql

# 5. Crear vista gold de anomalias
psql -d <base> -f sql/gold/05_view_anomalies.sql

# 6. Ejecutar tests de calidad (opcional)
psql -d <base> -f sql/test/test_silver.sql
psql -d <base> -f sql/test/test_gold.sql


> **Nota:** `02_load_insert.sql` usa rutas relativas (`./data/raw/...`), por eso es necesario ejecutar `psql` desde la raiz del repositorio.

## Pipeline de datos

### Bronze (ingesta cruda)
Carga los CSVs tal como vienen de cada fuente. Todas las columnas se ingresan como `TEXT` sin conversion ni validacion. Hay dos estrategias segun la estructura del archivo fuente:

- **CSV estructurado** (Calamar, Obidos): columnas explicitas mapeadas directamente
- **Texto plano** (Ciudad Bolivar, Manaos, Tabatinga, Timbues): cada linea entra como `raw_line TEXT` y se parsea en Silver

### Bronze — Profiling (`03_profiling.sql`)
Antes de transformar, se verifica que cada campo parsea correctamente al tipo esperado. Por cada estacion se ejecutan tres checks:

1. **n_campos** — cuantos campos tiene cada fila (via conteo de separadores `;`)
2. **decimales** — si el campo valor tiene decimales, determina `INT` vs `DOUBLE PRECISION`
3. **cast** — confirma que fecha y valor convierten sin error, y reporta minimos y maximos

La salida de este script es la evidencia que justifica los tipos declarados en `silver/03_ddl.sql`.

### Silver (limpieza y armonizacion)
Para cada estacion aplica un patron canonico de tres pasos:

1. Parsing — extraer campos del formato crudo (split, cast, reemplazo de coma decimal)

2. Limpieza — filtros de calidad (regex numericos, rangos fisicos, outliers)

3. Almacenamiento — se guarda en silver.flow_daily con granularidad diaria (una fila por año, mes, día). Los dias sin observacion quedan como NULL.

Silver entrega el dato observado tal como existe. No se imputa ni se agrega a nivel mensual en esta capa.

### Gold (climatologia, imputacion y anomalias)
Vista gold.flow_monthly_anomalies que ejecuta cuatro operaciones en secuencia:

1. Agregacion diaria → mensual — promedio de los valores diarios por estacion y mes (Timbues se trata directamente como mensual).

2. Climatologia — calcula media y desviacion estandar por estacion y mes del anio, usando unicamente valores observados de Silver.

3. Imputacion — rellena los NULL de Silver con la climatologia mensual correspondiente. Cada fila incluye columnas *_imputado que indican si el valor es observado (FALSE) o imputado (TRUE).

Z-score — estandariza cada valor respecto a su climatologia

```
z = (valor - media_mensual) / desviacion_estandar_mensual
```

Al ser una `VIEW`, se recalcula automaticamente cuando los datos de Silver cambian.

### Tests de calidad

##Silver (test_silver.sql)
Verifica la integridad de silver.flow_daily:

1.Tabla no vacia

2. No duplicados en clave primaria

3. Meses y dias dentro de rangos

4. Caudales positivos y dentro de umbrales fisicos

5. Conteos minimos por estacion

6. Rango temporal razonable

7. Timbues siempre con day=1 (fuente mensual)

##Gold (test_gold.sql)
Valida la vista gold.flow_monthly_anomalies:

1. Vista no vacia

2. Numero de filas coincide con meses distintos en Silver

3. Media de anomalias por estacion-mes ≈ 0

4. Desviacion estandar por estacion-mes ≈ 1 (tolerancias ajustadas por longitud de serie e imputacion)

5. No hay anomalias extremas (|z| > 5)

6. Coherencia con datos observados en Silver

7. Columnas *_imputado nunca son NULL

Ambos scripts terminan con EXCEPTION ante cualquier fallo y emiten un resumen de PASS si todas las validaciones son exitosas.

## Tecnologias

- PostgreSQL (ETL y analitica en SQL puro)
- Sin dependencias externas (no Python, no dbt)

