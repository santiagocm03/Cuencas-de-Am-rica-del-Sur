-- ==========================================================
-- 04_insert.sql  (Silver)
-- ==========================================================
-- Proposito : Transformar datos crudos de bronze a caudales
--             mensuales limpios en silver.flow_monthly.
--
--             Cada estacion sigue el mismo patron canonico:
--               1. parsing   — extraer campos del dato crudo
--               2. limpieza  — filtrar registros invalidos
--               3. mensual   — agregar diario a mensual (AVG)
--
--           
--
--             Los meses sin observacion quedan como NULL en
--             silver.flow_monthly. Gold decide que hacer con
--             esos huecos.
--
-- Dependencias: 03_ddl.sql (silver), 02_load_insert.sql (bronze)
-- Ejecucion :
--   sudo -u postgres psql -d caudales -f sql/silver/04_insert.sql
-- ==========================================================

\set ON_ERROR_STOP on

\echo '========================================'
\echo 'INICIO TRANSFORMACION SILVER'
\echo '========================================'


-- ==========================================================
-- CALAMAR  (Rio Magdalena, Colombia)
-- ==========================================================
-- Fuente: bronze.calamar (21 columnas TEXT)
-- Parsing:  fecha::date, valor::double precision
-- Limpieza:
--   - Solo estacion 'CALAMAR' (sin sufijos entre corchetes)
--   - Solo serie 'Caudal medio diario'
--   - Datos hasta 2016-12 (cambio de sensor desde 2017)
--   - Valor numerico positivo <= 16000 m3/s (umbral fisico)
-- Agregacion: AVG diario -> mensual
-- ==========================================================
\echo 'Procesando SILVER - Calamar...'

INSERT INTO silver.flow_monthly (year, month, calamar_monthly)
WITH parsing AS (
    SELECT
        fecha::date                      AS fecha,
        nombre_estacion,
        descripcion_serie,
        valor
    FROM bronze.calamar
    WHERE fecha  IS NOT NULL
      AND valor  IS NOT NULL
),
limpieza AS (
    SELECT
        fecha,
        valor::double precision AS valor_num
    FROM parsing
    WHERE
        regexp_replace(nombre_estacion, '\s*\[.*\]', '') = 'CALAMAR'
        AND descripcion_serie = 'Caudal medio diario'
        AND fecha < DATE '2017-01-01'
        AND valor ~ '^[0-9]+(\.[0-9]+)?$'
        AND valor::double precision > 0
        AND valor::double precision <= 16000
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha)::int AS year,
        EXTRACT(MONTH FROM fecha)::int AS month,
        AVG(valor_num)                 AS calamar_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, calamar_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET calamar_monthly = EXCLUDED.calamar_monthly;

\echo 'OK - Calamar'


-- ==========================================================
-- CIUDAD BOLIVAR  (Rio Orinoco, Venezuela)
-- ==========================================================
-- Fuente: bronze.ciudad_bolivar (raw_line TEXT)
-- Estructura del raw_line (separador ';'):
--   campo 1: id_estacion
--   campo 2: nombre
--   campo 3: fecha (DD/MM/YYYY HH24:MI)
--   campo 4: valor (coma como separador decimal)
--   campo 5: origen
-- Limpieza:
--   - Solo lineas que comienzan con digito (excluir headers)
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Agregacion: AVG diario -> mensual
-- ==========================================================
\echo 'Procesando SILVER - Ciudad Bolivar...'

INSERT INTO silver.flow_monthly (year, month, bolivar_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        )                                            AS fecha_ts,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 4), ''),
            ',', '.'
        )::double precision                          AS valor_num
    FROM bronze.ciudad_bolivar
    WHERE raw_line ~ '^[0-9]'
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
      AND valor_num > 0
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS bolivar_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, bolivar_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET bolivar_monthly = EXCLUDED.bolivar_monthly;

\echo 'OK - Ciudad Bolivar'


-- ==========================================================
-- MANAOS  (Rio Negro / Amazonas, Brasil)
-- ==========================================================
-- Fuente: bronze.manaos (raw_line TEXT)
-- Estructura del raw_line: identica a Ciudad Bolivar.
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Agregacion: AVG diario -> mensual
-- Nota: los meses sin observacion quedan como NULL en Silver.
--       La decision de imputar pertenece a Gold.
-- ==========================================================
\echo 'Procesando SILVER - Manaos...'

INSERT INTO silver.flow_monthly (year, month, manaos_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        )                                            AS fecha_ts,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 4), ''),
            ',', '.'
        )::double precision                          AS valor_num
    FROM bronze.manaos
    WHERE raw_line ~ '^[0-9]'
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
      AND valor_num > 0
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS manaos_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, manaos_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET manaos_monthly = EXCLUDED.manaos_monthly;

\echo 'OK - Manaos'


-- ==========================================================
-- OBIDOS  (Rio Amazonas, Brasil)
-- ==========================================================
-- Fuente: bronze.obidos (5 columnas TEXT)
-- Columnas: id_estacion, nombre, fecha, valor, origen
-- Parsing:  fecha via to_timestamp, valor reemplazando coma
-- Limpieza:
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- Agregacion: AVG diario -> mensual
-- ==========================================================
\echo 'Procesando SILVER - Obidos...'

INSERT INTO silver.flow_monthly (year, month, obidos_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(fecha, 'DD/MM/YYYY HH24:MI')      AS fecha_ts,
        REPLACE(
            NULLIF(valor, ''),
            ',', '.'
        )::double precision                             AS valor_num
    FROM bronze.obidos
    WHERE fecha IS NOT NULL
      AND valor IS NOT NULL
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
      AND valor_num > 0
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS obidos_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, obidos_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET obidos_monthly = EXCLUDED.obidos_monthly;

\echo 'OK - Obidos'


-- ==========================================================
-- TABATINGA  (Rio Amazonas, Brasil)
-- ==========================================================
-- Fuente: bronze.tabatinga (raw_line TEXT)
-- Estructura del raw_line: identica a Ciudad Bolivar y Manaos.
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- Agregacion: AVG diario -> mensual
-- ==========================================================
\echo 'Procesando SILVER - Tabatinga...'

INSERT INTO silver.flow_monthly (year, month, tabatinga_monthly)
WITH parsing AS (
    SELECT
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        )                                            AS fecha_ts,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 4), ''),
            ',', '.'
        )::double precision                          AS valor_num
    FROM bronze.tabatinga
    WHERE raw_line ~ '^[0-9]'
),
limpieza AS (
    SELECT fecha_ts, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
      AND valor_num > 0
),
mensual AS (
    SELECT
        EXTRACT(YEAR  FROM fecha_ts)::int AS year,
        EXTRACT(MONTH FROM fecha_ts)::int AS month,
        AVG(valor_num)                    AS tabatinga_monthly
    FROM limpieza
    GROUP BY 1, 2
)
SELECT year, month, tabatinga_monthly
FROM mensual
ON CONFLICT (year, month)
DO UPDATE SET tabatinga_monthly = EXCLUDED.tabatinga_monthly;

\echo 'OK - Tabatinga'


-- ==========================================================
-- TIMBUES  (Rio Parana, Argentina)
-- ==========================================================
-- Fuente: bronze.timbues (raw_line TEXT)
-- Estructura del raw_line (separador ';'):
--   campo 1: fecha (DD/MM/YYYY)
--   campo 2: caudal mensual (coma como separador decimal)
-- Nota: este CSV ya viene en frecuencia mensual, no diaria.
--       No se agrega, se inserta directamente.
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL
-- ==========================================================
\echo 'Procesando SILVER - Timbues...'

INSERT INTO silver.flow_monthly (year, month, timbues_monthly)
WITH parsing AS (
    SELECT
        to_date(
            split_part(raw_line, ';', 1),
            'DD/MM/YYYY'
        )                                            AS fecha,
        REPLACE(
            NULLIF(split_part(raw_line, ';', 2), ''),
            ',', '.'
        )::double precision                          AS valor_num
    FROM bronze.timbues
    WHERE raw_line ~ '^[0-9]'
),
limpieza AS (
    SELECT fecha, valor_num
    FROM parsing
    WHERE valor_num IS NOT NULL
      AND valor_num > 0
)
SELECT
    EXTRACT(YEAR  FROM fecha)::int AS year,
    EXTRACT(MONTH FROM fecha)::int AS month,
    valor_num                      AS timbues_monthly
FROM limpieza
ON CONFLICT (year, month)
DO UPDATE SET timbues_monthly = EXCLUDED.timbues_monthly;

\echo 'OK - Timbues'


\echo '========================================'
\echo 'TRANSFORMACION SILVER FINALIZADA'
\echo '========================================'