-- ==========================================================
-- 04_insert.sql  (Silver)
-- ==========================================================
-- Proposito : Transformar datos crudos de bronze a caudales
--             diarios limpios en silver.flow_daily.
--
--             Cada estacion sigue el mismo patron canonico:
--               1. parsing  — extraer campos del dato crudo
--               2. limpieza — filtrar registros invalidos
--
--             Lo que NO hace este script (pertenece a Gold):
--               - Agregar de diario a mensual
--               - Calcular climatologia historica
--               - Imputar huecos
--               - Calcular anomalias o z-scores
--
--             Los dias sin observacion quedan como NULL en
--             silver.flow_daily. Gold decide que hacer con
--             esos huecos al agregar a mensual.
--
-- Nota sobre Timbues: la fuente ya viene en frecuencia mensual.
--             Se inserta con day=1 por convencion para mantener
--             la estructura de la tabla.
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
-- ==========================================================
\echo 'Procesando SILVER - Calamar...'

INSERT INTO silver.flow_daily (year, month, day, calamar_daily)
WITH parsing AS (
    SELECT
        fecha::date                      AS fecha,
        nombre_estacion,
        descripcion_serie,
        valor
    FROM bronze.calamar
    WHERE fecha IS NOT NULL
      AND valor IS NOT NULL
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
)
SELECT
    EXTRACT(YEAR  FROM fecha)::int AS year,
    EXTRACT(MONTH FROM fecha)::int AS month,
    EXTRACT(DAY   FROM fecha)::int AS day,
    valor_num                      AS calamar_daily
FROM limpieza
ON CONFLICT (year, month, day)
DO UPDATE SET calamar_daily = EXCLUDED.calamar_daily;

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
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Ciudad Bolivar...'

INSERT INTO silver.flow_daily (year, month, day, bolivar_daily)
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
)
SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int AS year,
    EXTRACT(MONTH FROM fecha_ts)::int AS month,
    EXTRACT(DAY   FROM fecha_ts)::int AS day,
    valor_num                         AS bolivar_daily
FROM limpieza
ON CONFLICT (year, month, day)
DO UPDATE SET bolivar_daily = EXCLUDED.bolivar_daily;

\echo 'OK - Ciudad Bolivar'


-- ==========================================================
-- MANAOS  (Rio Negro / Amazonas, Brasil)
-- ==========================================================
-- Fuente: bronze.manaos (raw_line TEXT)
-- Estructura del raw_line: identica a Ciudad Bolivar.
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Manaos...'

INSERT INTO silver.flow_daily (year, month, day, manaos_daily)
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
)
SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int AS year,
    EXTRACT(MONTH FROM fecha_ts)::int AS month,
    EXTRACT(DAY   FROM fecha_ts)::int AS day,
    valor_num                         AS manaos_daily
FROM limpieza
ON CONFLICT (year, month, day)
DO UPDATE SET manaos_daily = EXCLUDED.manaos_daily;

\echo 'OK - Manaos'


-- ==========================================================
-- OBIDOS  (Rio Amazonas, Brasil)
-- ==========================================================
-- Fuente: bronze.obidos (5 columnas TEXT)
-- Columnas: id_estacion, nombre, fecha, valor, origen
-- Limpieza:
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Obidos...'

INSERT INTO silver.flow_daily (year, month, day, obidos_daily)
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
)
SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int AS year,
    EXTRACT(MONTH FROM fecha_ts)::int AS month,
    EXTRACT(DAY   FROM fecha_ts)::int AS day,
    valor_num                         AS obidos_daily
FROM limpieza
ON CONFLICT (year, month, day)
DO UPDATE SET obidos_daily = EXCLUDED.obidos_daily;

\echo 'OK - Obidos'


-- ==========================================================
-- TABATINGA  (Rio Amazonas, Brasil)
-- ==========================================================
-- Fuente: bronze.tabatinga (raw_line TEXT)
-- Estructura del raw_line: identica a Ciudad Bolivar y Manaos.
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Tabatinga...'

INSERT INTO silver.flow_daily (year, month, day, tabatinga_daily)
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
)
SELECT
    EXTRACT(YEAR  FROM fecha_ts)::int AS year,
    EXTRACT(MONTH FROM fecha_ts)::int AS month,
    EXTRACT(DAY   FROM fecha_ts)::int AS day,
    valor_num                         AS tabatinga_daily
FROM limpieza
ON CONFLICT (year, month, day)
DO UPDATE SET tabatinga_daily = EXCLUDED.tabatinga_daily;

\echo 'OK - Tabatinga'


-- ==========================================================
-- TIMBUES  (Rio Parana, Argentina)
-- ==========================================================
-- Fuente: bronze.timbues (raw_line TEXT)
-- Estructura del raw_line (separador ';'):
--   campo 1: fecha (DD/MM/YYYY)
--   campo 2: caudal mensual (coma como separador decimal)
-- Nota: esta fuente ya viene en frecuencia mensual.
--       Se inserta con day=1 por convencion para mantener
--       la estructura de la tabla. Gold lo trata como
--       valor mensual directamente (sin AVG).
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Timbues...'

INSERT INTO silver.flow_daily (year, month, day, timbues_daily)
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
    1                              AS day,  -- convencion: fuente mensual
    valor_num                      AS timbues_daily
FROM limpieza
ON CONFLICT (year, month, day)
DO UPDATE SET timbues_daily = EXCLUDED.timbues_daily;

\echo 'OK - Timbues'


\echo '========================================'
\echo 'TRANSFORMACION SILVER FINALIZADA'
\echo '========================================'