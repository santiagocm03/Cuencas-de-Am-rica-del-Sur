-- ==========================================================
-- 04_insert.sql  (Silver)
-- ==========================================================
-- Proposito : Parsear y limpiar cada tabla bronze en su
--             correspondiente tabla silver con tipos correctos.
--
--             Patron canonico por estacion:
--               1. parsing  — castear TEXT a tipos correctos
--               2. limpieza — filtrar registros invalidos
--
--             Lo que NO hace este script (pertenece a Gold):
--               - Unir estaciones en formato wide
--               - Agregar de diario a mensual
--               - Calcular climatologia
--               - Imputar huecos
--               - Calcular z-scores
--
-- Dependencias: 03_ddl.sql (silver), 02_load_insert.sql (bronze)
-- Ejecucion :
--   sudo -u postgres psql -d Database -f sql/silver/04_insert.sql
-- ==========================================================

\set ON_ERROR_STOP on

SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'INICIO TRANSFORMACION SILVER'
\echo '========================================'


-- ==========================================================
-- CALAMAR  (Rio Magdalena, Colombia)
-- ==========================================================
-- Bronze: 21 columnas TEXT
-- Limpieza:
--   - Solo estacion 'CALAMAR' (sin sufijos entre corchetes)
--   - Solo serie 'Caudal medio diario'
--   - Datos hasta 2016-12 (cambio de sensor desde 2017)
--   - Valor numerico positivo <= 16000 m3/s (umbral fisico)
-- ==========================================================
\echo 'Procesando SILVER - Calamar...'

TRUNCATE silver.calamar;
INSERT INTO silver.calamar
SELECT
    codigo_estacion,
    nombre_estacion,
    REPLACE(latitud,  '.', '')::double precision / 1e9  AS latitud,
    REPLACE(longitud, '.', '')::double precision / 1e9  AS longitud,
    altitud::double precision,
    categoria,
    entidad,
    area_operativa,
    departamento,
    municipio,
    CASE WHEN fecha_instalacion IS NOT NULL AND fecha_instalacion <> ''
         THEN to_timestamp(fecha_instalacion, 'DD/MM/YYYY HH24:MI')
         ELSE NULL END                                  AS fecha_instalacion,
    CASE WHEN fecha_suspension IS NOT NULL AND fecha_suspension <> ''
         THEN to_timestamp(fecha_suspension, 'DD/MM/YYYY HH24:MI')
         ELSE NULL END                                  AS fecha_suspension,
    id_parametro,
    etiqueta,
    descripcion_serie,
    frecuencia,
    fecha::date,
    valor::double precision,
    grado::integer,
    calificador,
    nivel_aprobacion::integer
FROM bronze.calamar
WHERE
    regexp_replace(nombre_estacion, '\s*\[.*\]', '') = 'CALAMAR'
    AND descripcion_serie = 'Caudal medio diario'
    AND fecha < '2017-01-01'
    AND valor IS NOT NULL
    AND valor ~ '^[0-9]+(\.[0-9]+)?$'
    AND valor::double precision > 0
    AND valor::double precision <= 16000;

\echo 'OK - Calamar'


-- ==========================================================
-- CIUDAD BOLIVAR  (Rio Orinoco, Venezuela)
-- ==========================================================
-- Bronze: raw_line con 7 campos separados por ';'
--   campo 1: id_station
--   campo 2: nom
--   campo 3: fecha (D/MM/YYYY HH24:MI)
--   campo 4: valor (coma decimal)
--   campo 5: origine
--   campo 6: qualite
--   campo 7: vacio (trailing semicolon)
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Ciudad Bolivar...'

TRUNCATE silver.ciudad_bolivar;
INSERT INTO silver.ciudad_bolivar
WITH parsing AS (
    SELECT
        split_part(raw_line, ';', 1)                         AS id_station,
        split_part(raw_line, ';', 2)                         AS nom,
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        )                                                    AS fecha,
        REPLACE(
            NULLIF(TRIM(split_part(raw_line, ';', 4)), ''),
            ',', '.'
        )::double precision                                  AS valor,
        NULLIF(TRIM(split_part(raw_line, ';', 5)), '')       AS origine,
        NULLIF(TRIM(split_part(raw_line, ';', 6)), '')       AS qualite
    FROM bronze.ciudad_bolivar
    WHERE raw_line ~ '^[0-9]'
)
SELECT id_station, nom, fecha, valor, origine, qualite
FROM parsing
WHERE valor IS NOT NULL
  AND valor > 0;

\echo 'OK - Ciudad Bolivar'


-- ==========================================================
-- MANAOS  (Rio Negro / Amazonas, Brasil)
-- ==========================================================
-- Bronze: raw_line con 6 campos separados por ';'
--   campo 1: id_station
--   campo 2: nom
--   campo 3: fecha (D/MM/YYYY HH24:MI)
--   campo 4: valor (coma decimal)
--   campo 5: origine
--   campo 6: qualite
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Manaos...'

TRUNCATE silver.manaos;
INSERT INTO silver.manaos
WITH parsing AS (
    SELECT
        split_part(raw_line, ';', 1)                         AS id_station,
        split_part(raw_line, ';', 2)                         AS nom,
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        )                                                    AS fecha,
        REPLACE(
            NULLIF(TRIM(split_part(raw_line, ';', 4)), ''),
            ',', '.'
        )::double precision                                  AS valor,
        NULLIF(TRIM(split_part(raw_line, ';', 5)), '')       AS origine,
        NULLIF(TRIM(split_part(raw_line, ';', 6)), '')       AS qualite
    FROM bronze.manaos
    WHERE raw_line ~ '^[0-9]'
)
SELECT id_station, nom, fecha, valor, origine, qualite
FROM parsing
WHERE valor IS NOT NULL
  AND valor > 0;

\echo 'OK - Manaos'


-- ==========================================================
-- OBIDOS  (Rio Amazonas, Brasil)
-- ==========================================================
-- Bronze: 5 columnas TEXT
--   id_station, nom, fecha (DD/MM/YYYY HH24:MI),
--   valor (coma decimal), origine
-- Limpieza:
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Obidos...'

TRUNCATE silver.obidos;
INSERT INTO silver.obidos
WITH parsing AS (
    SELECT
        id_estacion                                          AS id_station,
        nombre                                               AS nom,
        to_timestamp(fecha, 'DD/MM/YYYY HH24:MI')           AS fecha,
        REPLACE(
            NULLIF(TRIM(valor), ''),
            ',', '.'
        )::double precision                                  AS valor,
        origen                                               AS origine
    FROM bronze.obidos
    WHERE fecha IS NOT NULL
      AND valor IS NOT NULL
)
SELECT id_station, nom, fecha, valor, origine
FROM parsing
WHERE valor IS NOT NULL
  AND valor > 0;

\echo 'OK - Obidos'


-- ==========================================================
-- TABATINGA  (Rio Amazonas, Brasil)
-- ==========================================================
-- Bronze: raw_line con 7 campos separados por ';'
--   campo 1: id_station
--   campo 2: nom
--   campo 3: fecha (D/MM/YYYY HH24:MI)
--   campo 4: valor (coma decimal)
--   campo 5: origine
--   campo 6: qualite
--   campo 7: valor_medio_mensual (coma decimal, precalculado)
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos en campo principal
-- ==========================================================
\echo 'Procesando SILVER - Tabatinga...'

TRUNCATE silver.tabatinga;
INSERT INTO silver.tabatinga
WITH parsing AS (
    SELECT
        split_part(raw_line, ';', 1)                         AS id_station,
        split_part(raw_line, ';', 2)                         AS nom,
        to_timestamp(
            split_part(raw_line, ';', 3),
            'DD/MM/YYYY HH24:MI'
        )                                                    AS fecha,
        REPLACE(
            NULLIF(TRIM(split_part(raw_line, ';', 4)), ''),
            ',', '.'
        )::double precision                                  AS valor,
        NULLIF(TRIM(split_part(raw_line, ';', 5)), '')       AS origine,
        NULLIF(TRIM(split_part(raw_line, ';', 6)), '')       AS qualite,
        REPLACE(
            NULLIF(TRIM(split_part(raw_line, ';', 7)), ''),
            ',', '.'
        )::double precision                                  AS valor_medio_mensual
    FROM bronze.tabatinga
    WHERE raw_line ~ '^[0-9]'
)
SELECT id_station, nom, fecha, valor, origine, qualite, valor_medio_mensual
FROM parsing
WHERE valor IS NOT NULL
  AND valor > 0;

\echo 'OK - Tabatinga'


-- ==========================================================
-- TIMBUES  (Rio Parana, Argentina)
-- ==========================================================
-- Bronze: raw_line con 2 campos separados por ';'
--   campo 1: fecha (DD/MM/YYYY)
--   campo 2: valor mensual (coma decimal)
-- Nota: frecuencia mensual, no diaria.
-- Limpieza:
--   - Solo lineas que comienzan con digito
--   - Reemplazar coma decimal por punto
--   - Descartar valores NULL o no positivos
-- ==========================================================
\echo 'Procesando SILVER - Timbues...'

TRUNCATE silver.timbues;
INSERT INTO silver.timbues
WITH parsing AS (
    SELECT
        to_date(
            split_part(raw_line, ';', 1),
            'DD/MM/YYYY'
        )                                                    AS fecha,
        REPLACE(
            NULLIF(TRIM(split_part(raw_line, ';', 2)), ''),
            ',', '.'
        )::double precision                                  AS valor
    FROM bronze.timbues
    WHERE raw_line ~ '^[0-9]'
)
SELECT fecha, valor
FROM parsing
WHERE valor IS NOT NULL
  AND valor > 0;

\echo 'OK - Timbues'


\echo '========================================'
\echo 'TRANSFORMACION SILVER FINALIZADA'
\echo '========================================'