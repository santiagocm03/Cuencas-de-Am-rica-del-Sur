-- ==========================================================
-- 03_profiling.sql  (Bronze)
-- ==========================================================
-- Proposito : Perfilar los tipos de dato de las tablas bronze
--             para verificar que cada campo parsea correctamente
--             y determinar el tipo apropiado para Silver.
--
--             Por cada estacion se ejecutan 3 checks:
--               1. n_campos   — cuantos campos tiene cada fila
--               2. decimales  — INT vs DOUBLE PRECISION
--               3. cast       — confirma que fecha y valor
--                               parsean sin error
--
-- Dependencias: 02_load_insert.sql (bronze cargado)
-- Ejecucion :
--   sudo -u postgres psql -d caudales -f sql/bronze/03_profiling.sql
-- ==========================================================

\set ON_ERROR_STOP on

\pset format unaligned
\pset tuples_only on
\pset fieldsep ' | '

\echo '========================================'
\echo 'INICIO PROFILING DE TIPOS - BRONZE'
\echo '========================================'


-- ==========================================================
-- CALAMAR — CSV estructurado (21 columnas TEXT)
-- ==========================================================

\echo ''
\echo '========== CALAMAR =========='

\echo ''
\echo '-- [1] Numero de columnas en la tabla'
\echo 'n_columnas'
SELECT COUNT(*) AS n_columnas
FROM information_schema.columns
WHERE table_schema = 'bronze' AND table_name = 'calamar';

\echo ''
\echo '-- [2] valor: tiene decimales? -> define INT vs DOUBLE PRECISION'
\echo 'con_decimales | sin_decimales | tipo_recomendado'
SELECT
    COUNT(*) FILTER (WHERE valor LIKE '%.%')       AS con_decimales,
    COUNT(*) FILTER (WHERE valor NOT LIKE '%.%')   AS sin_decimales,
    CASE
        WHEN COUNT(*) FILTER (WHERE valor LIKE '%.%') > 0
        THEN 'DOUBLE PRECISION'
        ELSE 'INT (pero DOUBLE PRECISION si Silver calcula AVG)'
    END AS tipo_recomendado
FROM bronze.calamar
WHERE valor ~ '^[0-9]+(\.[0-9]+)?$';

\echo ''
\echo '-- [3] Cast exitoso: fecha -> DATE, valor -> DOUBLE PRECISION'
\echo 'fecha_min | fecha_max | valor_min | valor_max | cast_exitoso'
SELECT
    MIN(fecha::date)                               AS fecha_min,
    MAX(fecha::date)                               AS fecha_max,
    MIN(valor::double precision)                   AS valor_min,
    MAX(valor::double precision)                   AS valor_max,
    'SI' AS cast_exitoso
FROM bronze.calamar
WHERE valor ~ '^[0-9]+(\.[0-9]+)?$';


-- ==========================================================
-- OBIDOS — CSV con 5 columnas TEXT
-- ==========================================================

\echo ''
\echo '========== OBIDOS =========='

\echo ''
\echo '-- [1] Numero de columnas en la tabla'
\echo 'n_columnas'
SELECT COUNT(*) AS n_columnas
FROM information_schema.columns
WHERE table_schema = 'bronze' AND table_name = 'obidos';

\echo ''
\echo '-- [2] valor: tiene decimales? -> define INT vs DOUBLE PRECISION'
\echo 'con_decimales | sin_decimales | tipo_recomendado'
SELECT
    COUNT(*) FILTER (WHERE REPLACE(valor,',','.') LIKE '%.%') AS con_decimales,
    COUNT(*) FILTER (WHERE REPLACE(valor,',','.') NOT LIKE '%.%') AS sin_decimales,
    CASE
        WHEN COUNT(*) FILTER (WHERE REPLACE(valor,',','.') LIKE '%.%') > 0
        THEN 'DOUBLE PRECISION'
        ELSE 'INT (pero DOUBLE PRECISION si Silver calcula AVG)'
    END AS tipo_recomendado
FROM bronze.obidos
WHERE valor IS NOT NULL;

\echo ''
\echo '-- [3] Cast exitoso: fecha -> TIMESTAMP, valor -> DOUBLE PRECISION'
\echo 'fecha_min | fecha_max | valor_min | valor_max | cast_exitoso'
SELECT
    MIN(to_timestamp(fecha, 'DD/MM/YYYY HH24:MI')) AS fecha_min,
    MAX(to_timestamp(fecha, 'DD/MM/YYYY HH24:MI')) AS fecha_max,
    MIN(REPLACE(valor, ',', '.')::double precision) AS valor_min,
    MAX(REPLACE(valor, ',', '.')::double precision) AS valor_max,
    'SI' AS cast_exitoso
FROM bronze.obidos
WHERE valor IS NOT NULL;


-- ==========================================================
-- CIUDAD BOLIVAR — raw_line TEXT
-- ==========================================================

\echo ''
\echo '========== CIUDAD BOLIVAR =========='

\echo ''
\echo '-- [1] Numero de campos por linea (separador ;)'
\echo 'n_campos | frecuencia'
SELECT
    LENGTH(raw_line) - LENGTH(REPLACE(raw_line, ';', '')) + 1 AS n_campos,
    COUNT(*) AS frecuencia
FROM bronze.ciudad_bolivar
WHERE raw_line ~ '^[0-9]'
GROUP BY 1
ORDER BY 2 DESC;

\echo ''
\echo '-- [2] campo 4 (valor): tiene decimales? -> define INT vs DOUBLE PRECISION'
\echo 'con_decimales | sin_decimales | tipo_recomendado'
SELECT
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') LIKE '%.%') AS con_decimales,
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') NOT LIKE '%.%') AS sin_decimales,
    CASE
        WHEN COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') LIKE '%.%') > 0
        THEN 'DOUBLE PRECISION'
        ELSE 'INT (pero DOUBLE PRECISION si Silver calcula AVG)'
    END AS tipo_recomendado
FROM bronze.ciudad_bolivar
WHERE raw_line ~ '^[0-9]';

\echo ''
\echo '-- [3] Cast exitoso: campo 3 -> TIMESTAMP, campo 4 -> DOUBLE PRECISION'
\echo 'fecha_min | fecha_max | valor_min | valor_max | cast_exitoso'
SELECT
    MIN(to_timestamp(split_part(raw_line,';',3), 'DD/MM/YYYY HH24:MI')) AS fecha_min,
    MAX(to_timestamp(split_part(raw_line,';',3), 'DD/MM/YYYY HH24:MI')) AS fecha_max,
    MIN(REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.')::double precision) AS valor_min,
    MAX(REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.')::double precision) AS valor_max,
    'SI' AS cast_exitoso
FROM bronze.ciudad_bolivar
WHERE raw_line ~ '^[0-9]';


-- ==========================================================
-- MANAOS — raw_line TEXT
-- ==========================================================

\echo ''
\echo '========== MANAOS =========='

\echo ''
\echo '-- [1] Numero de campos por linea (separador ;)'
\echo 'n_campos | frecuencia'
SELECT
    LENGTH(raw_line) - LENGTH(REPLACE(raw_line, ';', '')) + 1 AS n_campos,
    COUNT(*) AS frecuencia
FROM bronze.manaos
WHERE raw_line ~ '^[0-9]'
GROUP BY 1
ORDER BY 2 DESC;

\echo ''
\echo '-- [2] campo 4 (valor): tiene decimales? -> define INT vs DOUBLE PRECISION'
\echo 'con_decimales | sin_decimales | tipo_recomendado'
SELECT
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') LIKE '%.%') AS con_decimales,
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') NOT LIKE '%.%') AS sin_decimales,
    CASE
        WHEN COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') LIKE '%.%') > 0
        THEN 'DOUBLE PRECISION'
        ELSE 'INT (pero DOUBLE PRECISION si Silver calcula AVG)'
    END AS tipo_recomendado
FROM bronze.manaos
WHERE raw_line ~ '^[0-9]';

\echo ''
\echo '-- [3] Cast exitoso: campo 3 -> TIMESTAMP, campo 4 -> DOUBLE PRECISION'
\echo 'fecha_min | fecha_max | valor_min | valor_max | cast_exitoso'
SELECT
    MIN(to_timestamp(split_part(raw_line,';',3), 'DD/MM/YYYY HH24:MI')) AS fecha_min,
    MAX(to_timestamp(split_part(raw_line,';',3), 'DD/MM/YYYY HH24:MI')) AS fecha_max,
    MIN(REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.')::double precision) AS valor_min,
    MAX(REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.')::double precision) AS valor_max,
    'SI' AS cast_exitoso
FROM bronze.manaos
WHERE raw_line ~ '^[0-9]';


-- ==========================================================
-- TABATINGA — raw_line TEXT
-- ==========================================================

\echo ''
\echo '========== TABATINGA =========='

\echo ''
\echo '-- [1] Numero de campos por linea (separador ;)'
\echo 'n_campos | frecuencia'
SELECT
    LENGTH(raw_line) - LENGTH(REPLACE(raw_line, ';', '')) + 1 AS n_campos,
    COUNT(*) AS frecuencia
FROM bronze.tabatinga
WHERE raw_line ~ '^[0-9]'
GROUP BY 1
ORDER BY 2 DESC;

\echo ''
\echo '-- [2] campo 4 (valor): tiene decimales? -> define INT vs DOUBLE PRECISION'
\echo 'con_decimales | sin_decimales | tipo_recomendado'
SELECT
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') LIKE '%.%') AS con_decimales,
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') NOT LIKE '%.%') AS sin_decimales,
    CASE
        WHEN COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.') LIKE '%.%') > 0
        THEN 'DOUBLE PRECISION'
        ELSE 'INT (pero DOUBLE PRECISION si Silver calcula AVG)'
    END AS tipo_recomendado
FROM bronze.tabatinga
WHERE raw_line ~ '^[0-9]';

\echo ''
\echo '-- [3] Cast exitoso: campo 3 -> TIMESTAMP, campo 4 -> DOUBLE PRECISION'
\echo 'fecha_min | fecha_max | valor_min | valor_max | cast_exitoso'
SELECT
    MIN(to_timestamp(split_part(raw_line,';',3), 'DD/MM/YYYY HH24:MI')) AS fecha_min,
    MAX(to_timestamp(split_part(raw_line,';',3), 'DD/MM/YYYY HH24:MI')) AS fecha_max,
    MIN(REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.')::double precision) AS valor_min,
    MAX(REPLACE(NULLIF(split_part(raw_line,';',4),''),',','.')::double precision) AS valor_max,
    'SI' AS cast_exitoso
FROM bronze.tabatinga
WHERE raw_line ~ '^[0-9]';


-- ==========================================================
-- TIMBUES — raw_line TEXT (2 campos, ya mensual)
-- ==========================================================

\echo ''
\echo '========== TIMBUES =========='

\echo ''
\echo '-- [1] Numero de campos por linea (separador ;)'
\echo 'n_campos | frecuencia'
SELECT
    LENGTH(raw_line) - LENGTH(REPLACE(raw_line, ';', '')) + 1 AS n_campos,
    COUNT(*) AS frecuencia
FROM bronze.timbues
WHERE raw_line ~ '^[0-9]'
GROUP BY 1
ORDER BY 2 DESC;

\echo ''
\echo '-- [2] campo 2 (valor): tiene decimales? -> define INT vs DOUBLE PRECISION'
\echo 'con_decimales | sin_decimales | tipo_recomendado'
SELECT
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',2),''),',','.') LIKE '%.%') AS con_decimales,
    COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',2),''),',','.') NOT LIKE '%.%') AS sin_decimales,
    CASE
        WHEN COUNT(*) FILTER (WHERE REPLACE(NULLIF(split_part(raw_line,';',2),''),',','.') LIKE '%.%') > 0
        THEN 'DOUBLE PRECISION'
        ELSE 'INT (pero DOUBLE PRECISION si Silver calcula AVG)'
    END AS tipo_recomendado
FROM bronze.timbues
WHERE raw_line ~ '^[0-9]';

\echo ''
\echo '-- [3] Cast exitoso: campo 1 -> DATE, campo 2 -> DOUBLE PRECISION'
\echo 'fecha_min | fecha_max | valor_min | valor_max | cast_exitoso'
SELECT
    MIN(to_date(split_part(raw_line,';',1), 'DD/MM/YYYY'))                         AS fecha_min,
    MAX(to_date(split_part(raw_line,';',1), 'DD/MM/YYYY'))                         AS fecha_max,
    MIN(REPLACE(NULLIF(split_part(raw_line,';',2),''),',','.')::double precision)   AS valor_min,
    MAX(REPLACE(NULLIF(split_part(raw_line,';',2),''),',','.')::double precision)   AS valor_max,
    'SI' AS cast_exitoso
FROM bronze.timbues
WHERE raw_line ~ '^[0-9]';


\echo ''
\echo '========================================'
\echo 'PROFILING DE TIPOS FINALIZADO'
\echo '========================================'