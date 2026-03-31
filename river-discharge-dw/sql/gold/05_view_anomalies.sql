-- ==========================================================
-- 05_gold.sql  (Gold)
-- ==========================================================
-- Proposito : Construir la capa Gold a partir de las tablas
--             silver limpias por estacion.
--
--             Crea dos objetos:
--               1. gold.flow_monthly (TABLE)
--                  Une las 6 tablas silver, agrega de diario
--                  a mensual (AVG) y consolida en formato wide.
--                  Timbues ya viene mensual: se inserta directo.
--
--               2. gold.flow_monthly_anomalies (VIEW)
--                  Sobre flow_monthly calcula:
--                  - Climatologia (media y desv. std. por estacion-mes)
--                  - Imputacion de NULLs con climatologia
--                  - Z-scores estandarizados
--                  Las columnas *_imputado indican meses imputados.
--
-- Dependencias: silver.calamar, silver.ciudad_bolivar,
--               silver.manaos, silver.obidos,
--               silver.tabatinga, silver.timbues
-- Ejecucion :
--   sudo -u postgres psql -d Database -f sql/gold/05_view_anomalies.sql
-- Consulta :
--   SELECT * FROM gold.flow_monthly_anomalies
--   WHERE year = 2015 ORDER BY month;
-- ==========================================================

\set ON_ERROR_STOP on

SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'INICIO CONSTRUCCION GOLD'
\echo '========================================'


-- ==========================================================
-- PARTE 1: gold.flow_monthly  (TABLE)
-- ==========================================================
-- Una fila por (year, month), una columna por estacion.
-- Valores en m3/s. NULL indica mes sin observacion.
-- ==========================================================
\echo 'Creando gold.flow_monthly...'

DROP TABLE IF EXISTS gold.flow_monthly CASCADE;
CREATE TABLE gold.flow_monthly (
    year  INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),

    calamar_monthly    DOUBLE PRECISION,
    bolivar_monthly    DOUBLE PRECISION,
    manaos_monthly     DOUBLE PRECISION,
    obidos_monthly     DOUBLE PRECISION,
    tabatinga_monthly  DOUBLE PRECISION,
    timbues_monthly    DOUBLE PRECISION,

    CONSTRAINT pk_flow_monthly PRIMARY KEY (year, month)
);

COMMENT ON TABLE gold.flow_monthly IS
    'Caudales mensuales promedio (m3/s) por estacion. NULL indica mes sin observacion.';

-- Calamar: AVG diario -> mensual
INSERT INTO gold.flow_monthly (year, month, calamar_monthly)
SELECT
    EXTRACT(YEAR  FROM fecha)::int,
    EXTRACT(MONTH FROM fecha)::int,
    AVG(valor)
FROM silver.calamar
GROUP BY 1, 2;

-- Ciudad Bolivar: AVG diario -> mensual
INSERT INTO gold.flow_monthly (year, month, bolivar_monthly)
SELECT
    EXTRACT(YEAR  FROM fecha)::int,
    EXTRACT(MONTH FROM fecha)::int,
    AVG(valor)
FROM silver.ciudad_bolivar
GROUP BY 1, 2
ON CONFLICT (year, month)
DO UPDATE SET bolivar_monthly = EXCLUDED.bolivar_monthly;

-- Manaos: AVG diario -> mensual
INSERT INTO gold.flow_monthly (year, month, manaos_monthly)
SELECT
    EXTRACT(YEAR  FROM fecha)::int,
    EXTRACT(MONTH FROM fecha)::int,
    AVG(valor)
FROM silver.manaos
GROUP BY 1, 2
ON CONFLICT (year, month)
DO UPDATE SET manaos_monthly = EXCLUDED.manaos_monthly;

-- Obidos: AVG diario -> mensual
INSERT INTO gold.flow_monthly (year, month, obidos_monthly)
SELECT
    EXTRACT(YEAR  FROM fecha)::int,
    EXTRACT(MONTH FROM fecha)::int,
    AVG(valor)
FROM silver.obidos
GROUP BY 1, 2
ON CONFLICT (year, month)
DO UPDATE SET obidos_monthly = EXCLUDED.obidos_monthly;

-- Tabatinga: AVG diario -> mensual
INSERT INTO gold.flow_monthly (year, month, tabatinga_monthly)
SELECT
    EXTRACT(YEAR  FROM fecha)::int,
    EXTRACT(MONTH FROM fecha)::int,
    AVG(valor)
FROM silver.tabatinga
GROUP BY 1, 2
ON CONFLICT (year, month)
DO UPDATE SET tabatinga_monthly = EXCLUDED.tabatinga_monthly;

-- Timbues: ya viene mensual, insercion directa
INSERT INTO gold.flow_monthly (year, month, timbues_monthly)
SELECT
    EXTRACT(YEAR  FROM fecha)::int,
    EXTRACT(MONTH FROM fecha)::int,
    valor
FROM silver.timbues
ON CONFLICT (year, month)
DO UPDATE SET timbues_monthly = EXCLUDED.timbues_monthly;

\echo 'OK - gold.flow_monthly'


-- ==========================================================
-- PARTE 2: gold.flow_monthly_anomalies  (VIEW)
-- ==========================================================
-- Calcula z-scores sobre gold.flow_monthly.
-- Imputa NULLs con climatologia antes de estandarizar.
-- ==========================================================
\echo 'Creando gold.flow_monthly_anomalies...'

DROP VIEW  IF EXISTS gold.flow_monthly_anomalies;

CREATE OR REPLACE VIEW gold.flow_monthly_anomalies AS

-- 1. Formato largo: una fila por (year, month, station)
--    Solo valores observados para calcular climatologia limpia
WITH base AS (
    SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM gold.flow_monthly WHERE calamar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'bolivar',               bolivar_monthly          FROM gold.flow_monthly WHERE bolivar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'manaos',                manaos_monthly           FROM gold.flow_monthly WHERE manaos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'obidos',                obidos_monthly           FROM gold.flow_monthly WHERE obidos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'tabatinga',             tabatinga_monthly        FROM gold.flow_monthly WHERE tabatinga_monthly IS NOT NULL
    UNION ALL
    SELECT year, month, 'timbues',               timbues_monthly          FROM gold.flow_monthly WHERE timbues_monthly   IS NOT NULL
),

-- 2. Climatologia: media y desv. std. por estacion y mes
--    Calculada SOLO sobre observados para no contaminar con imputados
stats AS (
    SELECT
        station,
        month,
        AVG(value)         AS mean_month,
        STDDEV_SAMP(value) AS std_month
    FROM base
    GROUP BY station, month
),

-- 3. Grilla completa (year, month, station) incluyendo NULLs
grilla AS (
    SELECT fm.year, fm.month, s.station
    FROM gold.flow_monthly fm
    CROSS JOIN (SELECT DISTINCT station FROM base) s
),

-- 4. Formato largo incluyendo NULLs para detectar huecos
flow_largo AS (
    SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM gold.flow_monthly
    UNION ALL
    SELECT year, month, 'bolivar',               bolivar_monthly          FROM gold.flow_monthly
    UNION ALL
    SELECT year, month, 'manaos',                manaos_monthly           FROM gold.flow_monthly
    UNION ALL
    SELECT year, month, 'obidos',                obidos_monthly           FROM gold.flow_monthly
    UNION ALL
    SELECT year, month, 'tabatinga',             tabatinga_monthly        FROM gold.flow_monthly
    UNION ALL
    SELECT year, month, 'timbues',               timbues_monthly          FROM gold.flow_monthly
),

-- 5. Imputacion: NULLs -> climatologia mensual
imputado AS (
    SELECT
        g.year,
        g.month,
        g.station,
        COALESCE(fl.value, st.mean_month)              AS value,
        CASE WHEN fl.value IS NULL THEN TRUE ELSE FALSE END AS es_imputado
    FROM grilla g
    JOIN flow_largo fl ON fl.year = g.year AND fl.month = g.month AND fl.station = g.station
    JOIN stats     st  ON st.station = g.station AND st.month = g.month
),

-- 6. Z-score: (valor - media) / desv_std
--    NULLIF evita division por cero en series con un solo anio
anom AS (
    SELECT
        i.year,
        i.month,
        i.station,
        i.es_imputado,
        (i.value - s.mean_month) / NULLIF(s.std_month, 0) AS anomaly
    FROM imputado i
    JOIN stats s ON s.station = i.station AND s.month = i.month
)

-- 7. Pivot a formato wide
SELECT
    year,
    month,
    MAX(anomaly)         FILTER (WHERE station = 'calamar')   AS calamar_anomaly,
    MAX(anomaly)         FILTER (WHERE station = 'bolivar')   AS bolivar_anomaly,
    MAX(anomaly)         FILTER (WHERE station = 'manaos')    AS manaos_anomaly,
    MAX(anomaly)         FILTER (WHERE station = 'obidos')    AS obidos_anomaly,
    MAX(anomaly)         FILTER (WHERE station = 'tabatinga') AS tabatinga_anomaly,
    MAX(anomaly)         FILTER (WHERE station = 'timbues')   AS timbues_anomaly,
    BOOL_OR(es_imputado) FILTER (WHERE station = 'calamar')   AS calamar_imputado,
    BOOL_OR(es_imputado) FILTER (WHERE station = 'bolivar')   AS bolivar_imputado,
    BOOL_OR(es_imputado) FILTER (WHERE station = 'manaos')    AS manaos_imputado,
    BOOL_OR(es_imputado) FILTER (WHERE station = 'obidos')    AS obidos_imputado,
    BOOL_OR(es_imputado) FILTER (WHERE station = 'tabatinga') AS tabatinga_imputado,
    BOOL_OR(es_imputado) FILTER (WHERE station = 'timbues')   AS timbues_imputado
FROM anom
GROUP BY year, month
ORDER BY year, month;

COMMENT ON VIEW gold.flow_monthly_anomalies IS
    'Z-scores de caudal mensual por estacion. Imputa NULLs con climatologia mensual. '
    'Las columnas *_imputado indican meses imputados (TRUE) vs observados (FALSE).';

\echo 'OK - gold.flow_monthly_anomalies'

\echo '========================================'
\echo 'CONSTRUCCION GOLD FINALIZADA'
\echo '========================================'