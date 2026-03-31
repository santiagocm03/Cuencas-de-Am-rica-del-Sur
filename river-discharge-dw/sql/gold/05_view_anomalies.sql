-- ==========================================================
-- 05_view_anomalies.sql  (Gold)
-- ==========================================================
-- Proposito : Vista de anomalias estandarizadas (z-scores)
--             de caudal mensual por estacion.
--
--             z = (valor_mensual - media_mensual) / desv_std_mensual
--
--             Ejecuta cuatro operaciones en secuencia:
--               1. agregacion  — AVG diario -> mensual
--               2. climatologia — media y desv. std. por estacion-mes
--               3. imputacion  — NULLs se rellenan con climatologia
--               4. z-score     — estandariza respecto a climatologia
--
--             Al ser una VIEW (no tabla), se recalcula
--             automaticamente cuando Silver cambia.
--
-- Nota sobre Timbues: la fuente ya viene en frecuencia mensual
--             (day=1 por convencion en Silver). Se trata
--             directamente como valor mensual sin AVG.
--
-- Dependencias: silver.flow_daily
-- Ejecucion :
--   sudo -u postgres psql -d caudales -f sql/gold/05_view_anomalies.sql
-- Consulta :
--   SELECT * FROM gold.flow_monthly_anomalies
--   WHERE year = 2015 ORDER BY month;
-- ==========================================================

\set ON_ERROR_STOP on


DROP VIEW  IF EXISTS gold.flow_monthly_anomalies;

CREATE OR REPLACE VIEW gold.flow_monthly_anomalies AS

-- 1. Agregacion diaria -> mensual (AVG por estacion-mes)
--    Timbues se excluye aqui porque ya viene mensual (day=1)
WITH mensual AS (
    SELECT
        year,
        month,
        AVG(calamar_daily)   AS calamar_monthly,
        AVG(bolivar_daily)   AS bolivar_monthly,
        AVG(manaos_daily)    AS manaos_monthly,
        AVG(obidos_daily)    AS obidos_monthly,
        AVG(tabatinga_daily) AS tabatinga_monthly,
        MAX(timbues_daily)   AS timbues_monthly  -- MAX porque solo hay 1 fila (day=1)
    FROM silver.flow_daily
    GROUP BY year, month
),

-- 2. Normalizar a formato largo para calcular estadisticos
--    Solo valores no NULL para no contaminar la climatologia
base AS (
    SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM mensual WHERE calamar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'bolivar',               bolivar_monthly          FROM mensual WHERE bolivar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'manaos',                manaos_monthly           FROM mensual WHERE manaos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'obidos',                obidos_monthly           FROM mensual WHERE obidos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'tabatinga',             tabatinga_monthly        FROM mensual WHERE tabatinga_monthly IS NOT NULL
    UNION ALL
    SELECT year, month, 'timbues',               timbues_monthly          FROM mensual WHERE timbues_monthly   IS NOT NULL
),

-- 3. Climatologia: media y desv. std. por estacion y mes del anio
--    Calculada SOLO sobre valores observados (no imputados)
stats AS (
    SELECT
        station,
        month,
        AVG(value)         AS mean_month,
        STDDEV_SAMP(value) AS std_month
    FROM base
    GROUP BY station, month
),

-- 4. Grilla completa de (year, month, station)
--    incluye los meses que mensual tiene como NULL
grilla AS (
    SELECT
        m.year,
        m.month,
        s.station
    FROM mensual m
    CROSS JOIN (SELECT DISTINCT station FROM base) s
),

-- 5. Traer el valor mensual para cada celda de la grilla
--    Las celdas sin observacion quedan con value = NULL
mensual_largo AS (
    SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM mensual
    UNION ALL
    SELECT year, month, 'bolivar',               bolivar_monthly          FROM mensual
    UNION ALL
    SELECT year, month, 'manaos',                manaos_monthly           FROM mensual
    UNION ALL
    SELECT year, month, 'obidos',                obidos_monthly           FROM mensual
    UNION ALL
    SELECT year, month, 'tabatinga',             tabatinga_monthly        FROM mensual
    UNION ALL
    SELECT year, month, 'timbues',               timbues_monthly          FROM mensual
),

-- 6. Imputacion: NULLs se rellenan con climatologia mensual
--    es_imputado = TRUE marca los valores imputados para auditoria
imputado AS (
    SELECT
        g.year,
        g.month,
        g.station,
        COALESCE(ml.value, st.mean_month) AS value,
        CASE WHEN ml.value IS NULL THEN TRUE ELSE FALSE END AS es_imputado
    FROM grilla g
    JOIN mensual_largo ml
      ON ml.year    = g.year
     AND ml.month   = g.month
     AND ml.station = g.station
    JOIN stats st
      ON st.station = g.station
     AND st.month   = g.month
),

-- 7. Z-score: estandariza respecto a la climatologia
--    NULLIF protege contra division por cero (solo 1 anio de datos)
anom AS (
    SELECT
        i.year,
        i.month,
        i.station,
        i.es_imputado,
        (i.value - s.mean_month) / NULLIF(s.std_month, 0) AS anomaly
    FROM imputado i
    JOIN stats s
      ON s.station = i.station
     AND s.month   = i.month
)

-- 8. Pivotar a formato wide (una columna por estacion)
--    Columnas *_imputado indican si el valor fue imputado ese mes
SELECT
    year,
    month,
    MAX(anomaly)     FILTER (WHERE station = 'calamar')   AS calamar_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'bolivar')   AS bolivar_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'manaos')    AS manaos_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'obidos')    AS obidos_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'tabatinga') AS tabatinga_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'timbues')   AS timbues_anomaly,
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
    'Anomalias estandarizadas (z-scores) de caudal mensual por estacion. '
    'Agrega silver.flow_daily a mensual, calcula climatologia, imputa NULLs '
    'y calcula z-scores. Las columnas *_imputado indican meses imputados.';