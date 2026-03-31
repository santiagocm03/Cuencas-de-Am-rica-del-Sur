-- ==========================================================
-- 05_view_anomalies.sql  (Gold)
-- ==========================================================
-- Proposito : Vista de anomalias estandarizadas (z-scores)
--             de caudal mensual por estacion.
--
--             z = (valor_observado - media_mensual) / desv_std_mensual
--
--             La media y desviacion estandar se calculan por
--             estacion y mes del anio (climatologia mensual),
--             usando todos los anios disponibles en Silver.
--
--             Los meses sin observacion en Silver (NULL) se
--             imputan con la climatologia mensual correspondiente
--             antes de calcular el z-score.
--
--             Al ser una VIEW (no tabla), las anomalias se
--             recalculan automaticamente cuando Silver cambia.
--
-- Dependencias: silver.flow_monthly (tabla con datos limpios)
-- Ejecucion :
--   sudo -u postgres psql -d caudales -f sql/gold/05_view_anomalies.sql
-- Consulta :
--   SELECT * FROM gold.flow_monthly_anomalies
--   WHERE year = 2015 ORDER BY month;
-- ==========================================================

\set ON_ERROR_STOP on

-- Eliminar tabla legacy si existe (migracion de tabla a VIEW)

DROP VIEW  IF EXISTS gold.flow_monthly_anomalies;

CREATE OR REPLACE VIEW gold.flow_monthly_anomalies AS

-- 1. Normalizar a formato largo: una fila por (year, month, estacion)
--    Solo filas con valor observado (no NULL) para calcular climatologia
WITH base AS (
    SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM silver.flow_monthly WHERE calamar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'bolivar',               bolivar_monthly          FROM silver.flow_monthly WHERE bolivar_monthly   IS NOT NULL
    UNION ALL
    SELECT year, month, 'manaos',                manaos_monthly           FROM silver.flow_monthly WHERE manaos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'obidos',                obidos_monthly           FROM silver.flow_monthly WHERE obidos_monthly    IS NOT NULL
    UNION ALL
    SELECT year, month, 'tabatinga',             tabatinga_monthly        FROM silver.flow_monthly WHERE tabatinga_monthly IS NOT NULL
    UNION ALL
    SELECT year, month, 'timbues',               timbues_monthly          FROM silver.flow_monthly WHERE timbues_monthly   IS NOT NULL
),

-- 2. Climatologia: media y desv. std. por estacion y mes del anio
--    Se calcula SOLO sobre valores observados (no imputados)
--    para no contaminar la referencia climatologica con ella misma
stats AS (
    SELECT
        station,
        month,
        AVG(value)         AS mean_month,
        STDDEV_SAMP(value) AS std_month
    FROM base
    GROUP BY station, month
),

-- 3. Generar la grilla completa de (year, month, station)
--    para todos los anios presentes en Silver, incluyendo
--    los meses que Silver tiene como NULL
grilla AS (
    SELECT
        fm.year,
        fm.month,
        s.station
    FROM silver.flow_monthly fm
    CROSS JOIN (SELECT DISTINCT station FROM base) s
),

-- 4. Traer el valor observado de Silver para cada celda de la grilla.
--    Las celdas sin observacion quedan con value = NULL aqui.
silver_largo AS (
    SELECT year, month, 'calamar'   AS station, calamar_monthly   AS value FROM silver.flow_monthly
    UNION ALL
    SELECT year, month, 'bolivar',               bolivar_monthly          FROM silver.flow_monthly
    UNION ALL
    SELECT year, month, 'manaos',                manaos_monthly           FROM silver.flow_monthly
    UNION ALL
    SELECT year, month, 'obidos',                obidos_monthly           FROM silver.flow_monthly
    UNION ALL
    SELECT year, month, 'tabatinga',             tabatinga_monthly        FROM silver.flow_monthly
    UNION ALL
    SELECT year, month, 'timbues',               timbues_monthly          FROM silver.flow_monthly
),

-- 5. Imputacion: si el valor observado es NULL,
--    se reemplaza con la climatologia mensual correspondiente.
--    COALESCE(observado, climatologia) -> nunca NULL si hay climatologia
imputado AS (
    SELECT
        g.year,
        g.month,
        g.station,
        COALESCE(sl.value, st.mean_month) AS value,
        CASE
            WHEN sl.value IS NULL THEN TRUE
            ELSE FALSE
        END AS es_imputado
    FROM grilla g
    JOIN silver_largo sl
      ON sl.year    = g.year
     AND sl.month   = g.month
     AND sl.station = g.station
    JOIN stats st
      ON st.station = g.station
     AND st.month   = g.month
),

-- 6. Calcular z-score sobre la serie imputada
--    NULLIF protege contra division por cero cuando solo hay 1 anio de datos
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

-- 7. Pivotar de vuelta a formato wide (una columna por estacion)
SELECT
    year,
    month,
    MAX(anomaly)     FILTER (WHERE station = 'calamar')   AS calamar_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'bolivar')   AS bolivar_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'manaos')    AS manaos_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'obidos')    AS obidos_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'tabatinga') AS tabatinga_anomaly,
    MAX(anomaly)     FILTER (WHERE station = 'timbues')   AS timbues_anomaly,
    -- Columnas de auditoria: indican que estaciones fueron imputadas ese mes
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
    'Los NULLs de Silver se imputan con climatologia mensual antes del calculo. '
    'Las columnas *_imputado indican si ese mes fue imputado (TRUE) u observado (FALSE).';

SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold';
SELECT table_name FROM information_schema.views WHERE table_schema = 'gold';
SELECT COUNT(*) FROM silver.flow_monthly;

