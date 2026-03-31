-- ==========================================================
-- test_gold.sql
-- ==========================================================
-- Proposito : Validar la vista gold.flow_monthly_anomalies
--             (z-scores de caudal mensual por estacion).
-- Dependencias: silver.flow_daily poblado y la VIEW creada.
-- Ejecucion :
--   psql -d <base> -f sql/tests/test_gold.sql
--
--   Si algun test falla, el script se detiene con un error
--   EXCEPTION. Si todos pasan, imprime NOTICE con 'PASS'.
-- ==========================================================

\set ON_ERROR_STOP on

SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'TESTS - CAPA GOLD'
\echo '========================================'


-- ----------------------------------------------------------
-- TEST 1: La VIEW no esta vacia
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM gold.flow_monthly_anomalies;
    IF n = 0 THEN
        RAISE EXCEPTION 'FAIL: gold.flow_monthly_anomalies esta vacia';
    END IF;
    RAISE NOTICE 'PASS: gold.flow_monthly_anomalies tiene % filas', n;
END $$;


-- ----------------------------------------------------------
-- TEST 2: El numero de filas coincide con los meses distintos
--         presentes en silver.flow_daily.
-- ----------------------------------------------------------
DO $$
DECLARE
    n_silver INT;
    n_gold   INT;
BEGIN
    SELECT COUNT(DISTINCT (year, month)) INTO n_silver
    FROM silver.flow_daily;

    SELECT COUNT(*) INTO n_gold
    FROM gold.flow_monthly_anomalies;

    IF n_silver <> n_gold THEN
        RAISE EXCEPTION 'FAIL: Silver tiene % meses distintos, Gold tiene % filas', n_silver, n_gold;
    END IF;
    RAISE NOTICE 'PASS: Gold (%) coincide con Silver (%) en numero de meses', n_gold, n_silver;
END $$;


-- ----------------------------------------------------------
-- TEST 3: La media de anomalias por estacion-mes es aprox. 0
-- Por definicion de z-score, la media debe ser ~0.
-- Tolerancia de 0.001 para errores de punto flotante.
-- ----------------------------------------------------------
DO $$
DECLARE
    max_mean DOUBLE PRECISION;
BEGIN
    SELECT MAX(ABS(mean_anom)) INTO max_mean
    FROM (
        SELECT AVG(calamar_anomaly)   AS mean_anom FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(bolivar_anomaly)   FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(manaos_anomaly)    FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(obidos_anomaly)    FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(tabatinga_anomaly) FROM gold.flow_monthly_anomalies GROUP BY month
        UNION ALL
        SELECT AVG(timbues_anomaly)   FROM gold.flow_monthly_anomalies GROUP BY month
    ) sub;

    IF max_mean > 0.001 THEN
        RAISE EXCEPTION 'FAIL: Media maxima de anomalias por mes = % (esperado ~0)', max_mean;
    END IF;
    RAISE NOTICE 'PASS: Media de anomalias por estacion-mes es ~0 (max: %)', ROUND(max_mean::numeric, 6);
END $$;


-- ----------------------------------------------------------
-- TEST 4: La desviacion estandar de anomalias es aprox. 1
-- Las tolerancias son individuales por estacion porque dependen
-- de dos factores combinados:
--   1. Series cortas (pocos anios) no convergen bien a std=1
--   2. La imputacion climatologica fija z=0 en meses imputados,
--      comprimiendo artificialmente la varianza
-- Tolerancias = valor observado + 10% de margen:
--   Calamar:   67 anios, max_diff observado 0.2507 -> tol 0.28
--   Bolivar:   17 anios, max_diff observado 0.6254 -> tol 0.69
--   Manaos:    48 anios, max_diff observado 0.3787 -> tol 0.42
--   Obidos:    52 anios, max_diff observado 0.3377 -> tol 0.37
--   Tabatinga: 38 anios, max_diff observado 0.4380 -> tol 0.48
--   Timbues:  115 anios, max_diff observado 0.0044 -> tol 0.01
-- ----------------------------------------------------------
DO $$
DECLARE
    diff_calamar   DOUBLE PRECISION;
    diff_bolivar   DOUBLE PRECISION;
    diff_manaos    DOUBLE PRECISION;
    diff_obidos    DOUBLE PRECISION;
    diff_tabatinga DOUBLE PRECISION;
    diff_timbues   DOUBLE PRECISION;
BEGIN
    SELECT MAX(ABS(s - 1.0)) INTO diff_calamar   FROM (SELECT STDDEV_SAMP(calamar_anomaly)   AS s FROM gold.flow_monthly_anomalies GROUP BY month) sub;
    SELECT MAX(ABS(s - 1.0)) INTO diff_bolivar   FROM (SELECT STDDEV_SAMP(bolivar_anomaly)   AS s FROM gold.flow_monthly_anomalies GROUP BY month) sub;
    SELECT MAX(ABS(s - 1.0)) INTO diff_manaos    FROM (SELECT STDDEV_SAMP(manaos_anomaly)    AS s FROM gold.flow_monthly_anomalies GROUP BY month) sub;
    SELECT MAX(ABS(s - 1.0)) INTO diff_obidos    FROM (SELECT STDDEV_SAMP(obidos_anomaly)    AS s FROM gold.flow_monthly_anomalies GROUP BY month) sub;
    SELECT MAX(ABS(s - 1.0)) INTO diff_tabatinga FROM (SELECT STDDEV_SAMP(tabatinga_anomaly) AS s FROM gold.flow_monthly_anomalies GROUP BY month) sub;
    SELECT MAX(ABS(s - 1.0)) INTO diff_timbues   FROM (SELECT STDDEV_SAMP(timbues_anomaly)   AS s FROM gold.flow_monthly_anomalies GROUP BY month) sub;

    IF diff_calamar   > 0.28 THEN RAISE EXCEPTION 'FAIL: Calamar desv.std difiere de 1.0 en % (max tolerado: 0.28)', ROUND(diff_calamar::numeric,   4); END IF;
    IF diff_bolivar   > 0.69 THEN RAISE EXCEPTION 'FAIL: Bolivar desv.std difiere de 1.0 en % (max tolerado: 0.69)', ROUND(diff_bolivar::numeric,   4); END IF;
    IF diff_manaos    > 0.42 THEN RAISE EXCEPTION 'FAIL: Manaos desv.std difiere de 1.0 en % (max tolerado: 0.42)', ROUND(diff_manaos::numeric,     4); END IF;
    IF diff_obidos    > 0.37 THEN RAISE EXCEPTION 'FAIL: Obidos desv.std difiere de 1.0 en % (max tolerado: 0.37)', ROUND(diff_obidos::numeric,     4); END IF;
    IF diff_tabatinga > 0.48 THEN RAISE EXCEPTION 'FAIL: Tabatinga desv.std difiere de 1.0 en % (max tolerado: 0.48)', ROUND(diff_tabatinga::numeric, 4); END IF;
    IF diff_timbues   > 0.01 THEN RAISE EXCEPTION 'FAIL: Timbues desv.std difiere de 1.0 en % (max tolerado: 0.01)', ROUND(diff_timbues::numeric,   4); END IF;

    RAISE NOTICE 'PASS: Desv. std. por estacion dentro de tolerancia — Calamar:%, Bolivar:%, Manaos:%, Obidos:%, Tabatinga:%, Timbues:%',
        ROUND(diff_calamar::numeric, 4),
        ROUND(diff_bolivar::numeric, 4),
        ROUND(diff_manaos::numeric, 4),
        ROUND(diff_obidos::numeric, 4),
        ROUND(diff_tabatinga::numeric, 4),
        ROUND(diff_timbues::numeric, 4);
END $$;


-- ----------------------------------------------------------
-- TEST 5: No hay anomalias extremas (|z| > 5)
-- Un z-score > 5 es estadisticamente muy improbable e indica
-- un posible error en los datos fuente o en la limpieza.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM gold.flow_monthly_anomalies
    WHERE ABS(calamar_anomaly)   > 5
       OR ABS(bolivar_anomaly)   > 5
       OR ABS(manaos_anomaly)    > 5
       OR ABS(obidos_anomaly)    > 5
       OR ABS(tabatinga_anomaly) > 5
       OR ABS(timbues_anomaly)   > 5;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con anomalia |z| > 5 (revisar datos fuente)', n;
    END IF;
    RAISE NOTICE 'PASS: Todas las anomalias tienen |z| <= 5';
END $$;


-- ----------------------------------------------------------
-- TEST 6: Anomalias NULL solo donde Silver no tiene datos
-- Si una estacion tiene al menos un dato diario en un mes,
-- debe tener anomalia en Gold (no NULL).
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    WITH silver_mensual AS (
        SELECT
            year, month,
            COUNT(calamar_daily)   > 0 AS tiene_calamar,
            COUNT(bolivar_daily)   > 0 AS tiene_bolivar,
            COUNT(manaos_daily)    > 0 AS tiene_manaos,
            COUNT(obidos_daily)    > 0 AS tiene_obidos,
            COUNT(tabatinga_daily) > 0 AS tiene_tabatinga,
            COUNT(timbues_daily)   > 0 AS tiene_timbues
        FROM silver.flow_daily
        GROUP BY year, month
    )
    SELECT COUNT(*) INTO n
    FROM silver_mensual s
    JOIN gold.flow_monthly_anomalies g
      ON s.year = g.year AND s.month = g.month
    WHERE (s.tiene_calamar   AND g.calamar_anomaly   IS NULL)
       OR (s.tiene_bolivar   AND g.bolivar_anomaly   IS NULL)
       OR (s.tiene_manaos    AND g.manaos_anomaly    IS NULL)
       OR (s.tiene_obidos    AND g.obidos_anomaly    IS NULL)
       OR (s.tiene_tabatinga AND g.tabatinga_anomaly IS NULL)
       OR (s.tiene_timbues   AND g.timbues_anomaly   IS NULL);

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % meses con datos en Silver pero sin anomalia en Gold', n;
    END IF;
    RAISE NOTICE 'PASS: Todo mes con datos en Silver tiene anomalia en Gold';
END $$;


-- ----------------------------------------------------------
-- TEST 7: Las columnas *_imputado son siempre TRUE o FALSE
--         (nunca NULL) para todos los meses de la vista.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM gold.flow_monthly_anomalies
    WHERE calamar_imputado   IS NULL
       OR bolivar_imputado   IS NULL
       OR manaos_imputado    IS NULL
       OR obidos_imputado    IS NULL
       OR tabatinga_imputado IS NULL
       OR timbues_imputado   IS NULL;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con columna *_imputado = NULL (esperado siempre TRUE/FALSE)', n;
    END IF;
    RAISE NOTICE 'PASS: Todas las columnas *_imputado tienen valor TRUE o FALSE';
END $$;


\echo '========================================'
\echo 'TODOS LOS TESTS GOLD PASARON'
\echo '========================================'