-- ==========================================================
-- test_gold.sql
-- ==========================================================
-- Propósito : Validar la tabla gold.flow_monthly y la vista
--             gold.flow_monthly_anomalies.
-- Dependencias: gold.flow_monthly, gold.flow_monthly_anomalies
-- Ejecución :
--   psudo -u postgres psql -d postgres -f sql/test/test_silver.sql
-- ==========================================================

\set ON_ERROR_STOP on
SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'TESTS - CAPA GOLD'
\echo '========================================'


-- ----------------------------------------------------------
-- TEST 1: gold.flow_monthly no está vacía
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM gold.flow_monthly;
    IF n = 0 THEN
        RAISE EXCEPTION 'FAIL: gold.flow_monthly está vacía';
    END IF;
    RAISE NOTICE 'PASS: gold.flow_monthly tiene % filas', n;
END $$;


-- ----------------------------------------------------------
-- TEST 2: gold.flow_monthly_anomalies no está vacía
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM gold.flow_monthly_anomalies;
    IF n = 0 THEN
        RAISE EXCEPTION 'FAIL: gold.flow_monthly_anomalies está vacía';
    END IF;
    RAISE NOTICE 'PASS: gold.flow_monthly_anomalies tiene % filas', n;
END $$;


-- ----------------------------------------------------------
-- TEST 3: Cantidad de meses en gold.flow_monthly coincide con
--         los meses presentes en las tablas silver (suma de años-mes únicos)
-- ----------------------------------------------------------
DO $$
DECLARE
    n_gold INT;
    n_silver INT;
BEGIN
    SELECT COUNT(*) INTO n_gold FROM gold.flow_monthly;

    WITH silver_months AS (
        SELECT EXTRACT(YEAR FROM fecha)::int AS y, EXTRACT(MONTH FROM fecha)::int AS m FROM silver.calamar
        UNION
        SELECT EXTRACT(YEAR FROM fecha)::int, EXTRACT(MONTH FROM fecha)::int FROM silver.ciudad_bolivar
        UNION
        SELECT EXTRACT(YEAR FROM fecha)::int, EXTRACT(MONTH FROM fecha)::int FROM silver.manaos
        UNION
        SELECT EXTRACT(YEAR FROM fecha)::int, EXTRACT(MONTH FROM fecha)::int FROM silver.obidos
        UNION
        SELECT EXTRACT(YEAR FROM fecha)::int, EXTRACT(MONTH FROM fecha)::int FROM silver.tabatinga
        UNION
        SELECT EXTRACT(YEAR FROM fecha)::int, EXTRACT(MONTH FROM fecha)::int FROM silver.timbues
    )
    SELECT COUNT(*) INTO n_silver FROM silver_months;

    IF n_gold != n_silver THEN
        RAISE EXCEPTION 'FAIL: gold.flow_monthly tiene % meses, pero Silver tiene % meses distintos', n_gold, n_silver;
    END IF;
    RAISE NOTICE 'PASS: gold.flow_monthly tiene % meses, coincidiendo con Silver', n_gold;
END $$;


-- ----------------------------------------------------------
-- TEST 4: Media de anomalías por estación-mes ≈ 0 (tolerancia 1e-6)
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

    IF max_mean > 1e-6 THEN
        RAISE EXCEPTION 'FAIL: Media máxima de anomalías por mes = % (esperado ~0)', max_mean;
    END IF;
    RAISE NOTICE 'PASS: Media de anomalías por estación-mes es ~0 (max: %)', ROUND(max_mean::numeric, 12);
END $$;


-- ----------------------------------------------------------
-- TEST 5: Desviación estándar de anomalías por estación-mes ≈ 1
--         (tolerancias ajustadas por longitud de serie e imputación)
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

    RAISE NOTICE 'PASS: Desv. std. por estación dentro de tolerancia — Calamar:%, Bolivar:%, Manaos:%, Obidos:%, Tabatinga:%, Timbues:%',
        ROUND(diff_calamar::numeric, 4),
        ROUND(diff_bolivar::numeric, 4),
        ROUND(diff_manaos::numeric, 4),
        ROUND(diff_obidos::numeric, 4),
        ROUND(diff_tabatinga::numeric, 4),
        ROUND(diff_timbues::numeric, 4);
END $$;


-- ----------------------------------------------------------
-- TEST 6: No hay anomalías extremas (|z| > 5)
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
        RAISE EXCEPTION 'FAIL: % filas con anomalía |z| > 5 (revisar datos fuente)', n;
    END IF;
    RAISE NOTICE 'PASS: Todas las anomalías tienen |z| <= 5';
END $$;


-- ----------------------------------------------------------
-- TEST 7: Columnas *_imputado nunca son NULL
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


-- ----------------------------------------------------------
-- TEST 8: Los valores imputados deben tener anomalía = 0
--         (porque se imputa con la climatología)
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM gold.flow_monthly_anomalies
    WHERE (calamar_imputado   = TRUE AND calamar_anomaly   != 0)
       OR (bolivar_imputado   = TRUE AND bolivar_anomaly   != 0)
       OR (manaos_imputado    = TRUE AND manaos_anomaly    != 0)
       OR (obidos_imputado    = TRUE AND obidos_anomaly    != 0)
       OR (tabatinga_imputado = TRUE AND tabatinga_anomaly != 0)
       OR (timbues_imputado   = TRUE AND timbues_anomaly   != 0);

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con imputado = TRUE pero anomalía != 0', n;
    END IF;
    RAISE NOTICE 'PASS: Todos los valores imputados tienen anomalía = 0';
END $$;


\echo '========================================'
\echo 'TODOS LOS TESTS GOLD PASARON'
\echo '========================================'