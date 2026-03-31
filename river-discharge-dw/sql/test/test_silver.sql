-- ==========================================================
-- test_silver.sql
-- ==========================================================
-- Propósito : Validar la integridad y calidad de los datos
--             en las tablas silver por estación.
-- Dependencias: silver.calamar, silver.ciudad_bolivar,
--               silver.manaos, silver.obidos,
--               silver.tabatinga, silver.timbues
-- Ejecución :
--   sudo -u postgres psql -d postgres -f sql/test/test_silver.sql 
-- ==========================================================

\set ON_ERROR_STOP on
SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'TESTS - CAPA SILVER (TABLAS POR ESTACIÓN)'
\echo '========================================'


-- ----------------------------------------------------------
-- TEST 1: Todas las tablas tienen datos
-- ----------------------------------------------------------
DO $$
DECLARE
    n_calamar   INT;
    n_bolivar   INT;
    n_manaos    INT;
    n_obidos    INT;
    n_tabatinga INT;
    n_timbues   INT;
BEGIN
    SELECT COUNT(*) INTO n_calamar   FROM silver.calamar;
    SELECT COUNT(*) INTO n_bolivar   FROM silver.ciudad_bolivar;
    SELECT COUNT(*) INTO n_manaos    FROM silver.manaos;
    SELECT COUNT(*) INTO n_obidos    FROM silver.obidos;
    SELECT COUNT(*) INTO n_tabatinga FROM silver.tabatinga;
    SELECT COUNT(*) INTO n_timbues   FROM silver.timbues;

    IF n_calamar = 0 THEN RAISE EXCEPTION 'FAIL: silver.calamar está vacía'; END IF;
    IF n_bolivar = 0 THEN RAISE EXCEPTION 'FAIL: silver.ciudad_bolivar está vacía'; END IF;
    IF n_manaos = 0 THEN RAISE EXCEPTION 'FAIL: silver.manaos está vacía'; END IF;
    IF n_obidos = 0 THEN RAISE EXCEPTION 'FAIL: silver.obidos está vacía'; END IF;
    IF n_tabatinga = 0 THEN RAISE EXCEPTION 'FAIL: silver.tabatinga está vacía'; END IF;
    IF n_timbues = 0 THEN RAISE EXCEPTION 'FAIL: silver.timbues está vacía'; END IF;

    RAISE NOTICE 'PASS: Todas las tablas tienen datos — Calamar:%, Bolivar:%, Manaos:%, Obidos:%, Tabatinga:%, Timbues:%',
        n_calamar, n_bolivar, n_manaos, n_obidos, n_tabatinga, n_timbues;
END $$;


-- ----------------------------------------------------------
-- TEST 2: Valores de caudal positivos
-- ----------------------------------------------------------
DO $$
DECLARE
    n_calamar   INT;
    n_bolivar   INT;
    n_manaos    INT;
    n_obidos    INT;
    n_tabatinga INT;
    n_timbues   INT;
BEGIN
    SELECT COUNT(*) INTO n_calamar   FROM silver.calamar   WHERE valor <= 0;
    SELECT COUNT(*) INTO n_bolivar   FROM silver.ciudad_bolivar WHERE valor <= 0;
    SELECT COUNT(*) INTO n_manaos    FROM silver.manaos    WHERE valor <= 0;
    SELECT COUNT(*) INTO n_obidos    FROM silver.obidos    WHERE valor <= 0;
    SELECT COUNT(*) INTO n_tabatinga FROM silver.tabatinga WHERE valor <= 0;
    SELECT COUNT(*) INTO n_timbues   FROM silver.timbues   WHERE valor <= 0;

    IF n_calamar   > 0 THEN RAISE EXCEPTION 'FAIL: Calamar tiene % filas con valor <= 0', n_calamar;   END IF;
    IF n_bolivar   > 0 THEN RAISE EXCEPTION 'FAIL: Bolivar tiene % filas con valor <= 0', n_bolivar;   END IF;
    IF n_manaos    > 0 THEN RAISE EXCEPTION 'FAIL: Manaos tiene % filas con valor <= 0', n_manaos;     END IF;
    IF n_obidos    > 0 THEN RAISE EXCEPTION 'FAIL: Obidos tiene % filas con valor <= 0', n_obidos;     END IF;
    IF n_tabatinga > 0 THEN RAISE EXCEPTION 'FAIL: Tabatinga tiene % filas con valor <= 0', n_tabatinga; END IF;
    IF n_timbues   > 0 THEN RAISE EXCEPTION 'FAIL: Timbues tiene % filas con valor <= 0', n_timbues;   END IF;

    RAISE NOTICE 'PASS: Todos los caudales son positivos';
END $$;


-- ----------------------------------------------------------
-- TEST 3: Rangos físicos máximos por estación
-- ----------------------------------------------------------
DO $$
DECLARE
    n_calamar   INT;
    n_bolivar   INT;
    n_manaos    INT;
    n_obidos    INT;
    n_tabatinga INT;
    n_timbues   INT;
BEGIN
    SELECT COUNT(*) INTO n_calamar   FROM silver.calamar   WHERE valor > 16000;
    SELECT COUNT(*) INTO n_bolivar   FROM silver.ciudad_bolivar WHERE valor > 100000;
    SELECT COUNT(*) INTO n_manaos    FROM silver.manaos    WHERE valor > 200000;
    SELECT COUNT(*) INTO n_obidos    FROM silver.obidos    WHERE valor > 300000;
    SELECT COUNT(*) INTO n_tabatinga FROM silver.tabatinga WHERE valor > 80000;
    SELECT COUNT(*) INTO n_timbues   FROM silver.timbues   WHERE valor > 30000;

    IF n_calamar   > 0 THEN RAISE EXCEPTION 'FAIL: Calamar tiene % filas con caudal > 16000', n_calamar;   END IF;
    IF n_bolivar   > 0 THEN RAISE EXCEPTION 'FAIL: Bolivar tiene % filas con caudal > 100000', n_bolivar; END IF;
    IF n_manaos    > 0 THEN RAISE EXCEPTION 'FAIL: Manaos tiene % filas con caudal > 200000', n_manaos;   END IF;
    IF n_obidos    > 0 THEN RAISE EXCEPTION 'FAIL: Obidos tiene % filas con caudal > 300000', n_obidos;   END IF;
    IF n_tabatinga > 0 THEN RAISE EXCEPTION 'FAIL: Tabatinga tiene % filas con caudal > 80000', n_tabatinga; END IF;
    IF n_timbues   > 0 THEN RAISE EXCEPTION 'FAIL: Timbues tiene % filas con caudal > 30000', n_timbues;   END IF;

    RAISE NOTICE 'PASS: Todos los caudales dentro de rangos físicos razonables';
END $$;


-- ----------------------------------------------------------
-- TEST 4: Fechas válidas y dentro de rango histórico (1900-2030)
-- ----------------------------------------------------------
DO $$
DECLARE
    min_year_c INT; max_year_c INT;
    min_year_b INT; max_year_b INT;
    min_year_m INT; max_year_m INT;
    min_year_o INT; max_year_o INT;
    min_year_t INT; max_year_t INT;
    min_year_tm INT; max_year_tm INT;
BEGIN
    SELECT MIN(EXTRACT(YEAR FROM fecha)), MAX(EXTRACT(YEAR FROM fecha)) INTO min_year_c, max_year_c FROM silver.calamar;
    SELECT MIN(EXTRACT(YEAR FROM fecha)), MAX(EXTRACT(YEAR FROM fecha)) INTO min_year_b, max_year_b FROM silver.ciudad_bolivar;
    SELECT MIN(EXTRACT(YEAR FROM fecha)), MAX(EXTRACT(YEAR FROM fecha)) INTO min_year_m, max_year_m FROM silver.manaos;
    SELECT MIN(EXTRACT(YEAR FROM fecha)), MAX(EXTRACT(YEAR FROM fecha)) INTO min_year_o, max_year_o FROM silver.obidos;
    SELECT MIN(EXTRACT(YEAR FROM fecha)), MAX(EXTRACT(YEAR FROM fecha)) INTO min_year_t, max_year_t FROM silver.tabatinga;
    SELECT MIN(EXTRACT(YEAR FROM fecha)), MAX(EXTRACT(YEAR FROM fecha)) INTO min_year_tm, max_year_tm FROM silver.timbues;

    IF min_year_c < 1900 OR max_year_c > 2030 THEN RAISE EXCEPTION 'FAIL: Calamar año fuera de [1900,2030]'; END IF;
    IF min_year_b < 1900 OR max_year_b > 2030 THEN RAISE EXCEPTION 'FAIL: Bolivar año fuera de [1900,2030]'; END IF;
    IF min_year_m < 1900 OR max_year_m > 2030 THEN RAISE EXCEPTION 'FAIL: Manaos año fuera de [1900,2030]'; END IF;
    IF min_year_o < 1900 OR max_year_o > 2030 THEN RAISE EXCEPTION 'FAIL: Obidos año fuera de [1900,2030]'; END IF;
    IF min_year_t < 1900 OR max_year_t > 2030 THEN RAISE EXCEPTION 'FAIL: Tabatinga año fuera de [1900,2030]'; END IF;
    IF min_year_tm < 1900 OR max_year_tm > 2030 THEN RAISE EXCEPTION 'FAIL: Timbues año fuera de [1900,2030]'; END IF;

    RAISE NOTICE 'PASS: Rangos temporales aceptables';
END $$;


-- ----------------------------------------------------------
-- TEST 5: Timbues: solo registros mensuales (fecha siempre primer día)
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.timbues
    WHERE EXTRACT(DAY FROM fecha) != 1;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: Timbues tiene % registros con día != 1 (se espera día 1 para datos mensuales)', n;
    END IF;
    RAISE NOTICE 'PASS: Timbues tiene todos los registros con día 1 (datos mensuales)';
END $$;


\echo '========================================'
\echo 'TODOS LOS TESTS SILVER PASARON'
\echo '========================================'