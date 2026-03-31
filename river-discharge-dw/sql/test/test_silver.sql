-- ==========================================================
-- test_silver.sql
-- ==========================================================
-- Proposito : Validar la integridad y calidad de los datos
--             en silver.flow_daily despues de ejecutar el
--             pipeline Bronze -> Silver.
-- Ejecucion :
--   sudo -u postgres psql -d caudales -f sql/test/test_silver.sql 

--
--   Si algun test falla, el script se detiene con un error
--   EXCEPTION. Si todos pasan, imprime NOTICE con 'PASS'.
-- ==========================================================

\set ON_ERROR_STOP on
SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'TESTS - CAPA SILVER'
\echo '========================================'


-- ----------------------------------------------------------
-- TEST 1: La tabla no esta vacia
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n FROM silver.flow_daily;
    IF n = 0 THEN
        RAISE EXCEPTION 'FAIL: silver.flow_daily esta vacia (0 filas)';
    END IF;
    RAISE NOTICE 'PASS: silver.flow_daily tiene % filas', n;
END $$;


-- ----------------------------------------------------------
-- TEST 2: No hay duplicados en la clave primaria (year, month, day)
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM (
        SELECT year, month, day
        FROM silver.flow_daily
        GROUP BY year, month, day
        HAVING COUNT(*) > 1
    ) dups;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % combinaciones (year, month, day) duplicadas', n;
    END IF;
    RAISE NOTICE 'PASS: No hay duplicados en (year, month, day)';
END $$;


-- ----------------------------------------------------------
-- TEST 3: Todos los meses estan entre 1 y 12
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_daily
    WHERE month < 1 OR month > 12;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con mes fuera de rango [1,12]', n;
    END IF;
    RAISE NOTICE 'PASS: Todos los meses estan entre 1 y 12';
END $$;


-- ----------------------------------------------------------
-- TEST 4: Todos los dias estan entre 1 y 31
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_daily
    WHERE day < 1 OR day > 31;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con dia fuera de rango [1,31]', n;
    END IF;
    RAISE NOTICE 'PASS: Todos los dias estan entre 1 y 31';
END $$;


-- ----------------------------------------------------------
-- TEST 5: No hay valores negativos de caudal
-- Un caudal negativo es fisicamente imposible.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_daily
    WHERE calamar_daily   < 0
       OR bolivar_daily   < 0
       OR manaos_daily    < 0
       OR obidos_daily    < 0
       OR tabatinga_daily < 0
       OR timbues_daily   < 0;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con caudal negativo', n;
    END IF;
    RAISE NOTICE 'PASS: No hay valores negativos de caudal';
END $$;


-- ----------------------------------------------------------
-- TEST 6: Rangos fisicos razonables por estacion
-- Los umbrales superiores son aproximaciones del caudal
-- historico maximo diario conocido por estacion.
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_daily
    WHERE calamar_daily   > 16000
       OR bolivar_daily   > 100000
       OR manaos_daily    > 200000
       OR obidos_daily    > 300000
       OR tabatinga_daily > 80000
       OR timbues_daily   > 30000;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: % filas con caudal fuera de rango fisico', n;
    END IF;
    RAISE NOTICE 'PASS: Todos los caudales dentro de rangos fisicos razonables';
END $$;


-- ----------------------------------------------------------
-- TEST 7: Cada estacion tiene un minimo de registros diarios
-- Umbrales basados en los rangos temporales conocidos.
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
    SELECT
        COUNT(calamar_daily),
        COUNT(bolivar_daily),
        COUNT(manaos_daily),
        COUNT(obidos_daily),
        COUNT(tabatinga_daily),
        COUNT(timbues_daily)
    INTO n_calamar, n_bolivar, n_manaos, n_obidos, n_tabatinga, n_timbues
    FROM silver.flow_daily;

    IF n_calamar < 10000 THEN
        RAISE EXCEPTION 'FAIL: Calamar tiene solo % registros diarios (esperado >=10000)', n_calamar;
    END IF;
    IF n_bolivar < 1000 THEN
        RAISE EXCEPTION 'FAIL: Bolivar tiene solo % registros diarios (esperado >=1000)', n_bolivar;
    END IF;
    IF n_manaos < 1000 THEN
        RAISE EXCEPTION 'FAIL: Manaos tiene solo % registros diarios (esperado >=1000)', n_manaos;
    END IF;
    IF n_obidos < 1000 THEN
        RAISE EXCEPTION 'FAIL: Obidos tiene solo % registros diarios (esperado >=1000)', n_obidos;
    END IF;
    IF n_tabatinga < 1000 THEN
        RAISE EXCEPTION 'FAIL: Tabatinga tiene solo % registros diarios (esperado >=1000)', n_tabatinga;
    END IF;
    IF n_timbues < 50 THEN
        RAISE EXCEPTION 'FAIL: Timbues tiene solo % registros mensuales (esperado >=50)', n_timbues;
    END IF;

    RAISE NOTICE 'PASS: Conteos por estacion — Calamar:%, Bolivar:%, Manaos:%, Obidos:%, Tabatinga:%, Timbues:%',
        n_calamar, n_bolivar, n_manaos, n_obidos, n_tabatinga, n_timbues;
END $$;


-- ----------------------------------------------------------
-- TEST 8: Anios dentro de un rango historico razonable
-- ----------------------------------------------------------
DO $$
DECLARE
    min_year INT;
    max_year INT;
BEGIN
    SELECT MIN(year), MAX(year) INTO min_year, max_year
    FROM silver.flow_daily;

    IF min_year < 1900 THEN
        RAISE EXCEPTION 'FAIL: Anio minimo % es anterior a 1900', min_year;
    END IF;
    IF max_year > 2030 THEN
        RAISE EXCEPTION 'FAIL: Anio maximo % es posterior a 2030', max_year;
    END IF;
    RAISE NOTICE 'PASS: Rango temporal [%, %] es razonable', min_year, max_year;
END $$;


-- ----------------------------------------------------------
-- TEST 9: Timbues solo tiene day=1 (fuente mensual por convencion)
-- ----------------------------------------------------------
DO $$
DECLARE
    n INT;
BEGIN
    SELECT COUNT(*) INTO n
    FROM silver.flow_daily
    WHERE timbues_daily IS NOT NULL
      AND day <> 1;

    IF n > 0 THEN
        RAISE EXCEPTION 'FAIL: Timbues tiene % filas con day != 1 (esperado siempre day=1)', n;
    END IF;
    RAISE NOTICE 'PASS: Timbues tiene siempre day=1 (convencion fuente mensual)';
END $$;


\echo '========================================'
\echo 'TODOS LOS TESTS SILVER PASARON'
\echo '========================================'