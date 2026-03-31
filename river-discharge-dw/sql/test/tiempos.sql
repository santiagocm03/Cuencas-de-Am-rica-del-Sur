-- ==========================================================
-- timing_benchmark.sql
-- ==========================================================
-- Propósito : Medir el tiempo de ejecución de las etapas
--             principales del pipeline.
-- Ejecución :
--   psql -d caudales -f sql/timing_benchmark.sql
-- ==========================================================

\echo '========================================'
\echo 'BENCHMARK DE TIEMPOS - CAUDALES DW'
\echo '========================================'

\timing on

-- 1. Carga bronze
\echo '--- Carga Bronze ---'
\i sql/bronze/02_load_insert.sql

-- 2. Transformación silver
\echo '--- Transformación Silver ---'
\i sql/silver/04_insert.sql

-- 3. Construcción gold (tabla mensual y vista)
\echo '--- Construcción Gold ---'
\i sql/gold/05_view_anomalies.sql

\timing off

\echo '========================================'
\echo 'BENCHMARK COMPLETADO'
\echo '========================================'