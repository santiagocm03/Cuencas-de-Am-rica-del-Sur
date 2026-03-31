-- ==========================================================
-- 03_ddl.sql  (Silver)
-- ==========================================================
-- Proposito : Crear la tabla de caudales diarios limpios
--             y armonizados. Consolida las 6 estaciones en
--             formato wide (una columna por estacion).
-- Dependencias: 00_init_schemas.sql
-- Ejecucion :
--   sudo -u postgres psql -d caudales -f sql/silver/03_ddl.sql
-- Notas:
--   - Valores en m3/s (metros cubicos por segundo).
--   - Granularidad diaria: una fila por (year, month, day).
--   - NULL indica ausencia de observacion para esa estacion-dia.
--   - La agregacion mensual, imputacion y z-scores pertenecen
--     a la capa Gold.
-- ==========================================================

\set ON_ERROR_STOP on

DROP TABLE IF EXISTS silver.flow_daily;

CREATE TABLE silver.flow_daily (
    year  INT NOT NULL,
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day   INT NOT NULL CHECK (day   BETWEEN 1 AND 31),

    calamar_daily    DOUBLE PRECISION,  -- Magdalena en Calamar (Colombia)
    bolivar_daily    DOUBLE PRECISION,  -- Orinoco en Ciudad Bolivar (Venezuela)
    manaos_daily     DOUBLE PRECISION,  -- Negro/Amazonas en Manaos (Brasil)
    obidos_daily     DOUBLE PRECISION,  -- Amazonas en Obidos (Brasil)
    tabatinga_daily  DOUBLE PRECISION,  -- Amazonas en Tabatinga (Brasil)
    timbues_daily    DOUBLE PRECISION,  -- Parana en Timbues (Argentina)

    CONSTRAINT pk_flow_daily PRIMARY KEY (year, month, day)
);

-- ---- Metadatos de la tabla ----
COMMENT ON TABLE silver.flow_daily IS
    'Caudales diarios (m3/s) por estacion, limpios. NULL indica dia sin observacion. La agregacion mensual pertenece a Gold.';

COMMENT ON COLUMN silver.flow_daily.year            IS 'Anio de la observacion';
COMMENT ON COLUMN silver.flow_daily.month           IS 'Mes de la observacion (1-12)';
COMMENT ON COLUMN silver.flow_daily.day             IS 'Dia de la observacion (1-31)';
COMMENT ON COLUMN silver.flow_daily.calamar_daily   IS 'Caudal diario en Calamar - Rio Magdalena (m3/s)';
COMMENT ON COLUMN silver.flow_daily.bolivar_daily   IS 'Caudal diario en Ciudad Bolivar - Rio Orinoco (m3/s)';
COMMENT ON COLUMN silver.flow_daily.manaos_daily    IS 'Caudal diario en Manaos - Rio Negro/Amazonas (m3/s)';
COMMENT ON COLUMN silver.flow_daily.obidos_daily    IS 'Caudal diario en Obidos - Rio Amazonas (m3/s)';
COMMENT ON COLUMN silver.flow_daily.tabatinga_daily IS 'Caudal diario en Tabatinga - Rio Amazonas (m3/s)';
COMMENT ON COLUMN silver.flow_daily.timbues_daily   IS 'Caudal diario en Timbues - Rio Parana (m3/s). Fuente ya viene en frecuencia mensual: day=1 por convencion.';