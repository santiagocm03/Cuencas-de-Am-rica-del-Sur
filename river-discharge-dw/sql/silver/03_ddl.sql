-- ==========================================================
-- 03_ddl.sql  (Silver)
-- ==========================================================
-- Proposito : Crear una tabla limpia por estacion.
--             Cada tabla replica las columnas del CSV original
--             pero con tipos de dato correctos (no TEXT).
--             La union de estaciones y agregacion mensual
--             ocurre en Gold.
-- Dependencias: 00_init_schemas.sql
-- Ejecucion :
--   sudo -u postgres psql -d postgres -f sql/silver/03_ddl.sql
-- ==========================================================

\set ON_ERROR_STOP on

-- ==========================================================
-- CALAMAR (Rio Magdalena, Colombia)
-- Fuente: IDEAM. 21 columnas. Frecuencia diaria.
-- ==========================================================
DROP TABLE IF EXISTS silver.calamar;
CREATE TABLE silver.calamar (
    codigo_estacion    TEXT,
    nombre_estacion    TEXT,
    latitud            DOUBLE PRECISION,
    longitud           DOUBLE PRECISION,
    altitud            DOUBLE PRECISION,
    categoria          TEXT,
    entidad            TEXT,
    area_operativa     TEXT,
    departamento       TEXT,
    municipio          TEXT,
    fecha_instalacion  TIMESTAMP,
    fecha_suspension   TIMESTAMP,
    id_parametro       TEXT,
    etiqueta           TEXT,
    descripcion_serie  TEXT,
    frecuencia         TEXT,
    fecha              DATE,
    valor              DOUBLE PRECISION,
    grado              INTEGER,
    calificador        TEXT,
    nivel_aprobacion   INTEGER
);

COMMENT ON TABLE silver.calamar IS
    'Caudal diario limpio en Calamar - Rio Magdalena (m3/s). Fuente: IDEAM Colombia.';

-- ==========================================================
-- CIUDAD BOLIVAR (Rio Orinoco, Venezuela)
-- Fuente: GRDC / BNO. 6 columnas. Frecuencia diaria.
-- ==========================================================
DROP TABLE IF EXISTS silver.ciudad_bolivar;
CREATE TABLE silver.ciudad_bolivar (
    id_station  TEXT,
    nom         TEXT,
    fecha       TIMESTAMP,
    valor       DOUBLE PRECISION,
    origine     TEXT,
    qualite     TEXT
);

COMMENT ON TABLE silver.ciudad_bolivar IS
    'Caudal diario limpio en Ciudad Bolivar - Rio Orinoco (m3/s). Fuente: GRDC / BNO Venezuela.';

-- ==========================================================
-- MANAOS (Rio Negro / Amazonas, Brasil)
-- Fuente: ANA Brasil / GRDC. 6 columnas. Frecuencia diaria.
-- ==========================================================
DROP TABLE IF EXISTS silver.manaos;
CREATE TABLE silver.manaos (
    id_station  TEXT,
    nom         TEXT,
    fecha       TIMESTAMP,
    valor       DOUBLE PRECISION,
    origine     TEXT,
    qualite     TEXT
);

COMMENT ON TABLE silver.manaos IS
    'Caudal diario limpio en Manaos - Rio Negro/Amazonas (m3/s). Fuente: ANA Brasil / GRDC.';

-- ==========================================================
-- OBIDOS (Rio Amazonas, Brasil)
-- Fuente: ANA Brasil / GRDC. 5 columnas. Frecuencia diaria.
-- ==========================================================
DROP TABLE IF EXISTS silver.obidos;
CREATE TABLE silver.obidos (
    id_station  TEXT,
    nom         TEXT,
    fecha       TIMESTAMP,
    valor       DOUBLE PRECISION,
    origine     TEXT
);

COMMENT ON TABLE silver.obidos IS
    'Caudal diario limpio en Obidos - Rio Amazonas (m3/s). Fuente: ANA Brasil / GRDC.';

-- ==========================================================
-- TABATINGA (Rio Amazonas, Brasil)
-- Fuente: ANA Brasil / GRDC. 7 columnas. Frecuencia diaria.
-- Campo 7 (valor_medio_mensual): caudal medio mensual
-- precalculado en la fuente, se conserva como referencia.
-- ==========================================================
DROP TABLE IF EXISTS silver.tabatinga;
CREATE TABLE silver.tabatinga (
    id_station           TEXT,
    nom                  TEXT,
    fecha                TIMESTAMP,
    valor                DOUBLE PRECISION,
    origine              TEXT,
    qualite              TEXT,
    valor_medio_mensual  DOUBLE PRECISION
);

COMMENT ON TABLE silver.tabatinga IS
    'Caudal diario limpio en Tabatinga - Rio Amazonas (m3/s). Fuente: ANA Brasil / GRDC.';

-- ==========================================================
-- TIMBUES (Rio Parana, Argentina)
-- Fuente: INA Argentina. 2 columnas. Frecuencia mensual.
-- ==========================================================
DROP TABLE IF EXISTS silver.timbues;
CREATE TABLE silver.timbues (
    fecha  DATE,
    valor  DOUBLE PRECISION
);

COMMENT ON TABLE silver.timbues IS
    'Caudal mensual limpio en Timbues - Rio Parana (m3/s). Fuente: INA Argentina. Frecuencia mensual.';