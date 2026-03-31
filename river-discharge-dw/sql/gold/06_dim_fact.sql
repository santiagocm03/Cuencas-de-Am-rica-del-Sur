-- ==========================================================
-- 06_dim_fact.sql  (Gold)
-- ==========================================================
-- Propósito : Construir un modelo dimensional (estrella)
--             a partir de los datos de gold.flow_monthly
--             y gold.flow_monthly_anomalies.
-- ==========================================================

\set ON_ERROR_STOP on
SET client_min_messages = NOTICE;

\echo '========================================'
\echo 'CONSTRUCCIÓN MODELO DIMENSIONAL (GOLD)'
\echo '========================================'

-- Dimensión Estación
DROP TABLE IF EXISTS gold.dim_estacion CASCADE;
CREATE TABLE gold.dim_estacion (
    id_estacion SERIAL PRIMARY KEY,
    codigo      TEXT,
    nombre      TEXT NOT NULL UNIQUE,
    rio         TEXT,
    cuenca      TEXT,
    pais        TEXT,
    latitud     DOUBLE PRECISION,
    longitud    DOUBLE PRECISION,
    altitud     DOUBLE PRECISION,
    fuente      TEXT
);

INSERT INTO gold.dim_estacion (codigo, nombre, rio, cuenca, pais, latitud, longitud, altitud, fuente)
VALUES
    ('23037020', 'Calamar', 'Magdalena', 'Magdalena', 'Colombia', 10.138, -74.915, 13, 'IDEAM'),
    (NULL,       'Ciudad Bolivar', 'Orinoco', 'Orinoco', 'Venezuela', NULL, NULL, NULL, 'GRDC/BNO'),
    (NULL,       'Manaos', 'Negro/Amazonas', 'Amazonas', 'Brasil', NULL, NULL, NULL, 'ANA/GRDC'),
    (NULL,       'Obidos', 'Amazonas', 'Amazonas', 'Brasil', NULL, NULL, NULL, 'ANA/GRDC'),
    (NULL,       'Tabatinga', 'Amazonas', 'Amazonas', 'Brasil', NULL, NULL, NULL, 'ANA/GRDC'),
    (NULL,       'Timbues', 'Parana', 'Parana', 'Argentina', NULL, NULL, NULL, 'INA')
ON CONFLICT (nombre) DO NOTHING;

-- Dimensión Tiempo
DROP TABLE IF EXISTS gold.dim_tiempo CASCADE;
CREATE TABLE gold.dim_tiempo (
    id_tiempo     SERIAL PRIMARY KEY,
    anio          INT NOT NULL,
    mes           INT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    nombre_mes    TEXT,
    trimestre     INT,
    semestre      INT,
    UNIQUE (anio, mes)
);

INSERT INTO gold.dim_tiempo (anio, mes, nombre_mes, trimestre, semestre)
SELECT DISTINCT
    year,
    month,
    CASE month
        WHEN 1  THEN 'Enero' WHEN 2  THEN 'Febrero' WHEN 3  THEN 'Marzo'
        WHEN 4  THEN 'Abril' WHEN 5  THEN 'Mayo'   WHEN 6  THEN 'Junio'
        WHEN 7  THEN 'Julio' WHEN 8  THEN 'Agosto' WHEN 9  THEN 'Septiembre'
        WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'
    END,
    CASE WHEN month IN (1,2,3) THEN 1 WHEN month IN (4,5,6) THEN 2 WHEN month IN (7,8,9) THEN 3 ELSE 4 END,
    CASE WHEN month <= 6 THEN 1 ELSE 2 END
FROM gold.flow_monthly
ORDER BY year, month
ON CONFLICT (anio, mes) DO NOTHING;

-- Tabla de Hechos
DROP TABLE IF EXISTS gold.fact_caudal_mensual CASCADE;
CREATE TABLE gold.fact_caudal_mensual (
    id_fact          SERIAL PRIMARY KEY,
    id_estacion      INT NOT NULL REFERENCES gold.dim_estacion(id_estacion),
    id_tiempo        INT NOT NULL REFERENCES gold.dim_tiempo(id_tiempo),
    caudal_observado DOUBLE PRECISION,
    es_imputado      BOOLEAN NOT NULL,
    z_score          DOUBLE PRECISION
);

-- Calamar
INSERT INTO gold.fact_caudal_mensual (id_estacion, id_tiempo, caudal_observado, es_imputado, z_score)
SELECT e.id_estacion, t.id_tiempo, fm.calamar_monthly, anom.calamar_imputado, anom.calamar_anomaly
FROM gold.flow_monthly fm
JOIN gold.dim_tiempo t ON t.anio = fm.year AND t.mes = fm.month
JOIN gold.dim_estacion e ON e.nombre = 'Calamar'
LEFT JOIN gold.flow_monthly_anomalies anom ON anom.year = fm.year AND anom.month = fm.month
WHERE fm.calamar_monthly IS NOT NULL OR anom.calamar_imputado = TRUE;

-- Ciudad Bolivar
INSERT INTO gold.fact_caudal_mensual (id_estacion, id_tiempo, caudal_observado, es_imputado, z_score)
SELECT e.id_estacion, t.id_tiempo, fm.bolivar_monthly, anom.bolivar_imputado, anom.bolivar_anomaly
FROM gold.flow_monthly fm
JOIN gold.dim_tiempo t ON t.anio = fm.year AND t.mes = fm.month
JOIN gold.dim_estacion e ON e.nombre = 'Ciudad Bolivar'
LEFT JOIN gold.flow_monthly_anomalies anom ON anom.year = fm.year AND anom.month = fm.month
WHERE fm.bolivar_monthly IS NOT NULL OR anom.bolivar_imputado = TRUE;

-- Manaos
INSERT INTO gold.fact_caudal_mensual (id_estacion, id_tiempo, caudal_observado, es_imputado, z_score)
SELECT e.id_estacion, t.id_tiempo, fm.manaos_monthly, anom.manaos_imputado, anom.manaos_anomaly
FROM gold.flow_monthly fm
JOIN gold.dim_tiempo t ON t.anio = fm.year AND t.mes = fm.month
JOIN gold.dim_estacion e ON e.nombre = 'Manaos'
LEFT JOIN gold.flow_monthly_anomalies anom ON anom.year = fm.year AND anom.month = fm.month
WHERE fm.manaos_monthly IS NOT NULL OR anom.manaos_imputado = TRUE;

-- Obidos
INSERT INTO gold.fact_caudal_mensual (id_estacion, id_tiempo, caudal_observado, es_imputado, z_score)
SELECT e.id_estacion, t.id_tiempo, fm.obidos_monthly, anom.obidos_imputado, anom.obidos_anomaly
FROM gold.flow_monthly fm
JOIN gold.dim_tiempo t ON t.anio = fm.year AND t.mes = fm.month
JOIN gold.dim_estacion e ON e.nombre = 'Obidos'
LEFT JOIN gold.flow_monthly_anomalies anom ON anom.year = fm.year AND anom.month = fm.month
WHERE fm.obidos_monthly IS NOT NULL OR anom.obidos_imputado = TRUE;

-- Tabatinga
INSERT INTO gold.fact_caudal_mensual (id_estacion, id_tiempo, caudal_observado, es_imputado, z_score)
SELECT e.id_estacion, t.id_tiempo, fm.tabatinga_monthly, anom.tabatinga_imputado, anom.tabatinga_anomaly
FROM gold.flow_monthly fm
JOIN gold.dim_tiempo t ON t.anio = fm.year AND t.mes = fm.month
JOIN gold.dim_estacion e ON e.nombre = 'Tabatinga'
LEFT JOIN gold.flow_monthly_anomalies anom ON anom.year = fm.year AND anom.month = fm.month
WHERE fm.tabatinga_monthly IS NOT NULL OR anom.tabatinga_imputado = TRUE;

-- Timbues
INSERT INTO gold.fact_caudal_mensual (id_estacion, id_tiempo, caudal_observado, es_imputado, z_score)
SELECT e.id_estacion, t.id_tiempo, fm.timbues_monthly, anom.timbues_imputado, anom.timbues_anomaly
FROM gold.flow_monthly fm
JOIN gold.dim_tiempo t ON t.anio = fm.year AND t.mes = fm.month
JOIN gold.dim_estacion e ON e.nombre = 'Timbues'
LEFT JOIN gold.flow_monthly_anomalies anom ON anom.year = fm.year AND anom.month = fm.month
WHERE fm.timbues_monthly IS NOT NULL OR anom.timbues_imputado = TRUE;

\echo 'OK - Modelo dimensional construido'