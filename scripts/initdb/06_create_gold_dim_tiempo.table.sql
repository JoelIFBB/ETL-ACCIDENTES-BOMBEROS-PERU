-- scripts/initdb/06_create_gold_dim_tiempo.sql

\c bomberos_db;

-- Creación de la dimensión analítica de tiempo (Grano por día)
CREATE TABLE DIM_TIEMPO (
    ID_TIEMPO            INT             PRIMARY KEY,  -- Llave inteligente (Ej: 20260701)
    FECHA_COMPLETA       DATE            NOT NULL,     -- Campo tipo DATE (2026-07-01)
    ANIO                 INTEGER         NOT NULL,     -- Año (2026)
    MES                  INTEGER         NOT NULL,     -- Número de mes (7)
    NOMBRE_MES           VARCHAR(20)     NOT NULL,     -- Nombre en español (Julio)
    DIA                  INTEGER         NOT NULL,     -- Día del mes (1)
    DIA_SEMANA           INTEGER         NOT NULL,     -- Número de día (1 = Lunes, 7 = Domingo)
    NOMBRE_DIA           VARCHAR(20)     NOT NULL      -- Lunes, Martes...
);

-- INDEXACIÓN ANALÍTICA

-- Índices específicos para acelerar agrupaciones y filtros temporales en dashboards
CREATE INDEX IDX_GOLD_DIM_TIEMPO_ANIO_MES  ON DIM_TIEMPO(ANIO, MES);