-- scripts/initdb/04_create_gold_dim_distrito.sql

\c bomberos_db;

-- Creación de la dimensión analítica de distritos
CREATE TABLE DIM_DISTRITO (
    ID_DISTRITO          SERIAL          PRIMARY KEY,
    NOMBRE_DISTRITO      VARCHAR(100)    NOT NULL
);

-- INDEXACIÓN ANALÍTICA

-- Búsqueda rápida por nombre para el proceso de carga y cruce desde Python (accidents_silver.distrito)
CREATE UNIQUE INDEX IDX_GOLD_DIM_DISTRITO_NOMBRE
    ON DIM_DISTRITO(NOMBRE_DISTRITO);