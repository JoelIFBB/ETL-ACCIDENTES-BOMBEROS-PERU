-- scripts/initdb/05_create_gold_dim_estado.sql

\c bomberos_db;

-- Creación de la dimensión analítica de estados de la emergencia
CREATE TABLE DIM_ESTADO (
    ID_ESTADO            SERIAL          PRIMARY KEY,
    NOMBRE_ESTADO        VARCHAR(50)     NOT NULL
);

-- INDEXACIÓN ANALÍTICA

-- Búsqueda rápida por nombre para el proceso de carga y cruce desde Python (accidents_silver.estado)
CREATE UNIQUE INDEX IDX_GOLD_DIM_ESTADO_NOMBRE
    ON DIM_ESTADO(NOMBRE_ESTADO);