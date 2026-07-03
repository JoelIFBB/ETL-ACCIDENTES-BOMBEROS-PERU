-- scripts/initdb/03_create_gold_dim_tipo.sql

\c bomberos_db;

-- Creación de la dimensión analítica de tipo de accidente
CREATE TABLE DIM_TIPO (
    ID_TIPO              SERIAL          PRIMARY KEY,
    CODIGO_ORIGINAL      VARCHAR(255)    NOT NULL,
    CATEGORIA_NIVEL_1    VARCHAR(100)    NOT NULL,
    SUBCATEGORIA_NIVEL_2 VARCHAR(100)    NOT NULL,
    EVENTO_NIVEL_3       VARCHAR(255)    NOT NULL
);

-- INDEXACIÓN ANALÍTICA

-- Búsqueda rápida por código original para el proceso de Upsert/Carga desde Python (Evita duplicar strings)
CREATE UNIQUE INDEX IDX_GOLD_DIM_TIPO_CODIGO_ORIGINAL
    ON DIM_TIPO(CODIGO_ORIGINAL);

-- Indexación de las jerarquías para optimizar los filtros y agregaciones de los dashboards de BI
CREATE INDEX IDX_GOLD_DIM_TIPO_NIVEL_1    ON DIM_TIPO(CATEGORIA_NIVEL_1);
CREATE INDEX IDX_GOLD_DIM_TIPO_NIVEL_2    ON DIM_TIPO(SUBCATEGORIA_NIVEL_2);
CREATE INDEX IDX_GOLD_DIM_TIPO_NIVEL_3    ON DIM_TIPO(EVENTO_NIVEL_3);