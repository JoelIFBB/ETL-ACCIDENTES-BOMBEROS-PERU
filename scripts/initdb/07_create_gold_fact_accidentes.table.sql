-- scripts/initdb/07_create_gold_fact_accidentes.sql

\c bomberos_db;

-- Creación de la tabla de hechos del modelo estrella de emergencias
CREATE TABLE FACT_EMERGENCIA (
    ID_FACT         SERIAL          PRIMARY KEY,
    NRO_PARTE       VARCHAR(50)     NOT NULL UNIQUE,
    ID_TIPO         INT             NOT NULL DEFAULT -1,
    ID_DISTRITO     INT             NOT NULL DEFAULT -1,
    ID_ESTADO       INT             NOT NULL DEFAULT -1,
    ID_TIEMPO       INT             NOT NULL,
    TURNO           VARCHAR(20)     NOT NULL,
    MAQUINAS_COUNT  SMALLINT        NOT NULL DEFAULT 0,
    LATITUD         NUMERIC(10, 6),
    LONGITUD        NUMERIC(10, 6),

    CONSTRAINT FK_FACT_TIPO     FOREIGN KEY (ID_TIPO)     REFERENCES DIM_TIPO(ID_TIPO),
    CONSTRAINT FK_FACT_DISTRITO FOREIGN KEY (ID_DISTRITO) REFERENCES DIM_DISTRITO(ID_DISTRITO),
    CONSTRAINT FK_FACT_ESTADO   FOREIGN KEY (ID_ESTADO)   REFERENCES DIM_ESTADO(ID_ESTADO),
    CONSTRAINT FK_FACT_TIEMPO   FOREIGN KEY (ID_TIEMPO)   REFERENCES DIM_TIEMPO(ID_TIEMPO)
);

-- INDEXACIÓN ANALÍTICA

-- Claves foráneas indexadas para acelerar JOINs en consultas de BI
CREATE INDEX IDX_GOLD_FACT_TIPO      ON FACT_EMERGENCIA(ID_TIPO);
CREATE INDEX IDX_GOLD_FACT_DISTRITO  ON FACT_EMERGENCIA(ID_DISTRITO);
CREATE INDEX IDX_GOLD_FACT_ESTADO    ON FACT_EMERGENCIA(ID_ESTADO);
CREATE INDEX IDX_GOLD_FACT_TIEMPO    ON FACT_EMERGENCIA(ID_TIEMPO);

-- Filtros frecuentes en dashboards de análisis operacional
CREATE INDEX IDX_GOLD_FACT_TURNO     ON FACT_EMERGENCIA(TURNO);
CREATE INDEX IDX_GOLD_FACT_COORDS    ON FACT_EMERGENCIA(LATITUD, LONGITUD)
    WHERE LATITUD IS NOT NULL AND LONGITUD IS NOT NULL;

-- =============================================================================
-- MIEMBROS 'SIN DATOS' — permiten FK -1 sin violar integridad referencial
-- Deben insertarse antes que cualquier registro de la FACT
-- =============================================================================

INSERT INTO DIM_TIPO     VALUES (-1, 'SIN DATOS', 'SIN DATOS', 'SIN DATOS', 'SIN DATOS');
INSERT INTO DIM_DISTRITO VALUES (-1, 'SIN DATOS');
INSERT INTO DIM_ESTADO   VALUES (-1, 'SIN DATOS');