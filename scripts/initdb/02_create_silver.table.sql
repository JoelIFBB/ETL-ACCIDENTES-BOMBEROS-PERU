-- scripts/initdb/02_create_silver_table.sql

\c bomberos_db;

CREATE TABLE accidents_silver (
    id              SERIAL          PRIMARY KEY,
    nro_parte       VARCHAR(50)     NOT NULL,
    fecha_hora      TIMESTAMP       NOT NULL,
    direccion       TEXT,
    distrito        TEXT,
    tipo            VARCHAR(100),
    estado          VARCHAR(50),
    estado_anterior VARCHAR(50),
    es_actual       BOOLEAN         NOT NULL DEFAULT TRUE,
    maquinas        TEXT,
    maquinas_count  INTEGER         DEFAULT 0,
    latitud         NUMERIC(10, 7),
    longitud        NUMERIC(10, 7),
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- CDC: búsqueda rápida del estado actual por nro_parte
CREATE UNIQUE INDEX idx_silver_nro_parte_actual
    ON accidents_silver(nro_parte)
    WHERE es_actual = TRUE;

-- Consultas por día y mes
CREATE INDEX idx_silver_fecha     ON accidents_silver(fecha_hora);
CREATE INDEX idx_silver_mes       ON accidents_silver(DATE_TRUNC('month', fecha_hora));

-- Consultas por tipo y distrito en Gold
CREATE INDEX idx_silver_tipo      ON accidents_silver(tipo);
CREATE INDEX idx_silver_distrito  ON accidents_silver(distrito);

-- Historial por accidente
CREATE INDEX idx_silver_nro_parte ON accidents_silver(nro_parte);