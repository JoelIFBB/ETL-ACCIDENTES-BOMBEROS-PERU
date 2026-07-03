-- scripts/initdb/01_create_bomberos_db.sql

CREATE DATABASE bomberos_db;

\c bomberos_db;

CREATE TABLE pipeline_hash_log (
    id           SERIAL        PRIMARY KEY,
    pipeline     VARCHAR(100)  NOT NULL,
    hash         CHAR(64)      NOT NULL,
    recorded_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);