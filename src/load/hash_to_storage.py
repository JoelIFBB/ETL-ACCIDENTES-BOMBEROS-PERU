# src/load/hash_to_storage.py
import logging
import os
import time
from typing import Optional

import psycopg2

logger = logging.getLogger(__name__)


def _get_env(key: str) -> str:
    """
    Lee una variable de entorno y lanza error claro si no está definida.
    Evita errores crípticos de PostgreSQL por credenciales faltantes.
    """
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variable de entorno '{key}' no está definida. "
            f"Agrégala a tu archivo .env"
        )
    return value


def _get_connection(max_retries: int = 3, delay: float = 2.0):
    """
    Intenta conectarse a bomberos_db hasta max_retries veces.
    Espera delay segundos entre intentos para tolerar fallos momentáneos.
    """
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            return psycopg2.connect(
                host=_get_env("BOMBEROS_DB_HOST"),
                port=int(os.getenv("BOMBEROS_DB_PORT", 5432)),
                dbname=_get_env("BOMBEROS_DB_NAME"),
                user=_get_env("BOMBEROS_DB_USER"),
                password=_get_env("BOMBEROS_DB_PASSWORD"),
            )
        except psycopg2.OperationalError as e:
            last_error = e
            logger.warning("Intento %d/%d fallido: %s", attempt, max_retries, e)
            if attempt < max_retries:
                time.sleep(delay)

    raise psycopg2.OperationalError(
        f"No se pudo conectar a PostgreSQL tras {max_retries} intentos: {last_error}"
    )


def load_last_hash() -> str | None:
    """
    Lee el hash más reciente desde pipeline_hash_log.
    Devuelve None si es la primera ejecución.
    """
    conn = None
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT hash FROM pipeline_hash_log
                WHERE pipeline = 'accidents'
                ORDER BY recorded_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
        conn.commit()

        if row is None:
            logger.info("Primera ejecución — no existe hash previo.")
            return None

        logger.info("Hash anterior leído: %s...", row[0][:16])
        return row[0]

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Error leyendo hash desde PostgreSQL: %s", e)
        raise
    finally:
        if conn:
            conn.close()


def save_hash(hash_value: str) -> None:
    """
    Inserta el hash de la ejecución actual en pipeline_hash_log.
    Cada ejecución exitosa genera una nueva fila — el historial se preserva.
    """
    conn = None
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_hash_log (pipeline, hash)
                VALUES (%s, %s)
            """, ("accidents", hash_value))
        conn.commit()
        logger.info("Hash guardado: %s...", hash_value[:16])

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Error guardando hash en PostgreSQL: %s", e)
        raise
    finally:
        if conn:
            conn.close()