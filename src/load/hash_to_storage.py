# src/load/hash_to_storage.py
import logging
from typing import Optional

import psycopg2

from src.utils.db import get_env, get_connection

logger = logging.getLogger(__name__)


def load_last_hash() -> str | None:
    """
    Lee el hash más reciente desde pipeline_hash_log.
    Devuelve None si es la primera ejecución.
    """
    conn = None
    try:
        conn = get_connection()
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
        conn = get_connection()
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