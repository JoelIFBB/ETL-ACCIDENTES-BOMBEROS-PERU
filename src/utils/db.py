# src/utils/db.py
import logging
import os
import time

import psycopg2

logger = logging.getLogger(__name__)


def get_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variable de entorno '{key}' no está definida. "
            f"Agrégala a tu archivo .env"
        )
    return value


def get_connection(max_retries: int = 3, delay: float = 2.0):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            return psycopg2.connect(
                host=get_env("BOMBEROS_DB_HOST"),
                port=int(os.getenv("BOMBEROS_DB_PORT", 5432)),
                dbname=get_env("BOMBEROS_DB_NAME"),
                user=get_env("BOMBEROS_DB_USER"),
                password=get_env("BOMBEROS_DB_PASSWORD"),
            )
        except psycopg2.OperationalError as e:
            last_error = e
            logger.warning("Intento %d/%d fallido: %s", attempt, max_retries, e)
            if attempt < max_retries:
                time.sleep(delay)

    raise psycopg2.OperationalError(
        f"No se pudo conectar tras {max_retries} intentos: {last_error}"
    )
