# src/load/silver_to_storage.py
import logging
import os
import time

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)


def _get_env(key: str) -> str:
    """
    Lee una variable de entorno y lanza error claro si no está definida.
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
        f"No se pudo conectar tras {max_retries} intentos: {last_error}"
    )


def _get_estados_actuales(cur, nro_partes: list[str]) -> dict[str, str]:
    """
    Consulta el estado actual de todos los accidentes del batch en una sola query.
    Devuelve {nro_parte: estado} para los que ya existen en Silver.
    """
    if not nro_partes:
        return {}
    cur.execute("""
        SELECT nro_parte, estado FROM accidents_silver
        WHERE nro_parte = ANY(%s) AND es_actual = TRUE
    """, (nro_partes,))
    return {row[0]: row[1] for row in cur.fetchall()}


def _insert_nuevo(cur, row: pd.Series) -> None:
    """
    Inserta un accidente nuevo — primera vez que aparece en Silver.
    es_actual = TRUE por defecto, estado_anterior = NULL.
    """
    cur.execute("""
        INSERT INTO accidents_silver (
            nro_parte, fecha_hora, direccion, distrito,
            tipo, tipo_categoria, tipo_subcategoria, tipo_detalle,
            estado, estado_anterior, es_actual,
            maquinas, maquinas_count, latitud, longitud
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, TRUE, %s, %s, %s, %s)
    """, (
        row["NroParte"],
        row["Fecha_hora"],
        row.get("direccion"),
        row.get("distrito"),
        row.get("Tipo"),
        row.get("tipo_categoria"),
        row.get("tipo_subcategoria"),
        row.get("tipo_detalle"),
        row.get("Estado"),
        row.get("Maquinas"),
        row.get("Maquinas_count", 0),
        row.get("latitud"),
        row.get("longitud"),
    ))


def _update_estado(cur, row: pd.Series, estado_anterior: str) -> None:
    """
    Registra un cambio de estado:
    1. Marca la fila actual como es_actual = FALSE
    2. Inserta fila nueva con el estado nuevo y estado_anterior
    """
    cur.execute("""
        UPDATE accidents_silver
        SET es_actual = FALSE
        WHERE nro_parte = %s AND es_actual = TRUE
    """, (row["NroParte"],))

    cur.execute("""
        INSERT INTO accidents_silver (
            nro_parte, fecha_hora, direccion, distrito,
            tipo, tipo_categoria, tipo_subcategoria, tipo_detalle,
            estado, estado_anterior, es_actual,
            maquinas, maquinas_count, latitud, longitud
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s, %s, %s)
    """, (
        row["NroParte"],
        row["Fecha_hora"],
        row.get("direccion"),
        row.get("distrito"),
        row.get("Tipo"),
        row.get("tipo_categoria"),
        row.get("tipo_subcategoria"),
        row.get("tipo_detalle"),
        row.get("Estado"),
        estado_anterior,
        row.get("Maquinas"),
        row.get("Maquinas_count", 0),
        row.get("latitud"),
        row.get("longitud"),
    ))


def upload_silver_data(df: pd.DataFrame) -> dict:
    """
    Inserta o actualiza registros en accidents_silver aplicando CDC con historial.

    Lógica por cada accidente:
    - No existe en Silver → INSERT nuevo
    - Existe con mismo estado → ignorar
    - Existe con estado distinto → marcar anterior como FALSE + INSERT nuevo
    """
    conn = None
    insertados   = 0
    actualizados = 0
    ignorados    = 0

    try:
        conn = _get_connection()

        with conn.cursor() as cur:
            # Una sola query para todos los NroParte del batch
            estados_db = _get_estados_actuales(cur, df["NroParte"].tolist())

            for _, row in df.iterrows():
                nro          = row["NroParte"]
                estado_db    = estados_db.get(nro)

                if estado_db is None:
                    _insert_nuevo(cur, row)
                    insertados += 1
                elif estado_db != row.get("Estado"):
                    _update_estado(cur, row, estado_db)
                    actualizados += 1
                else:
                    ignorados += 1

        conn.commit()
        logger.info(
            "✓ Silver — %d nuevos, %d actualizados, %d sin cambios",
            insertados, actualizados, ignorados,
        )
        return {
            "insertados":   insertados,
            "actualizados": actualizados,
            "ignorados":    ignorados,
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Error en Silver: %s", e)
        raise
    finally:
        if conn:
            conn.close()