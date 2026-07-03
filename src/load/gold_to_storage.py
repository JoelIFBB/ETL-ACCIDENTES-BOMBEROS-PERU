# src/load/gold_to_storage.py
import logging
import pandas as pd

from src.utils.db import get_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DIMENSIONES — upsert + lectura de IDs reales de PostgreSQL
# ---------------------------------------------------------------------------

def _upsert_dim_tipo(cur, df: pd.DataFrame) -> dict[str, int]:
    """
    Inserta tipos nuevos en DIM_TIPO ignorando duplicados.
    Devuelve el mapeo {CODIGO_ORIGINAL: ID_TIPO} con los IDs reales de PostgreSQL.
    """
    rows = df[["CODIGO_ORIGINAL", "CATEGORIA_NIVEL_1",
               "SUBCATEGORIA_NIVEL_2", "EVENTO_NIVEL_3"]].values.tolist()

    cur.executemany("""
        INSERT INTO DIM_TIPO (CODIGO_ORIGINAL, CATEGORIA_NIVEL_1, SUBCATEGORIA_NIVEL_2, EVENTO_NIVEL_3)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (CODIGO_ORIGINAL) DO NOTHING
    """, rows)

    cur.execute("SELECT ID_TIPO, CODIGO_ORIGINAL FROM DIM_TIPO")
    return {row[1]: row[0] for row in cur.fetchall()}


def _upsert_dim_distrito(cur, df: pd.DataFrame) -> dict[str, int]:
    """
    Inserta distritos nuevos en DIM_DISTRITO ignorando duplicados.
    Devuelve el mapeo {NOMBRE_DISTRITO: ID_DISTRITO} con IDs reales.
    """
    rows = df[["NOMBRE_DISTRITO"]].values.tolist()

    cur.executemany("""
        INSERT INTO DIM_DISTRITO (NOMBRE_DISTRITO)
        VALUES (%s)
        ON CONFLICT (NOMBRE_DISTRITO) DO NOTHING
    """, rows)

    cur.execute("SELECT ID_DISTRITO, NOMBRE_DISTRITO FROM DIM_DISTRITO")
    return {row[1]: row[0] for row in cur.fetchall()}


def _upsert_dim_estado(cur, df: pd.DataFrame) -> dict[str, int]:
    """
    Inserta estados nuevos en DIM_ESTADO ignorando duplicados.
    Devuelve el mapeo {NOMBRE_ESTADO: ID_ESTADO} con IDs reales.
    """
    rows = df[["NOMBRE_ESTADO"]].values.tolist()

    cur.executemany("""
        INSERT INTO DIM_ESTADO (NOMBRE_ESTADO)
        VALUES (%s)
        ON CONFLICT (NOMBRE_ESTADO) DO NOTHING
    """, rows)

    cur.execute("SELECT ID_ESTADO, NOMBRE_ESTADO FROM DIM_ESTADO")
    return {row[1]: row[0] for row in cur.fetchall()}


def _upsert_dim_tiempo(cur, df: pd.DataFrame) -> None:
    """
    Inserta fechas nuevas en DIM_TIEMPO ignorando duplicados.
    ID_TIEMPO es YYYYMMDD (int) — no usa SERIAL, es la PK natural.
    """
    rows = df[[
        "ID_TIEMPO", "FECHA_COMPLETA", "ANIO", "MES", "DIA",
        "DIA_SEMANA", "NOMBRE_MES", "NOMBRE_DIA",
    ]].values.tolist()

    cur.executemany("""
        INSERT INTO DIM_TIEMPO (ID_TIEMPO, FECHA_COMPLETA, ANIO, MES, DIA,
                                DIA_SEMANA, NOMBRE_MES, NOMBRE_DIA)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ID_TIEMPO) DO NOTHING
    """, rows)


# ---------------------------------------------------------------------------
# FACT — re-resolución con IDs reales + upsert
# ---------------------------------------------------------------------------

def _resolve_fact_ids(
    fact_df:       pd.DataFrame,
    map_tipo:      dict[str, int],
    map_distrito:  dict[str, int],
    map_estado:    dict[str, int],
    silver_df:     pd.DataFrame,
) -> pd.DataFrame:
    """
    Reemplaza los IDs temporales de pandas por los IDs reales de PostgreSQL.
    Necesario porque el SERIAL de PostgreSQL no coincide con range(1, n+1).
    Los registros sin match reciben -1 (miembro 'Sin datos').
    """
    # Re-unimos desde silver_df para tener las claves naturales disponibles
    fact = silver_df[["nro_parte", "tipo", "distrito", "estado",
                       "fecha_hora", "maquinas_count", "latitud", "longitud"]].copy()

    fact["ID_TIPO"]     = fact["tipo"].map(map_tipo).fillna(-1).astype(int)
    fact["ID_DISTRITO"] = fact["distrito"].map(map_distrito).fillna(-1).astype(int)
    fact["ID_ESTADO"]   = fact["estado"].map(map_estado).fillna(-1).astype(int)
    fact["ID_TIEMPO"]   = fact["fecha_hora"].dt.normalize().dt.strftime("%Y%m%d").astype(int)

    fact["TURNO"] = pd.cut(
        fact["fecha_hora"].dt.hour,
        bins=[-1, 5, 11, 18, 23],
        labels=["Madrugada", "Mañana", "Tarde", "Noche"],
        ordered=False,
    ).astype(str)

    return fact[[
        "nro_parte", "ID_TIPO", "ID_DISTRITO", "ID_ESTADO",
        "ID_TIEMPO", "TURNO", "maquinas_count", "latitud", "longitud",
    ]].rename(columns={
        "nro_parte":     "NRO_PARTE",
        "maquinas_count":"MAQUINAS_COUNT",
        "latitud":       "LATITUD",
        "longitud":      "LONGITUD",
    })


def _upsert_fact(cur, fact_df: pd.DataFrame) -> dict[str, int]:
    """
    Inserta o actualiza registros en FACT_EMERGENCIA.
    ON CONFLICT en NRO_PARTE actualiza todos los campos — refleja el
    estado más reciente del incidente (Silver ya garantiza es_actual=TRUE).
    """
    insertados  = 0
    actualizados = 0

    rows = fact_df[[
        "NRO_PARTE", "ID_TIPO", "ID_DISTRITO", "ID_ESTADO",
        "ID_TIEMPO", "TURNO", "MAQUINAS_COUNT", "LATITUD", "LONGITUD",
    ]].values.tolist()

    for row in rows:
        cur.execute("""
            INSERT INTO FACT_EMERGENCIA (
                NRO_PARTE, ID_TIPO, ID_DISTRITO, ID_ESTADO,
                ID_TIEMPO, TURNO, MAQUINAS_COUNT, LATITUD, LONGITUD
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (NRO_PARTE) DO UPDATE SET
                ID_TIPO      = EXCLUDED.ID_TIPO,
                ID_DISTRITO  = EXCLUDED.ID_DISTRITO,
                ID_ESTADO    = EXCLUDED.ID_ESTADO,
                ID_TIEMPO    = EXCLUDED.ID_TIEMPO,
                TURNO        = EXCLUDED.TURNO,
                MAQUINAS_COUNT = EXCLUDED.MAQUINAS_COUNT,
                LATITUD      = EXCLUDED.LATITUD,
                LONGITUD     = EXCLUDED.LONGITUD
        """, row)

        if cur.rowcount == 1:
            insertados += 1
        else:
            actualizados += 1

    return {"insertados": insertados, "actualizados": actualizados}


# ---------------------------------------------------------------------------
# PUNTO DE ENTRADA
# ---------------------------------------------------------------------------

def upload_gold_data(
    gold_tables: dict[str, pd.DataFrame],
    silver_df:   pd.DataFrame,
) -> dict:
    """
    Carga todas las tablas Gold a PostgreSQL en el orden correcto:
    1. Dimensiones primero (generan IDs reales via SERIAL)
    2. FACT al final (usa los IDs reales recién obtenidos)

    Parámetros:
        gold_tables — resultado de transform_to_gold()
        silver_df   — DataFrame Silver original para re-resolver FKs con IDs reales

    Retorna métricas de carga por tabla.
    """
    conn = None
    try:
        conn = get_connection()

        with conn.cursor() as cur:

            # 1. Dimensiones
            logger.info("Cargando DIM_TIPO...")
            map_tipo = _upsert_dim_tipo(cur, gold_tables["DIM_TIPO"])

            logger.info("Cargando DIM_DISTRITO...")
            map_distrito = _upsert_dim_distrito(cur, gold_tables["DIM_DISTRITO"])

            logger.info("Cargando DIM_ESTADO...")
            map_estado = _upsert_dim_estado(cur, gold_tables["DIM_ESTADO"])

            logger.info("Cargando DIM_TIEMPO...")
            _upsert_dim_tiempo(cur, gold_tables["DIM_TIEMPO"])

            # 2. Re-resolver FKs con IDs reales de PostgreSQL
            logger.info("Re-resolviendo FKs con IDs reales de PostgreSQL...")
            fact_df = _resolve_fact_ids(
                gold_tables["FACT_EMERGENCIA"],
                map_tipo, map_distrito, map_estado,
                silver_df,
            )

            # 3. FACT
            logger.info("Cargando FACT_EMERGENCIA...")
            fact_result = _upsert_fact(cur, fact_df)

        conn.commit()

        logger.info(
            "✓ Gold — FACT: %d insertados, %d actualizados",
            fact_result["insertados"], fact_result["actualizados"],
        )

        return {
            "dim_tipo_count":     len(map_tipo),
            "dim_distrito_count": len(map_distrito),
            "dim_estado_count":   len(map_estado),
            "fact_insertados":    fact_result["insertados"],
            "fact_actualizados":  fact_result["actualizados"],
        }

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error("Error en Gold loader: %s", e)
        raise

    finally:
        if conn:
            conn.close()