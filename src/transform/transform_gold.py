# src/transform/transform_gold.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DIMENSIONES
# ---------------------------------------------------------------------------

def build_dim_tipo(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye DIM_TIPO desde las columnas ya separadas por Silver.
    No re-parsea 'tipo' — evita duplicar lógica con transform_silver.
    Silver garantiza: tipo_categoria, tipo_subcategoria, tipo_detalle.
    ID_TIPO es generado por PostgreSQL (SERIAL) — aquí se usa como posición ordinal
    para que el loader pueda hacer el mapeo antes del INSERT.
    """
    cols = ["tipo", "tipo_categoria", "tipo_subcategoria", "tipo_detalle"]
    dim = (
        silver_df[cols]
        .drop_duplicates(subset=["tipo"])
        .dropna(subset=["tipo"])
        .reset_index(drop=True)
        .fillna("NO ESPECIFICADO")
    )

    dim = dim.rename(columns={
        "tipo":             "CODIGO_ORIGINAL",
        "tipo_categoria":   "CATEGORIA_NIVEL_1",
        "tipo_subcategoria":"SUBCATEGORIA_NIVEL_2",
        "tipo_detalle":     "EVENTO_NIVEL_3",
    })

    dim.insert(0, "ID_TIPO", range(1, len(dim) + 1))

    return dim


def build_dim_distrito(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Universo de distritos únicos provenientes de Silver.
    reset_index garantiza alineación de índices antes de construir el DataFrame.
    ID_DISTRITO refleja el SERIAL de PostgreSQL.
    """
    dim = (
        silver_df["distrito"]
        .dropna()
        .str.strip()
        .drop_duplicates()
        .reset_index(drop=True)
        .to_frame(name="NOMBRE_DISTRITO")
    )

    dim.insert(0, "ID_DISTRITO", range(1, len(dim) + 1))

    return dim


def build_dim_estado(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Universo de estados únicos de la emergencia.
    Permite filtros rápidos en dashboards de BI sin escanear la FACT.
    ID_ESTADO refleja el SERIAL de PostgreSQL.
    """
    dim = (
        silver_df["estado"]
        .dropna()
        .str.strip()
        .drop_duplicates()
        .reset_index(drop=True)
        .to_frame(name="NOMBRE_ESTADO")
    )

    dim.insert(0, "ID_ESTADO", range(1, len(dim) + 1))

    return dim


def build_dim_tiempo(silver_df: pd.DataFrame) -> pd.DataFrame:
    """
    Dimensión calendario con grano = día (una fila por fecha).
    ID_TIEMPO es llave inteligente YYYYMMDD (int) — sirve como PK y como
    llave de JOIN sin necesidad de SERIAL, ya que es naturalmente única por día.
    TURNO no está aquí: depende de la hora del incidente, vive en la FACT.
    """
    dim = (
        silver_df["fecha_hora"]
        .dropna()
        .dt.normalize()
        .drop_duplicates()
        .reset_index(drop=True)
        .to_frame(name="_ts")
    )

    dim["ID_TIEMPO"]      = dim["_ts"].dt.strftime("%Y%m%d").astype(int)
    dim["FECHA_COMPLETA"] = dim["_ts"].dt.date
    dim["ANIO"]           = dim["_ts"].dt.year
    dim["MES"]            = dim["_ts"].dt.month
    dim["DIA"]            = dim["_ts"].dt.day
    dim["DIA_SEMANA"]     = dim["_ts"].dt.isocalendar().day.astype(int)

    _meses = {
        1:"Enero", 2:"Febrero", 3:"Marzo",     4:"Abril",
        5:"Mayo",  6:"Junio",   7:"Julio",      8:"Agosto",
        9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre",
    }
    _dias = {
        1:"Lunes", 2:"Martes", 3:"Miércoles", 4:"Jueves",
        5:"Viernes", 6:"Sábado", 7:"Domingo",
    }

    dim["NOMBRE_MES"] = dim["MES"].map(_meses)
    dim["NOMBRE_DIA"] = dim["DIA_SEMANA"].map(_dias)

    return dim.drop(columns=["_ts"])


# ---------------------------------------------------------------------------
# FACT
# ---------------------------------------------------------------------------

def build_fact_emergencia(
    silver_df:    pd.DataFrame,
    dim_tipo:     pd.DataFrame,
    dim_distrito: pd.DataFrame,
    dim_estado:   pd.DataFrame,
    dim_tiempo:   pd.DataFrame,
) -> pd.DataFrame:
    """
    Construye FACT_EMERGENCIA — tabla de hechos del modelo estrella.

    Resuelve las FKs haciendo JOIN contra cada dimensión.
    Registros sin FK válida reciben -1 (miembro 'Sin datos' en la dimensión).

    Medidas incluidas:
        MAQUINAS_COUNT — cantidad de máquinas despachadas al incidente
        LATITUD, LONGITUD — coordenadas del incidente (pueden ser NULL)

    TURNO se calcula aquí desde la hora real del incidente, no desde DIM_TIEMPO,
    porque dos incidentes del mismo día pueden tener turnos distintos.
    """
    fact = silver_df.copy()

    # Resolver ID_TIPO
    fact = fact.merge(
        dim_tipo[["ID_TIPO", "CODIGO_ORIGINAL"]],
        left_on="tipo", right_on="CODIGO_ORIGINAL",
        how="left",
    ).drop(columns=["CODIGO_ORIGINAL"])

    # Resolver ID_DISTRITO
    fact = fact.merge(
        dim_distrito[["ID_DISTRITO", "NOMBRE_DISTRITO"]],
        left_on="distrito", right_on="NOMBRE_DISTRITO",
        how="left",
    ).drop(columns=["NOMBRE_DISTRITO"])

    # Resolver ID_ESTADO
    fact = fact.merge(
        dim_estado[["ID_ESTADO", "NOMBRE_ESTADO"]],
        left_on="estado", right_on="NOMBRE_ESTADO",
        how="left",
    ).drop(columns=["NOMBRE_ESTADO"])

    # Resolver ID_TIEMPO desde fecha truncada a día
    fact["_fecha_dia"] = fact["fecha_hora"].dt.normalize().dt.strftime("%Y%m%d").astype("Int64")
    fact = fact.merge(
        dim_tiempo[["ID_TIEMPO"]],
        left_on="_fecha_dia", right_on="ID_TIEMPO",
        how="left",
    ).drop(columns=["_fecha_dia"])

    # TURNO calculado desde la hora real del incidente
    fact["TURNO"] = pd.cut(
        fact["fecha_hora"].dt.hour,
        bins=[-1, 5, 11, 18, 23],
        labels=["Madrugada", "Mañana", "Tarde", "Noche"],
        ordered=False,
    ).astype(str)

    # FKs sin match → -1
    for fk in ["ID_TIPO", "ID_DISTRITO", "ID_ESTADO", "ID_TIEMPO"]:
        fact[fk] = fact[fk].fillna(-1).astype(int)

    return fact[[
        "nro_parte",
        "ID_TIPO",
        "ID_DISTRITO",
        "ID_ESTADO",
        "ID_TIEMPO",
        "TURNO",
        "maquinas_count",
        "latitud",
        "longitud",
    ]].rename(columns={
        "nro_parte":     "NRO_PARTE",
        "maquinas_count":"MAQUINAS_COUNT",
        "latitud":       "LATITUD",
        "longitud":      "LONGITUD",
    })


# ---------------------------------------------------------------------------
# ORQUESTADOR GOLD
# ---------------------------------------------------------------------------

def transform_to_gold(silver_records: list[dict]) -> dict[str, pd.DataFrame]:
    """
    Punto de entrada de la capa Gold.
    Recibe los registros activos de Silver (es_actual = TRUE) y retorna
    todas las tablas del modelo estrella listas para el loader a PostgreSQL.
    """
    logger.info("Iniciando Gold — %d registros recibidos", len(silver_records))

    silver_df = pd.DataFrame(silver_records)

    if silver_df.empty:
        logger.warning("Silver vacío — abortando Gold.")
        return {}

    silver_df["fecha_hora"] = pd.to_datetime(silver_df["fecha_hora"])

    dim_tipo     = build_dim_tipo(silver_df)
    dim_distrito = build_dim_distrito(silver_df)
    dim_estado   = build_dim_estado(silver_df)
    dim_tiempo   = build_dim_tiempo(silver_df)

    logger.info(
        "✓ Dimensiones — TIPO:%d  DISTRITO:%d  ESTADO:%d  TIEMPO:%d",
        len(dim_tipo), len(dim_distrito), len(dim_estado), len(dim_tiempo),
    )

    fact = build_fact_emergencia(silver_df, dim_tipo, dim_distrito, dim_estado, dim_tiempo)

    logger.info("✓ FACT_EMERGENCIA — %d filas", len(fact))

    return {
        "DIM_TIPO":        dim_tipo,
        "DIM_DISTRITO":    dim_distrito,
        "DIM_ESTADO":      dim_estado,
        "DIM_TIEMPO":      dim_tiempo,
        "FACT_EMERGENCIA": fact,
    }