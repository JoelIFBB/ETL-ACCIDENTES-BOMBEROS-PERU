# src/transform/transform_silver.py
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# Compilado fuera de la función — se hace una sola vez al importar el módulo
_REGEX_COORDS = re.compile(r'-?\d+\.\d+,-?\d+\.\d+')


def clean_text_column(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Limpia espacios al inicio y final de una columna de texto.
    Respeta los nulos — no los convierte al string 'nan'.
    """
    dataframe[column_name] = dataframe[column_name].str.strip()
    return dataframe


def column_todate(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Convierte una columna string a datetime.
    format='mixed' permite que pandas detecte el formato automáticamente.
    """
    dataframe[column_name] = pd.to_datetime(
        dataframe[column_name], format='mixed'
    )
    return dataframe


def split_date(dataframe: pd.DataFrame, column_name: str,
               column_date: str, column_hours: str) -> pd.DataFrame:
    """
    Separa una columna datetime en fecha y hora.
    Disponible para uso futuro — no se aplica en Silver.
    """
    dataframe[column_date]  = dataframe[column_name].dt.date
    dataframe[column_hours] = dataframe[column_name].dt.time
    return dataframe


def split_address(dataframe: pd.DataFrame, column_name: str,
                  column_address: str, column_district: str) -> pd.DataFrame:
    """
    Separa dirección y distrito usando el ÚLTIMO guión como separador.
    Usa rsplit para evitar cortar en guiones dentro de coordenadas.
    Ejemplo:
        "AV. ANGELICA GAMARRA (-12.0049,-77.0632) Nro. 230 - LOS OLIVOS"
        → direccion: "AV. ANGELICA GAMARRA (-12.0049,-77.0632) Nro. 230"
        → distrito:  "LOS OLIVOS"
    """
    split = dataframe[column_name].str.rsplit('-', n=1, expand=True)
    dataframe[column_address]  = split[0].str.strip()
    dataframe[column_district] = split[1].str.strip()
    return dataframe


def extract_coordinates(dataframe: pd.DataFrame,
                        column_name: str) -> pd.DataFrame:
    """
    Extrae latitud y longitud de la columna de dirección.
    - Coordenadas reales (-12.0049,-77.0632) → float, float
    - Coordenadas (0,0)  → None, None  (sin ubicación real)
    - Sin coordenadas    → None, None
    Resultado en dos columnas nuevas: latitud, longitud
    """
    def _parse(texto: str):
        if not isinstance(texto, str):
            return None, None
        match = _REGEX_COORDS.search(texto)
        if not match:
            return None, None
        partes = match.group(0).split(',')
        lat, lon = float(partes[0]), float(partes[1])
        if lat == 0.0 and lon == 0.0:
            return None, None
        return lat, lon

    coords = dataframe[column_name].apply(
        lambda x: pd.Series(_parse(x), index=["latitud", "longitud"])
    )
    dataframe["latitud"]  = coords["latitud"]
    dataframe["longitud"] = coords["longitud"]
    return dataframe


def count_machineries(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Cuenta la cantidad de máquinas que atendieron el incidente.
    Espera el formato normalizado de Bronze: "M2-1 | AMB-176 | M176-1"
    Resultado en columna nueva: Maquinas_count.
    """
    dataframe[column_name + "_count"] = dataframe[column_name].apply(
        lambda x: len([m.strip() for m in x.split('|') if m.strip()])
        if pd.notna(x) else 0
    )
    return dataframe


def split_tipo(dataframe: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Divide la columna tipo en 3 niveles jerárquicos usando ' / ' como separador.
    Maneja todos los casos sin romperse:
    - 2 niveles: categoria + subcategoria, detalle = None
    - 3 niveles: categoria + subcategoria + detalle
    - 4+ niveles: categoria + subcategoria + detalle (exceso concatenado)
    Ejemplo:
        "INCENDIO / VEHICULO / AUTOMOVIL / VIA PUBLICA"
        → tipo_categoria:    "INCENDIO"
        → tipo_subcategoria: "VEHICULO"
        → tipo_detalle:      "AUTOMOVIL / VIA PUBLICA"
    """
    split = dataframe[column_name].str.split(" / ", n=2, expand=True)
    dataframe["tipo_categoria"]    = split[0].str.strip() if 0 in split.columns else None
    dataframe["tipo_subcategoria"] = split[1].str.strip() if 1 in split.columns else None
    dataframe["tipo_detalle"]      = split[2].str.strip() if 2 in split.columns else None
    return dataframe


def transform_to_silver(records: list[dict]) -> pd.DataFrame:
    """
    Función principal — aplica todas las transformaciones en orden.
    Recibe los registros crudos de Bronze y devuelve un DataFrame
    limpio listo para insertar en accidents_silver.
    """
    logger.info("Iniciando transformación a Silver — %d registros", len(records))

    df = pd.DataFrame(records)

    # Paso 1 — limpiar espacios en columnas de texto
    for col in ["NroParte", "Tipo", "Estado", "Direccion_distrito", "Maquinas"]:
        df = clean_text_column(df, col)

    # Paso 2 — convertir Fecha_hora a datetime real
    df = column_todate(df, "Fecha_hora")

    # Paso 3 — separar dirección y distrito
    df = split_address(df, "Direccion_distrito", "direccion", "distrito")

    # Paso 4 — extraer coordenadas de la dirección ya separada
    df = extract_coordinates(df, "direccion")

    # Paso 5 — dividir tipo en 3 niveles jerárquicos
    df = split_tipo(df, "Tipo")

    # Paso 6 — contar máquinas por incidente
    df = count_machineries(df, "Maquinas")

    # Paso 7 — eliminar columna original ya procesada
    df = df.drop(columns=["Direccion_distrito"])

    logger.info("✓ Transformación completada — %d registros", len(df))
    return df