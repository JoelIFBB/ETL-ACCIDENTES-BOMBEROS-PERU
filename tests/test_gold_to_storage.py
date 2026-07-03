# tests/test_gold_to_storage.py
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.load.gold_to_storage import (
    _upsert_dim_tipo,
    _upsert_dim_distrito,
    _upsert_dim_estado,
    _upsert_dim_tiempo,
    _upsert_fact,
    _resolve_fact_ids,
    upload_gold_data,
)


@pytest.fixture
def sample_silver_df():
    return pd.DataFrame([{
        "nro_parte": "001",
        "tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
        "distrito": "MIRAFLORES",
        "estado": "ATENDIENDO",
        "fecha_hora": pd.Timestamp("2026-06-27 01:06:42"),
        "maquinas_count": 2,
        "latitud": -12.0049,
        "longitud": -77.0632,
    }])


def _make_mock_cur(**attrs):
    cur = MagicMock()
    cur.rowcount = 1
    fetchall = attrs.pop("fetchall_return_value", None)
    if fetchall is not None:
        cur.fetchall.return_value = fetchall
    for k, v in attrs.items():
        setattr(cur, k, v)
    return cur


def _make_mock_conn(cur):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    return conn


def test_upsert_dim_tipo_returns_mapping():
    cur = _make_mock_cur(
        fetchall_return_value=[(1, "INCENDIO / VEHICULO / AUTOMOVIL")]
    )
    df = pd.DataFrame([{
        "ID_TIPO": 1,
        "CODIGO_ORIGINAL": "INCENDIO / VEHICULO / AUTOMOVIL",
        "CATEGORIA_NIVEL_1": "INCENDIO",
        "SUBCATEGORIA_NIVEL_2": "VEHICULO",
        "EVENTO_NIVEL_3": "AUTOMOVIL",
    }])
    mapping = _upsert_dim_tipo(cur, df)
    assert mapping == {"INCENDIO / VEHICULO / AUTOMOVIL": 1}
    assert cur.executemany.called


def test_upsert_dim_distrito_returns_mapping():
    cur = _make_mock_cur(
        fetchall_return_value=[(1, "MIRAFLORES"), (2, "SURCO")]
    )
    df = pd.DataFrame([
        {"ID_DISTRITO": 1, "NOMBRE_DISTRITO": "MIRAFLORES"},
        {"ID_DISTRITO": 2, "NOMBRE_DISTRITO": "SURCO"},
    ])
    mapping = _upsert_dim_distrito(cur, df)
    assert mapping == {"MIRAFLORES": 1, "SURCO": 2}


def test_upsert_dim_estado_returns_mapping():
    cur = _make_mock_cur(
        fetchall_return_value=[(1, "ATENDIENDO"), (2, "CONTROLADO")]
    )
    df = pd.DataFrame([
        {"ID_ESTADO": 1, "NOMBRE_ESTADO": "ATENDIENDO"},
        {"ID_ESTADO": 2, "NOMBRE_ESTADO": "CONTROLADO"},
    ])
    mapping = _upsert_dim_estado(cur, df)
    assert mapping == {"ATENDIENDO": 1, "CONTROLADO": 2}


def test_upsert_dim_tiempo_inserts_without_return():
    cur = _make_mock_cur()
    df = pd.DataFrame([{
        "ID_TIEMPO": 20260627,
        "FECHA_COMPLETA": "2026-06-27",
        "ANIO": 2026, "MES": 6, "DIA": 27,
        "DIA_SEMANA": 6, "NOMBRE_MES": "Junio", "NOMBRE_DIA": "Sabado",
    }])
    _upsert_dim_tiempo(cur, df)
    assert cur.executemany.called


def test_upsert_fact_insert_count():
    cur = _make_mock_cur(rowcount=1)
    fact = pd.DataFrame([{
        "NRO_PARTE": "001", "ID_TIPO": 1, "ID_DISTRITO": 1,
        "ID_ESTADO": 1, "ID_TIEMPO": 20260627, "TURNO": "Madrugada",
        "MAQUINAS_COUNT": 2, "LATITUD": -12.0049, "LONGITUD": -77.0632,
    }])
    result = _upsert_fact(cur, fact)
    assert result == {"insertados": 1, "actualizados": 0}


def test_upsert_fact_update_count():
    cur = _make_mock_cur(rowcount=2)
    fact = pd.DataFrame([{
        "NRO_PARTE": "001", "ID_TIPO": 1, "ID_DISTRITO": 1,
        "ID_ESTADO": 1, "ID_TIEMPO": 20260627, "TURNO": "Madrugada",
        "MAQUINAS_COUNT": 2, "LATITUD": -12.0049, "LONGITUD": -77.0632,
    }])
    result = _upsert_fact(cur, fact)
    assert result == {"insertados": 0, "actualizados": 1}


def test_resolve_fact_ids_maps_real_ids(sample_silver_df):
    fact = pd.DataFrame([{
        "NRO_PARTE": "001",
        "tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
        "distrito": "MIRAFLORES",
        "estado": "ATENDIENDO",
        "fecha_hora": pd.Timestamp("2026-06-27 01:06:42"),
        "TURNO": "Madrugada",
        "MAQUINAS_COUNT": 2,
        "LATITUD": -12.0049,
        "LONGITUD": -77.0632,
    }])
    map_tipo = {"INCENDIO / VEHICULO / AUTOMOVIL": 10}
    map_distrito = {"MIRAFLORES": 20}
    map_estado = {"ATENDIENDO": 30}
    resolved = _resolve_fact_ids(
        fact, map_tipo, map_distrito, map_estado
    )
    assert resolved["ID_TIPO"].iloc[0] == 10
    assert resolved["ID_DISTRITO"].iloc[0] == 20
    assert resolved["ID_ESTADO"].iloc[0] == 30


def test_upload_gold_data_full_flow(sample_silver_df):
    cur = _make_mock_cur(
        rowcount=1,
        fetchall_return_value=[(1, "INCENDIO / VEHICULO / AUTOMOVIL")],
    )
    conn = _make_mock_conn(cur)

    gold_dim_distrito = pd.DataFrame([
        {"ID_DISTRITO": 1, "NOMBRE_DISTRITO": "MIRAFLORES"},
    ])
    gold_dim_estado = pd.DataFrame([
        {"ID_ESTADO": 1, "NOMBRE_ESTADO": "ATENDIENDO"},
    ])
    gold_dim_tiempo = pd.DataFrame([{
        "ID_TIEMPO": 20260627, "FECHA_COMPLETA": "2026-06-27",
        "ANIO": 2026, "MES": 6, "DIA": 27,
        "DIA_SEMANA": 6, "NOMBRE_MES": "Junio", "NOMBRE_DIA": "Sabado",
    }])
    gold_fact = pd.DataFrame([{
        "NRO_PARTE": "001",
        "tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
        "distrito": "MIRAFLORES",
        "estado": "ATENDIENDO",
        "fecha_hora": pd.Timestamp("2026-06-27 01:06:42"),
        "TURNO": "Madrugada",
        "MAQUINAS_COUNT": 2,
        "LATITUD": -12.0049,
        "LONGITUD": -77.0632,
    }])

    gold_tables = {
        "DIM_TIPO": pd.DataFrame([{
            "ID_TIPO": 1,
            "CODIGO_ORIGINAL": "INCENDIO / VEHICULO / AUTOMOVIL",
            "CATEGORIA_NIVEL_1": "INCENDIO",
            "SUBCATEGORIA_NIVEL_2": "VEHICULO",
            "EVENTO_NIVEL_3": "AUTOMOVIL",
        }]),
        "DIM_DISTRITO": gold_dim_distrito,
        "DIM_ESTADO": gold_dim_estado,
        "DIM_TIEMPO": gold_dim_tiempo,
        "FACT_EMERGENCIA": gold_fact,
    }

    with patch("src.load.gold_to_storage.get_connection",
               return_value=conn):
        result = upload_gold_data(gold_tables)

    assert result["fact_insertados"] == 1
    assert result["fact_actualizados"] == 0
    conn.commit.assert_called_once()
    conn.close.assert_called_once()
