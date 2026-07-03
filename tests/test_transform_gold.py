# tests/test_transform_gold.py
import pandas as pd
import pytest

from src.transform.transform_gold import (
    build_dim_tipo,
    build_dim_distrito,
    build_dim_estado,
    build_dim_tiempo,
    build_fact_emergencia,
    transform_to_gold,
)


@pytest.fixture
def silver_df():
    return pd.DataFrame([
        {
            "nro_parte": "001",
            "fecha_hora": pd.Timestamp("2026-06-27 01:06:42"),
            "direccion": "AV. A",
            "distrito": "MIRAFLORES",
            "tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
            "tipo_categoria": "INCENDIO",
            "tipo_subcategoria": "VEHICULO",
            "tipo_detalle": "AUTOMOVIL",
            "estado": "ATENDIENDO",
            "maquinas": "M2-1 | AMB-176",
            "maquinas_count": 2,
            "latitud": -12.0049,
            "longitud": -77.0632,
        },
        {
            "nro_parte": "002",
            "fecha_hora": pd.Timestamp("2026-06-27 10:30:00"),
            "direccion": "AV. B",
            "distrito": "SURCO",
            "tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
            "tipo_categoria": "INCENDIO",
            "tipo_subcategoria": "VEHICULO",
            "tipo_detalle": "AUTOMOVIL",
            "estado": "CONTROLADO",
            "maquinas": "M1",
            "maquinas_count": 1,
            "latitud": -12.1000,
            "longitud": -77.0000,
        },
    ])


def test_build_dim_tipo_dedup(silver_df):
    dim = build_dim_tipo(silver_df)
    assert len(dim) == 1


def test_build_dim_tipo_columns(silver_df):
    dim = build_dim_tipo(silver_df)
    assert list(dim.columns) == [
        "ID_TIPO", "CODIGO_ORIGINAL", "CATEGORIA_NIVEL_1",
        "SUBCATEGORIA_NIVEL_2", "EVENTO_NIVEL_3",
    ]


def test_build_dim_tipo_na_detalle_filled():
    df = pd.DataFrame([{
        "tipo": "RESCATE / VEHICULAR",
        "tipo_categoria": "RESCATE",
        "tipo_subcategoria": "VEHICULAR",
        "tipo_detalle": None,
    }])
    dim = build_dim_tipo(df)
    assert dim["EVENTO_NIVEL_3"].iloc[0] == "NO ESPECIFICADO"


def test_build_dim_distrito_unique(silver_df):
    dim = build_dim_distrito(silver_df)
    assert len(dim) == 2
    assert list(dim.columns) == ["ID_DISTRITO", "NOMBRE_DISTRITO"]


def test_build_dim_distrito_dedup():
    df = pd.DataFrame({"distrito": ["MIRAFLORES", "MIRAFLORES", "SURCO"]})
    dim = build_dim_distrito(df)
    assert len(dim) == 2


def test_build_dim_estado_unique(silver_df):
    dim = build_dim_estado(silver_df)
    assert len(dim) == 2
    assert list(dim.columns) == ["ID_ESTADO", "NOMBRE_ESTADO"]


def test_build_dim_tiempo_pk_and_spanish_names(silver_df):
    dim = build_dim_tiempo(silver_df)
    assert len(dim) == 1
    row = dim.iloc[0]
    assert row["ID_TIEMPO"] == 20260627
    assert row["ANIO"] == 2026
    assert row["MES"] == 6
    assert row["DIA"] == 27
    assert row["NOMBRE_MES"] == "Junio"


def test_build_dim_tiempo_different_dates():
    df = pd.DataFrame({
        "fecha_hora": [
            pd.Timestamp("2026-01-01 10:00:00"),
            pd.Timestamp("2026-06-27 01:00:00"),
        ]
    })
    dim = build_dim_tiempo(df)
    assert len(dim) == 2
    assert dim["ID_TIEMPO"].tolist() == [20260101, 20260627]


def test_build_fact_fk_resolution(silver_df):
    dim_tipo = build_dim_tipo(silver_df)
    dim_distrito = build_dim_distrito(silver_df)
    dim_estado = build_dim_estado(silver_df)
    dim_tiempo = build_dim_tiempo(silver_df)
    fact = build_fact_emergencia(
        silver_df, dim_tipo, dim_distrito, dim_estado, dim_tiempo
    )
    assert len(fact) == 2
    assert list(fact.columns) == [
        "NRO_PARTE", "ID_TIPO", "ID_DISTRITO", "ID_ESTADO",
        "ID_TIEMPO", "TURNO", "MAQUINAS_COUNT", "LATITUD", "LONGITUD",
    ]
    assert fact["ID_TIPO"].iloc[0] == 1
    assert fact["ID_TIPO"].iloc[1] == 1


def test_build_fact_turno_calculation(silver_df):
    dim_tipo = build_dim_tipo(silver_df)
    dim_distrito = build_dim_distrito(silver_df)
    dim_estado = build_dim_estado(silver_df)
    dim_tiempo = build_dim_tiempo(silver_df)
    fact = build_fact_emergencia(
        silver_df, dim_tipo, dim_distrito, dim_estado, dim_tiempo
    )
    assert fact["TURNO"].iloc[0] == "Madrugada"
    assert fact["TURNO"].iloc[1] == "Mañana"


def test_build_fact_unknown_district_gets_minus_one(silver_df):
    dim_tipo = build_dim_tipo(silver_df)
    dim_distrito = build_dim_distrito(silver_df)
    dim_estado = build_dim_estado(silver_df)
    dim_tiempo = build_dim_tiempo(silver_df)
    extra = pd.DataFrame([{
        "nro_parte": "003",
        "fecha_hora": pd.Timestamp("2026-06-27 01:06:42"),
        "direccion": "AV. X",
        "distrito": "SIN_DISTRITO",
        "tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
        "tipo_categoria": "INCENDIO",
        "tipo_subcategoria": "VEHICULO",
        "tipo_detalle": "AUTOMOVIL",
        "estado": "ATENDIENDO",
        "maquinas": "M1",
        "maquinas_count": 1,
        "latitud": None,
        "longitud": None,
    }])
    df = pd.concat([silver_df, extra], ignore_index=True)
    fact = build_fact_emergencia(
        df, dim_tipo, dim_distrito, dim_estado, dim_tiempo
    )
    unknown = fact[fact["NRO_PARTE"] == "003"]
    assert unknown["ID_DISTRITO"].iloc[0] == -1


def test_transform_to_gold_returns_dict_with_5_tables(silver_df):
    records = silver_df.to_dict("records")
    for r in records:
        r["fecha_hora"] = r["fecha_hora"].isoformat()
    result = transform_to_gold(records)
    assert set(result.keys()) == {
        "DIM_TIPO", "DIM_DISTRITO", "DIM_ESTADO",
        "DIM_TIEMPO", "FACT_EMERGENCIA",
    }


def test_transform_to_gold_empty_silver_returns_empty_dict():
    result = transform_to_gold([])
    assert result == {}
