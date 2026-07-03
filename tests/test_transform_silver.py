# tests/test_transform_silver.py
import pandas as pd
import pytest

from src.transform.transform_silver import (
    clean_text_column,
    column_todate,
    split_address,
    extract_coordinates,
    count_machineries,
    split_tipo,
    transform_to_silver,
)


def test_clean_text_column_strips_whitespace():
    df = pd.DataFrame({"col": ["  foo  ", "bar  ", "  baz"]})
    result = clean_text_column(df, "col")
    assert result["col"].tolist() == ["foo", "bar", "baz"]


def test_clean_text_column_handles_nan():
    df = pd.DataFrame({"col": ["  foo  ", None]})
    result = clean_text_column(df, "col")
    assert pd.isna(result["col"].iloc[1])


def test_column_todate_converts_string():
    df = pd.DataFrame({"dt": ["2026-06-27 01:06:42"]})
    result = column_todate(df, "dt")
    assert pd.api.types.is_datetime64_any_dtype(result["dt"])


def test_column_todate_mixed_format():
    df = pd.DataFrame({"dt": ["27/06/2026 01:06:42"]})
    result = column_todate(df, "dt")
    assert pd.api.types.is_datetime64_any_dtype(result["dt"])


def test_split_address_rsplit_on_last_hyphen():
    df = pd.DataFrame({"addr": ["AV. A - MIRAFLORES"]})
    result = split_address(df, "addr", "direccion", "distrito")
    assert result["direccion"].iloc[0] == "AV. A"
    assert result["distrito"].iloc[0] == "MIRAFLORES"


def test_split_address_with_coordinates_does_not_break():
    df = pd.DataFrame({
        "addr": [
            "AV. ANGELICA GAMARRA (-12.0049,-77.0632) Nro. 230 - LOS OLIVOS"
        ]
    })
    result = split_address(df, "addr", "direccion", "distrito")
    assert "(-12.0049,-77.0632)" in result["direccion"].iloc[0]
    assert result["distrito"].iloc[0] == "LOS OLIVOS"


def test_extract_coordinates_valid():
    df = pd.DataFrame({"dir": ["AV. A (-12.0049,-77.0632)"]})
    result = extract_coordinates(df, "dir")
    assert result["latitud"].iloc[0] == -12.0049
    assert result["longitud"].iloc[0] == -77.0632


def test_extract_coordinates_zero_zero_returns_none():
    df = pd.DataFrame({"dir": ["AV. A (0,0)"]})
    result = extract_coordinates(df, "dir")
    assert pd.isna(result["latitud"].iloc[0])
    assert pd.isna(result["longitud"].iloc[0])


def test_extract_coordinates_no_match_returns_none():
    df = pd.DataFrame({"dir": ["AV. A - MIRAFLORES"]})
    result = extract_coordinates(df, "dir")
    assert pd.isna(result["latitud"].iloc[0])
    assert pd.isna(result["longitud"].iloc[0])


def test_extract_coordinates_non_string_returns_none():
    df = pd.DataFrame({"dir": [None]})
    result = extract_coordinates(df, "dir")
    assert pd.isna(result["latitud"].iloc[0])
    assert pd.isna(result["longitud"].iloc[0])


def test_count_machineries_pipe_separated():
    df = pd.DataFrame({"M": ["M2-1 | AMB-176 | M176-1"]})
    result = count_machineries(df, "M")
    assert result["M_count"].iloc[0] == 3


def test_count_machineries_single():
    df = pd.DataFrame({"M": ["Autobomba 01"]})
    result = count_machineries(df, "M")
    assert result["M_count"].iloc[0] == 1


def test_count_machineries_null_returns_zero():
    df = pd.DataFrame({"M": [None]})
    result = count_machineries(df, "M")
    assert result["M_count"].iloc[0] == 0


def test_count_machineries_empty_string_returns_zero():
    df = pd.DataFrame({"M": [""]})
    result = count_machineries(df, "M")
    assert result["M_count"].iloc[0] == 0


def test_split_tipo_3_niveles():
    df = pd.DataFrame({"tipo": ["INCENDIO / VEHICULO / AUTOMOVIL"]})
    result = split_tipo(df, "tipo")
    assert result["tipo_categoria"].iloc[0] == "INCENDIO"
    assert result["tipo_subcategoria"].iloc[0] == "VEHICULO"
    assert result["tipo_detalle"].iloc[0] == "AUTOMOVIL"


def test_split_tipo_2_niveles_detalle_none():
    df = pd.DataFrame({"tipo": ["INCENDIO / VEHICULO"]})
    result = split_tipo(df, "tipo")
    assert result["tipo_categoria"].iloc[0] == "INCENDIO"
    assert result["tipo_subcategoria"].iloc[0] == "VEHICULO"
    assert pd.isna(result["tipo_detalle"].iloc[0])


def test_split_tipo_4_niveles_exceso_concatenado():
    df = pd.DataFrame({
        "tipo": ["INCENDIO / VEHICULO / AUTOMOVIL / VIA PUBLICA"]
    })
    result = split_tipo(df, "tipo")
    assert result["tipo_categoria"].iloc[0] == "INCENDIO"
    assert result["tipo_subcategoria"].iloc[0] == "VEHICULO"
    assert result["tipo_detalle"].iloc[0] == "AUTOMOVIL / VIA PUBLICA"


def test_transform_to_silver_full_pipeline():
    records = [{
        "NroParte": "2026022027",
        "Fecha_hora": "27/06/2026 01:06:42",
        "Direccion_distrito":
            "AV. A (-12.0049,-77.0632) - MIRAFLORES",
        "Tipo": "INCENDIO / VEHICULO / AUTOMOVIL",
        "Estado": "ATENDIENDO",
        "Maquinas": "M2-1 | AMB-176",
    }]
    result = transform_to_silver(records)
    assert len(result) == 1
    assert result["NroParte"].iloc[0] == "2026022027"
    assert result["direccion"].iloc[0] == "AV. A (-12.0049,-77.0632)"
    assert result["distrito"].iloc[0] == "MIRAFLORES"
    assert result["latitud"].iloc[0] == -12.0049
    assert result["longitud"].iloc[0] == -77.0632
    assert result["tipo_categoria"].iloc[0] == "INCENDIO"
    assert result["tipo_subcategoria"].iloc[0] == "VEHICULO"
    assert result["tipo_detalle"].iloc[0] == "AUTOMOVIL"
    assert result["Maquinas_count"].iloc[0] == 2
    assert "Direccion_distrito" not in result.columns
