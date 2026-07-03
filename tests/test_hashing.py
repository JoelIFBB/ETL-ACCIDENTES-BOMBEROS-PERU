# tests/test_hashing.py
import pytest
from src.utils.hashing import calculate_df_hash


def test_mismo_hash_mismo_orden():
    """Los mismos datos producen siempre el mismo hash."""
    datos = [
        {"NroParte": "001", "Tipo": "Incendio"},
        {"NroParte": "002", "Tipo": "Rescate"},
    ]
    assert calculate_df_hash(datos) == calculate_df_hash(datos)


def test_mismo_hash_orden_distinto():
    """Mismos datos en orden distinto producen el mismo hash."""
    datos_a = [
        {"NroParte": "001", "Tipo": "Incendio"},
        {"NroParte": "002", "Tipo": "Rescate"},
    ]
    datos_b = [
        {"NroParte": "002", "Tipo": "Rescate"},
        {"NroParte": "001", "Tipo": "Incendio"},
    ]
    assert calculate_df_hash(datos_a) == calculate_df_hash(datos_b)


def test_hash_distinto_datos_distintos():
    """Datos distintos producen hashes distintos."""
    datos_a = [{"NroParte": "001", "Tipo": "Incendio"}]
    datos_b = [{"NroParte": "001", "Tipo": "Rescate"}]
    assert calculate_df_hash(datos_a) != calculate_df_hash(datos_b)


def test_lista_vacia_no_explota():
    """Una lista vacía no lanza excepción."""
    resultado = calculate_df_hash([])
    assert isinstance(resultado, str)
    assert len(resultado) == 64  # SHA256 siempre produce 64 caracteres