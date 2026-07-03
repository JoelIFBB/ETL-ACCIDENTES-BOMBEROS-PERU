# tests/test_hash_to_storage.py
import pytest
from unittest.mock import patch, MagicMock
from src.load.hash_to_storage import load_last_hash, save_hash


def test_load_primera_ejecucion():
    """Si no hay hash previo → devuelve None."""
    with patch("src.load.hash_to_storage.psycopg2.connect") as mock_connect, \
         patch("src.load.hash_to_storage._get_env", return_value="fake"):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        resultado = load_last_hash()

        assert resultado is None


def test_load_devuelve_ultimo_hash():
    """Si existe un hash previo → lo devuelve correctamente."""
    hash_esperado = "a" * 64

    with patch("src.load.hash_to_storage.psycopg2.connect") as mock_connect, \
         patch("src.load.hash_to_storage._get_env", return_value="fake"):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (hash_esperado,)
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        resultado = load_last_hash()

        assert resultado == hash_esperado


def test_save_hash_ejecuta_insert():
    """save_hash debe ejecutar un INSERT con el pipeline y el hash."""
    hash_value = "b" * 64

    with patch("src.load.hash_to_storage.psycopg2.connect") as mock_connect, \
         patch("src.load.hash_to_storage._get_env", return_value="fake"):

        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        save_hash(hash_value)

        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "INSERT" in call_args[0][0]
        assert "accidents" in call_args[0][1]
        assert hash_value in call_args[0][1]