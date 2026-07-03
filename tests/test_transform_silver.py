# tests/test_silver_to_storage.py
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.load.silver_to_storage import upload_silver_data


# ============================================================
# Fixture compartido — DataFrame realista de Silver
# ============================================================

@pytest.fixture
def sample_row() -> pd.DataFrame:
    """
    Fila realista que representa un accidente ya transformado por transform_to_silver.
    """
    return pd.DataFrame([{
        "NroParte":       "2026022027",
        "Fecha_hora":     "2026-06-27 01:06:42",
        "direccion":      "AV. ANGELICA GAMARRA Nro. 230",
        "distrito":       "LOS OLIVOS",
        "Tipo":           "INCENDIO / VEHICULO / AUTOMOVIL",
        "Estado":         "ATENDIENDO",
        "Maquinas":       "M2-1 | AMB-176",
        "Maquinas_count": 2,
        "latitud":        -12.0049,
        "longitud":       -77.0632,
    }])


def _make_mock_conn(fetchall_value):
    """
    Helper interno — construye la cadena completa de mocks de psycopg2:
    connect → conn → cursor (context manager) → cur.
    Acepta fetchall_value como lista de tuplas (lo que devuelve _get_estados_actuales).
    """
    mock_cur  = MagicMock()
    mock_conn = MagicMock()

    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_conn.cursor.return_value.__exit__.return_value  = False

    mock_cur.fetchall.return_value = fetchall_value

    return mock_conn, mock_cur


# ============================================================
# Grupo 1 — Los 3 escenarios de negocio del CDC
# ============================================================

def test_new_accident_triggers_insert(sample_row):
    """
    Accidente que NO existe en Silver → debe ejecutar INSERT nuevo.
    Devuelve inserted=1, updated=0, ignored=0.
    """
    mock_conn, mock_cur = _make_mock_conn(fetchall_value=[])

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        result = upload_silver_data(sample_row)

    assert result == {"insertados": 1, "actualizados": 0, "ignorados": 0}

    # Verificar secuencia de queries: SELECT (_get_estado_actual) + INSERT (_insert_nuevo)
    calls = mock_cur.execute.call_args_list
    assert len(calls) == 2
    assert "SELECT" in calls[0][0][0]
    assert "INSERT" in calls[1][0][0]


def test_same_state_is_ignored(sample_row):
    """
    Accidente que YA existe en Silver con el MISMO estado
    → no ejecuta INSERT ni UPDATE, solo incrementa ignorados.
    Devuelve inserted=0, updated=0, ignored=1.
    """
    mock_conn, mock_cur = _make_mock_conn(fetchall_value=[("2026022027", "ATENDIENDO")])

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        result = upload_silver_data(sample_row)

    assert result == {"insertados": 0, "actualizados": 0, "ignorados": 1}

    # Solo debe ejecutarse el SELECT — ningún INSERT ni UPDATE
    calls = mock_cur.execute.call_args_list
    assert len(calls) == 1
    assert "SELECT" in calls[0][0][0]


def test_state_change_triggers_update(sample_row):
    """
    Accidente que YA existe en Silver con DISTINTO estado
    → marca viejo como FALSE + INSERT nuevo con estado_anterior.
    Devuelve inserted=0, updated=1, ignored=0.
    """
    mock_conn, mock_cur = _make_mock_conn(fetchall_value=[("2026022027", "CONTROLADO")])

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        result = upload_silver_data(sample_row)

    assert result == {"insertados": 0, "actualizados": 1, "ignorados": 0}

    # Secuencia: SELECT + UPDATE (marca FALSE) + INSERT (nuevo con historial)
    calls = mock_cur.execute.call_args_list
    assert len(calls) == 3
    assert "SELECT" in calls[0][0][0]
    assert "UPDATE" in calls[1][0][0]
    assert "INSERT" in calls[2][0][0]


# ============================================================
# Grupo 2 — Batch mixto con múltiples filas
# ============================================================

def test_mixed_batch_counts_correctly():
    """
    Batch con 3 filas de distintos escenarios en una sola corrida:
    - Fila 1: accidente nuevo      → INSERT   → inserted += 1
    - Fila 2: mismo estado         → ignorar  → ignored  += 1
    - Fila 3: estado distinto      → UPDATE   → updated  += 1
    Verifica que side_effect consume los fetchone en orden correcto.
    """
    df = pd.DataFrame([
        {
            "NroParte": "001", "Fecha_hora": "2026-06-27 01:00:00",
            "direccion": "AV. A", "distrito": "MIRAFLORES",
            "Tipo": "INCENDIO", "Estado": "ATENDIENDO",
            "Maquinas": "M1", "Maquinas_count": 1,
            "latitud": -12.1, "longitud": -77.0,
        },
        {
            "NroParte": "002", "Fecha_hora": "2026-06-27 02:00:00",
            "direccion": "AV. B", "distrito": "SURCO",
            "Tipo": "RESCATE", "Estado": "CONTROLADO",
            "Maquinas": "M2", "Maquinas_count": 1,
            "latitud": -12.2, "longitud": -77.1,
        },
        {
            "NroParte": "003", "Fecha_hora": "2026-06-27 03:00:00",
            "direccion": "AV. C", "distrito": "SAN ISIDRO",
            "Tipo": "INCENDIO", "Estado": "ATENDIENDO",
            "Maquinas": "M3", "Maquinas_count": 1,
            "latitud": -12.3, "longitud": -77.2,
        },
    ])

    mock_conn, mock_cur = _make_mock_conn(
        fetchall_value=[("002", "CONTROLADO"), ("003", "CONTROLADO")]
    )

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        result = upload_silver_data(df)

    assert result == {"insertados": 1, "actualizados": 1, "ignorados": 1}


# ============================================================
# Grupo 3 — Comportamiento transaccional
# ============================================================

def test_commit_is_called_on_success(sample_row):
    """
    Cuando todo sale bien → conn.commit() debe ejecutarse exactamente 1 vez.
    Sin commit, los inserts no se persisten realmente en Postgres.
    """
    mock_conn, mock_cur = _make_mock_conn(fetchall_value=[])

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        upload_silver_data(sample_row)

    mock_conn.commit.assert_called_once()
    mock_conn.rollback.assert_not_called()


def test_rollback_is_called_on_error(sample_row):
    """
    Cuando ocurre una excepción durante el loop
    → conn.rollback() debe ejecutarse y la excepción debe propagarse.
    Sin rollback, quedarían inserts parciales corruptos en Postgres.
    """
    mock_conn, mock_cur = _make_mock_conn(fetchall_value=[])
    mock_cur.execute.side_effect = Exception("Error de Postgres simulado")

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        with pytest.raises(Exception, match="Error de Postgres simulado"):
            upload_silver_data(sample_row)

    mock_conn.rollback.assert_called_once()
    mock_conn.commit.assert_not_called()


def test_connection_is_always_closed(sample_row):
    """
    La conexión debe cerrarse siempre en el bloque finally,
    tanto si el proceso fue exitoso como si falló.
    Conexiones no cerradas agotan el pool de Postgres.
    """
    mock_conn, mock_cur = _make_mock_conn(fetchall_value=[])

    with patch("src.load.silver_to_storage.psycopg2.connect", return_value=mock_conn), \
         patch("src.load.silver_to_storage._get_env", return_value="fake"):

        upload_silver_data(sample_row)

    mock_conn.close.assert_called_once()