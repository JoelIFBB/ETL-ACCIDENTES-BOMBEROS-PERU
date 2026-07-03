# tests/test_validate_bronze.py
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules["great_expectations"] = MagicMock()
sys.modules["great_expectations.expectations"] = MagicMock()

from src.validation.validate_bronze import validate_bronze


@pytest.fixture
def valid_records():
    return [{
        "NroParte": "2026022027",
        "Fecha_hora": "27/06/2026 01:06:42",
        "Direccion_distrito": "AV. A - MIRAFLORES",
        "Tipo": "INCENDIO",
        "Estado": "ATENDIENDO",
        "Maquinas": "M2-1 | AMB-176",
    }]


def _mock_gx_context(success):
    mock_result = MagicMock()
    mock_result.success = success

    mock_validation_def = MagicMock()
    mock_validation_def.run.return_value = mock_result

    mock_context = MagicMock()
    mock_context.validation_definitions.add.return_value = mock_validation_def
    mock_context.suites.add.return_value = MagicMock()
    mock_context.data_sources.add_pandas.return_value = MagicMock()
    mock_context.data_sources.add_pandas.return_value.add_dataframe_asset \
        .return_value.add_batch_definition_whole_dataframe \
        .return_value.get_batch.return_value = MagicMock()

    return mock_context


def test_valid_data_passes(valid_records):
    mock_context = _mock_gx_context(success=True)
    with patch("src.validation.validate_bronze.gx.get_context",
               return_value=mock_context):
        validate_bronze(valid_records)


def test_empty_list_raises():
    mock_context = _mock_gx_context(success=False)
    mock_context.validation_definitions.add.return_value.run.return_value \
        .results = [MagicMock()]
    mock_context.validation_definitions.add.return_value.run.return_value \
        .results[0].success = False
    mock_context.validation_definitions.add.return_value.run.return_value \
        .results[0].expectation_config.type = \
        "expect_table_row_count_to_be_between"

    with patch("src.validation.validate_bronze.gx.get_context",
               return_value=mock_context):
        with pytest.raises(ValueError,
                           match="expect_table_row_count_to_be_between"):
            validate_bronze([])
