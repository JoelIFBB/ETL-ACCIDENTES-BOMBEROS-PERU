# tests/test_raw_to_storage.py
import json
from unittest.mock import patch

import pendulum
import pytest

from src.load.raw_to_storage import upload_raw_data


@pytest.fixture
def sample_records():
    return [
        {"NroParte": "001", "Estado": "ATENDIENDO"},
        {"NroParte": "002", "Estado": "CONTROLADO"},
    ]


@pytest.fixture
def ingestion_datetime():
    return pendulum.DateTime(2026, 6, 27, 10, 30, 0)


def test_creates_folder_and_file(tmp_path, sample_records, ingestion_datetime):
    with patch("src.load.raw_to_storage._BRONZE_PATH", str(tmp_path)):
        upload_raw_data(sample_records, ingestion_datetime)

    folder = tmp_path / "ingestion_date=2026-06-27"
    assert folder.exists()

    files = list(folder.iterdir())
    assert len(files) == 1
    assert files[0].name.startswith("accidentes_")
    assert files[0].name.endswith(".json")


def test_json_is_valid(tmp_path, sample_records, ingestion_datetime):
    with patch("src.load.raw_to_storage._BRONZE_PATH", str(tmp_path)):
        upload_raw_data(sample_records, ingestion_datetime)

    folder = tmp_path / "ingestion_date=2026-06-27"
    file_path = list(folder.iterdir())[0]
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert data == sample_records


def test_existing_folder_does_not_explode(tmp_path, sample_records,
                                          ingestion_datetime):
    folder = tmp_path / "ingestion_date=2026-06-27"
    folder.mkdir(parents=True)

    with patch("src.load.raw_to_storage._BRONZE_PATH", str(tmp_path)):
        upload_raw_data(sample_records, ingestion_datetime)

    files = list(folder.iterdir())
    assert len(files) == 1
