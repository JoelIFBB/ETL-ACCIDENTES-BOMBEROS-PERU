# src/load/raw_to_storage.py
import json
import logging
import os
import pendulum

logger = logging.getLogger(__name__)

_BRONZE_PATH = os.getenv(
    "BOMBEROS_BRONZE_PATH",
    "/opt/airflow/data/bronze/accidentes",
)


def upload_raw_data(records: list[dict], ingestion_datetime: pendulum.DateTime) -> None:
    """
    Guarda los registros como JSON en el volumen Bronze.
    Organizado por fecha de ingesta.
    """
    try:
        ingestion_date = ingestion_datetime.strftime("%Y-%m-%d")
        timestamp      = ingestion_datetime.strftime("%Y%m%d_%H%M%S")

        folder_path = f"{_BRONZE_PATH}/ingestion_date={ingestion_date}"
        os.makedirs(folder_path, exist_ok=True)

        file_path = f"{folder_path}/accidentes_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logger.info("RAW guardado en: %s (%d registros)", file_path, len(records))

    except Exception as e:
        logger.error("Error guardando datos en Bronze: %s", e)
        raise   