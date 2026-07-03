# dags/dag_accidents.py
import json
import logging
import pendulum
import os
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException

from src.extract.scraper import fetch_html, scrape_website
from src.load.hash_to_storage import load_last_hash, save_hash
from src.load.raw_to_storage import upload_raw_data
from src.utils.hashing import calculate_df_hash
from src.validation.validate_bronze import validate_bronze
from src.transform.transform_silver import transform_to_silver
from src.load.silver_to_storage import upload_silver_data
from src.transform.transform_gold import transform_to_gold
from src.load.gold_to_storage import upload_gold_data

import pandas as pd
import psycopg2

URL    = os.getenv("SGNORTE_URL")
TMP_DIR = os.getenv("BOMBEROS_TMP_DIR", "/opt/airflow/data/tmp")

logger = logging.getLogger(__name__)


def _get_silver_activos() -> list[dict]:
    """
    Consulta Silver y retorna solo los registros vigentes (es_actual = TRUE).
    Gold solo analiza el estado actual — el historial queda en Silver.
    """
    conn = psycopg2.connect(
        host=os.getenv("BOMBEROS_DB_HOST"),
        port=int(os.getenv("BOMBEROS_DB_PORT", 5432)),
        dbname=os.getenv("BOMBEROS_DB_NAME"),
        user=os.getenv("BOMBEROS_DB_USER"),
        password=os.getenv("BOMBEROS_DB_PASSWORD"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nro_parte, fecha_hora, direccion, distrito,
                    tipo, tipo_categoria, tipo_subcategoria, tipo_detalle,
                    estado, maquinas, maquinas_count, latitud, longitud
                FROM accidents_silver
                WHERE es_actual = TRUE
            """)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


@dag(
    dag_id="pipeline_accidents",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["scraping", "bomberos"],
)
def pipeline_accidents():

    @task(retries=2, execution_timeout=timedelta(minutes=5))
    def task_fetch() -> str:
        logger.info("Iniciando descarga del HTML...")
        html = fetch_html(URL)
        tmp_path = f"{TMP_DIR}/html_{pendulum.now('UTC').strftime('%Y%m%d_%H%M%S')}.html"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(html)
        logger.info(f"HTML descargado correctamente — {len(html)} caracteres")
        return tmp_path

    @task(retries=2, execution_timeout=timedelta(minutes=5))
    def task_parse(tmp_path: str) -> str:
        """
        Lee el HTML y ejecuta el ÚNICO scrape_website() de todo el pipeline.
        Guarda los registros parseados como JSON y retorna esa ruta,
        para que el resto de tasks reutilicen el resultado en vez de
        volver a scrapear el HTML (evita el re-scraping x6).
        """
        logger.info("Leyendo HTML desde %s...", tmp_path)
        with open(tmp_path, "r", encoding="utf-8") as f:
            html = f.read()
        logger.info("Iniciando parseo del HTML")
        records = scrape_website(html)
        if not records:
            logger.warning("El parseo no devolvió registros.")
            raise ValueError("Lista de registros vacía.")
        logger.info("✓ %d registros extraídos correctamente", len(records))

        records_path = tmp_path.replace(".html", "_parsed.json")
        with open(records_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)
        logger.info("✓ Registros parseados guardados en: %s", records_path)

        return records_path

    @task(retries=1, execution_timeout=timedelta(minutes=2))
    def task_validate_bronze(records_path: str) -> str:
        """Validates minimum data quality before uploading to Bronze."""
        with open(records_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        validate_bronze(records)
        logger.info("✓ Validación Bronze exitosa")
        return records_path

    @task
    def task_hash_check(records_path: str) -> str:
        logger.info("Calculando hash del DataFrame...")
        with open(records_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        current_hash = calculate_df_hash(records)
        last_hash    = load_last_hash()
        logger.info(f"Hash actual:  {current_hash}")
        logger.info(f"Hash anterior: {last_hash}")
        if current_hash == last_hash:
            logger.info("Sin cambios detectados. Deteniendo pipeline.")
            raise AirflowSkipException("No data changes.")
        logger.info("✓ Nuevos datos detectados. Continuando pipeline.")
        return records_path

    @task(retries=2, execution_timeout=timedelta(minutes=5))
    def task_upload(records_path: str) -> None:
        """Reads parsed records and uploads them to Bronze (data/raw/)."""
        logger.info(f"Leyendo registros parseados desde {records_path}...")
        with open(records_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        ingestion_time = pendulum.now("UTC")
        upload_raw_data(records, ingestion_time)
        logger.info(f"✓ {len(records)} registros subidos a Bronze")

    @task(retries=2, execution_timeout=timedelta(minutes=10))
    def task_silver(records_path: str) -> dict:
        """Reads parsed records, transforms and loads into accidents_silver applying CDC."""
        with open(records_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        df     = transform_to_silver(records)
        result = upload_silver_data(df)
        logger.info(
            "✓ Silver — %d insertados, %d actualizados, %d ignorados",
            result["insertados"], result["actualizados"], result["ignorados"]
)
        return result

    @task(retries=2, execution_timeout=timedelta(minutes=10))
    def task_gold(_silver_result: dict) -> None:
        """
        Construye y carga el modelo estrella en Gold.
        Depende de task_silver para garantizar que Silver esté actualizado
        antes de leer los registros vigentes (es_actual = TRUE).
        """
        logger.info("Consultando registros activos de Silver...")
        silver_records = _get_silver_activos()

        if not silver_records:
            logger.warning("Silver no tiene registros activos — abortando Gold.")
            return

        logger.info("✓ %d registros activos obtenidos de Silver", len(silver_records))

        silver_df = pd.DataFrame(silver_records)
        silver_df["fecha_hora"] = pd.to_datetime(silver_df["fecha_hora"])

        gold_tables = transform_to_gold(silver_records)
        result      = upload_gold_data(gold_tables, silver_df)

        logger.info(
            "✓ Gold — FACT: %d insertados, %d actualizados",
            result["fact_insertados"], result["fact_actualizados"],
        )

    @task(retries=2, execution_timeout=timedelta(minutes=2))
    def task_save_hash(records_path: str) -> None:
        """Persists current hash to detect changes in next execution."""
        logger.info("Guardando hash de la ejecución actual...")
        with open(records_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        current_hash = calculate_df_hash(records)
        save_hash(current_hash)
        logger.info(f"✓ Hash guardado: {current_hash[:16]}...")

    @task(trigger_rule="all_done")
    def task_cleanup(tmp_path: str, records_path: str) -> None:
        """
        Deletes the temporary HTML file AND the parsed records JSON
        at the end of the pipeline. Always runs — whether the
        pipeline succeeded or failed.
        """
        for path in [tmp_path, records_path]:
            if os.path.exists(path):
                os.remove(path)
                logger.info(f"✓ Archivo temporal eliminado: {path}")
            else:
                logger.warning(f"Archivo no encontrado: {path}")

    # Pipeline dependencies
    tmp_path       = task_fetch()
    records_path   = task_parse(tmp_path)
    validated_path = task_validate_bronze(records_path)
    checked_path   = task_hash_check(validated_path)
    upload         = task_upload(checked_path)
    silver         = task_silver(checked_path)
    gold           = task_gold(silver)
    save           = task_save_hash(checked_path)
    upload >> silver >> gold >> save >> task_cleanup(tmp_path, records_path)


pipeline_accidents()