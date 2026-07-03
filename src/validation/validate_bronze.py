# src/validation/validate_bronze.py
import logging
import pandas as pd
import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableRowCountToBeBetween,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnsToMatchOrderedList,
)

logger = logging.getLogger(__name__)

COLUMNAS_ESPERADAS = [
    "NroParte",
    "Fecha_hora",
    "Direccion_distrito",
    "Tipo",
    "Estado",
    "Maquinas",
]


def validate_bronze(records: list[dict]) -> None:
    """
    Valida la calidad mínima de los datos antes de subir a Bronze.
    Lanza ValueError si alguna validación falla.
    """
    logger.info("Iniciando validación de calidad Bronze...")

    df = pd.DataFrame(records)

    context = gx.get_context()

    data_source = context.data_sources.add_pandas(name="accidents_source")
    data_asset = data_source.add_dataframe_asset(name="accidents_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = context.suites.add(
        gx.ExpectationSuite(name="accidents_bronze_suite")
    )

    # Regla 1 — al menos 1 registro
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=1))

    # Regla 2 — NroParte nunca nulo
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="NroParte"))

    # Regla 3 — Fecha_hora nunca nula
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="Fecha_hora"))

    # Regla 4 — los 6 campos siempre presentes
    suite.add_expectation(
        ExpectTableColumnsToMatchOrderedList(column_list=COLUMNAS_ESPERADAS)
    )

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="accidents_bronze_validation",
            data=batch_definition,
            suite=suite,
        )
    )

    result = validation_definition.run(batch_parameters={"dataframe": df})

    if not result.success:
        fallidas = [
            r.expectation_config.type
            for r in result.results
            if not r.success
        ]
        logger.error("Validación fallida. Reglas incumplidas: %s", fallidas)
        raise ValueError(f"Datos no pasan validación Bronze: {fallidas}")

    logger.info("✓ Validación Bronze exitosa — %d registros válidos", len(records))