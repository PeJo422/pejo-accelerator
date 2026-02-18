from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_ENUM_METADATA_TABLE = "globaloptionsetmetadata"


def normalize_enum_mappings(config: dict[str, Any]) -> list[dict[str, str]]:
    """Normalize enum mapping config from YAML for Dataverse/Dynamics sources."""

    mappings = config.get("enums") or []
    if not isinstance(mappings, list):
        raise ValueError("`enums` must be a list")

    normalized: list[dict[str, str]] = []
    for idx, raw in enumerate(mappings):
        if not isinstance(raw, dict):
            raise ValueError(f"`enums[{idx}]` must be a mapping")

        column = raw.get("column")
        optionset = raw.get("optionset")
        if not column or not optionset:
            raise ValueError(f"`enums[{idx}]` requires `column` and `optionset`")

        normalized.append(
            {
                "column": str(column),
                "optionset": str(optionset),
                "metadata_table": str(raw.get("metadata_table", DEFAULT_ENUM_METADATA_TABLE)),
                "option_name_column": str(raw.get("option_name_column", "optionsetname")),
                "option_value_column": str(raw.get("option_value_column", "optionvalue")),
                "option_label_column": str(raw.get("option_label_column", "label")),
                "output_column": str(raw.get("output_column", f"{column}_label")),
            }
        )

    return normalized


def apply_enum_mappings(
    spark: SparkSession,
    df: DataFrame,
    enum_mappings: list[dict[str, str]],
) -> DataFrame:
    """Join Dataverse/Dynamics GlobalOptionSet metadata to produce label columns."""

    result = df
    for idx, mapping in enumerate(enum_mappings):
        alias = f"enum_{idx}"
        meta = (
            spark.table(mapping["metadata_table"])
            .select(
                F.col(mapping["option_name_column"]).alias("_enum_name"),
                F.col(mapping["option_value_column"]).alias("_enum_value"),
                F.col(mapping["option_label_column"]).alias("_enum_label"),
            )
            .alias(alias)
        )

        name_col = f"{alias}._enum_name"
        value_col = f"{alias}._enum_value"
        label_col = f"{alias}._enum_label"

        condition = (
            (F.col(name_col) == F.lit(mapping["optionset"]))
            & (F.col(value_col) == F.col(mapping["column"]))
        )

        result = (
            result.join(meta, condition, "left")
            .withColumn(mapping["output_column"], F.col(label_col))
            .drop("_enum_name", "_enum_value", "_enum_label")
        )

    return result
