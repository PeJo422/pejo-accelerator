from __future__ import annotations
from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_ENUM_METADATA_TABLE = "globaloptionsetmetadata"


def _normalize_one_mapping(raw: dict[str, Any], idx: int) -> dict[str, str]:
    column = raw.get("column")
    enum_name = raw.get("enum", raw.get("optionset"))
    mapping = raw.get("mapping") or {}

    if not column or not enum_name:
        raise ValueError(f"`enum[{idx}]` requires `column` and `enum`")
    if mapping and not isinstance(mapping, dict):
        raise ValueError(f"`enum[{idx}].mapping` must be a mapping")

    metadata_table = raw.get("metadata_table") or mapping.get("table") or DEFAULT_ENUM_METADATA_TABLE
    option_name_column = raw.get("option_name_column") or mapping.get("enum_column", "optionsetname")
    option_value_column = raw.get("option_value_column") or mapping.get("key_column", "optionvalue")
    option_label_column = raw.get("option_label_column") or mapping.get("label_column", "label")
    output_column = raw.get("output_column") or mapping.get("output_column", f"{column}_label")

    return {
        "column": str(column),
        "optionset": str(enum_name),
        "metadata_table": str(metadata_table),
        "option_name_column": str(option_name_column),
        "option_value_column": str(option_value_column),
        "option_label_column": str(option_label_column),
        "output_column": str(output_column),
    }


def normalize_enum_mappings(config: dict[str, Any]) -> list[dict[str, str]]:
    """Normalize enum mapping config from YAML.

    Supports both legacy `enums` and new `enum` attributes.
    """

    mappings = config.get("enum")
    if mappings is None:
        mappings = config.get("enums") or []

    if isinstance(mappings, dict):
        mappings = [mappings]
    if not isinstance(mappings, list):
        raise ValueError("`enum` must be a list (or mapping)")

    normalized: list[dict[str, str]] = []
    for idx, raw in enumerate(mappings):
        if not isinstance(raw, dict):
            raise ValueError(f"`enum[{idx}]` must be a mapping")

        normalized.append(_normalize_one_mapping(raw, idx))

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
