from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SUPPORTED_HASH_ALGOS = {"sha2_256", "sha2_512", "md5"}


def normalize_hashing_config(schema: dict[str, Any]) -> dict[str, Any]:
    business_key = schema.get("business_key") or []
    hash_columns = schema.get("hash_columns") or []

    if isinstance(business_key, str):
        business_key = [business_key]
    if isinstance(hash_columns, str):
        hash_columns = [hash_columns]

    if not isinstance(business_key, list):
        raise ValueError("`business_key` must be a string or list of strings")
    if not isinstance(hash_columns, list):
        raise ValueError("`hash_columns` must be a string or list of strings")

    algorithm = str(schema.get("hash_algorithm", "sha2_256")).lower()
    if algorithm not in SUPPORTED_HASH_ALGOS:
        supported = ", ".join(sorted(SUPPORTED_HASH_ALGOS))
        raise ValueError(f"Unsupported hash_algorithm '{algorithm}'. Supported: {supported}")

    return {
        "business_key": [str(c) for c in business_key],
        "hash_columns": [str(c) for c in hash_columns],
        "hash_algorithm": algorithm,
        "hash_separator": str(schema.get("hash_separator", "||")),
        "hash_null_replacement": str(schema.get("hash_null_replacement", "")),
    }


def _build_hash_expr(columns: list[str], algorithm: str, separator: str, null_replacement: str):
    payload_columns = [F.coalesce(F.col(c).cast("string"), F.lit(null_replacement)) for c in columns]
    payload = F.concat_ws(separator, *payload_columns)

    if algorithm == "sha2_256":
        return F.sha2(payload, 256)
    if algorithm == "sha2_512":
        return F.sha2(payload, 512)
    if algorithm == "md5":
        return F.md5(payload)

    raise ValueError(f"Unsupported hash_algorithm '{algorithm}'")


def apply_hashing_strategy(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    business_key = config.get("business_key") or []
    hash_columns = config.get("hash_columns") or []

    if not business_key and not hash_columns:
        return df

    algorithm = str(config.get("hash_algorithm", "sha2_256")).lower()
    separator = str(config.get("hash_separator", "||"))
    null_replacement = str(config.get("hash_null_replacement", ""))

    result = df
    if business_key:
        result = result.withColumn(
            "business_key_hash",
            _build_hash_expr(business_key, algorithm, separator, null_replacement),
        )

    if hash_columns:
        result = result.withColumn(
            "row_hash",
            _build_hash_expr(hash_columns, algorithm, separator, null_replacement),
        )

    return result
