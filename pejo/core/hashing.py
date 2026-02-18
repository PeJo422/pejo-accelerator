from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F


SUPPORTED_HASH_ALGOS = {"sha2_256", "sha2_512"}
FORBIDDEN_TABLE_HASH_KEYS = {"hash_algorithm", "hash_separator", "hash_null_replacement"}


@dataclass(frozen=True)
class HashConfig:
    algorithm: str
    separator: str
    null_replacement: str


def normalize_global_hash_config(platform_config: dict[str, Any]) -> HashConfig:
    hashing = platform_config.get("hashing") or {}
    if not isinstance(hashing, dict):
        raise ValueError("`hashing` in platform config must be a mapping")

    algorithm = str(hashing.get("algorithm", "sha2_256")).lower()
    if algorithm not in SUPPORTED_HASH_ALGOS:
        supported = ", ".join(sorted(SUPPORTED_HASH_ALGOS))
        raise ValueError(f"Unsupported hashing.algorithm '{algorithm}'. Supported: {supported}")

    return HashConfig(
        algorithm=algorithm,
        separator=str(hashing.get("separator", "||")),
        null_replacement=str(hashing.get("null_replacement", "")),
    )


def normalize_hashing_config(schema: dict[str, Any], hash_config: HashConfig) -> dict[str, Any]:
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

    forbidden = FORBIDDEN_TABLE_HASH_KEYS.intersection(schema.keys())
    if forbidden:
        keys = ", ".join(sorted(forbidden))
        raise ValueError(
            f"Table-level hashing overrides are not allowed: {keys}. Configure under platform.yaml -> hashing."
        )

    return {
        "business_key": [str(c) for c in business_key],
        "hash_columns": [str(c) for c in hash_columns],
        "hashing": hash_config,
    }


def compute_hash(df: DataFrame, columns: list[str], config: HashConfig) -> Column:
    ordered_columns = sorted({str(c) for c in columns})
    processed_cols = [
        F.coalesce(F.col(col_name).cast("string"), F.lit(config.null_replacement)) for col_name in ordered_columns
    ]
    payload = F.concat_ws(config.separator, *processed_cols)

    if config.algorithm == "sha2_256":
        return F.sha2(payload, 256)
    if config.algorithm == "sha2_512":
        return F.sha2(payload, 512)

    raise ValueError(f"Unsupported hashing algorithm '{config.algorithm}'")


def apply_hashing_strategy(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    business_key = config.get("business_key") or []
    hash_columns = config.get("hash_columns") or []
    hash_config = config.get("hashing")

    if not business_key and not hash_columns:
        return df
    if not isinstance(hash_config, HashConfig):
        raise ValueError("Missing normalized global hashing config on table metadata")

    result = df
    if business_key:
        result = result.withColumn("business_key_hash", compute_hash(result, business_key, hash_config))

    if hash_columns:
        result = result.withColumn("row_hash", compute_hash(result, hash_columns, hash_config))

    return result
