from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import warnings

from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F


SUPPORTED_HASH_ALGOS = {"sha2_256", "sha2_512"}
FORBIDDEN_TABLE_HASH_KEYS = {"hash_algorithm", "hash_separator", "hash_null_replacement"}


@dataclass(frozen=True)
class HashConfig:
    algorithm: str
    separator: str


def normalize_global_hash_config(platform_config: dict[str, Any]) -> HashConfig:
    hashing = platform_config.get("hashing") or {}
    if not isinstance(hashing, dict):
        raise ValueError("`hashing` in platform config must be a mapping")

    if "null_replacement" in hashing:
        raise ValueError(
            "Global hashing.null_replacement is not supported. Configure null handling per table/column in YAML."
        )

    algorithm = str(hashing.get("algorithm", "sha2_256")).lower()
    if algorithm not in SUPPORTED_HASH_ALGOS:
        supported = ", ".join(sorted(SUPPORTED_HASH_ALGOS))
        raise ValueError(f"Unsupported hashing.algorithm '{algorithm}'. Supported: {supported}")

    return HashConfig(
        algorithm=algorithm,
        separator=str(hashing.get("separator", "||")),
    )


def _normalize_column_settings(schema: dict[str, Any]) -> dict[str, dict[str, str]]:
    raw_columns = schema.get("columns") or []
    if not raw_columns:
        return {}
    if not isinstance(raw_columns, list):
        raise ValueError("`columns` must be a list")

    result: dict[str, dict[str, str]] = {}
    for idx, entry in enumerate(raw_columns):
        column_name: str
        cfg: dict[str, Any]
        if isinstance(entry, dict) and "column" in entry:
            column_name = str(entry["column"])
            cfg = {k: v for k, v in entry.items() if k != "column"}
        else:
            if not isinstance(entry, dict) or len(entry) != 1:
                raise ValueError(f"`columns[{idx}]` must be a single-key mapping or include `column`")
            column_name, cfg = next(iter(entry.items()))
            if not isinstance(cfg, dict):
                raise ValueError(f"`columns[{idx}].{column_name}` must be a mapping")

        null_handling = str(cfg.get("null_handling", "")).lower()
        if null_handling not in {"", "error", "warning", "replace"}:
            raise ValueError(
                f"`columns[{idx}].{column_name}.null_handling` must be one of: error, warning, replace"
            )

        normalized: dict[str, str] = {}
        if null_handling:
            normalized["null_handling"] = null_handling

        if null_handling == "replace":
            if "null_replacement" not in cfg:
                raise ValueError(
                    f"`columns[{idx}].{column_name}` with null_handling=replace requires `null_replacement`"
                )
            normalized["null_replacement"] = str(cfg.get("null_replacement"))

        if "alias" in cfg:
            normalized["alias"] = str(cfg.get("alias"))

        result[str(column_name)] = normalized

    return result


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
        "columns": _normalize_column_settings(schema),
        "hashing": hash_config,
    }


def _apply_null_handling(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    column_settings = config.get("columns") or {}
    if not column_settings:
        return df

    result = df
    for column_name, rules in column_settings.items():
        if column_name not in result.columns:
            raise ValueError(f"Column settings reference unknown source column '{column_name}'")

        mode = rules.get("null_handling", "")
        if not mode:
            continue

        has_nulls = result.filter(F.col(column_name).isNull()).limit(1).count() > 0
        if not has_nulls:
            continue

        if mode == "error":
            raise ValueError(f"Null value detected in column '{column_name}' with null_handling=error")
        if mode == "warning":
            warnings.warn(
                f"Null value detected in column '{column_name}' with null_handling=warning", UserWarning
            )
            continue
        if mode == "replace":
            replacement = str(rules.get("null_replacement", ""))
            result = result.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(replacement)))

    return result


def compute_hash(df: DataFrame, columns: list[str], config: HashConfig, replacements: dict[str, str]) -> Column:
    ordered_columns = sorted({str(c) for c in columns})
    processed_cols = [
        F.coalesce(F.col(col_name).cast("string"), F.lit(replacements.get(col_name, "")))
        for col_name in ordered_columns
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

    result = _apply_null_handling(df, config)
    replacement_map = {
        column_name: str(settings.get("null_replacement", ""))
        for column_name, settings in (config.get("columns") or {}).items()
    }

    if business_key:
        result = result.withColumn("business_key_hash", compute_hash(result, business_key, hash_config, replacement_map))

    if hash_columns:
        result = result.withColumn("row_hash", compute_hash(result, hash_columns, hash_config, replacement_map))

    return result
