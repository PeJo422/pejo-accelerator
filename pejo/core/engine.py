from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pejo.core.logging import RunLogger
from pejo.core.hashing import apply_hashing_strategy
from pejo.core.merge_builder import build_delta_merge_sql, build_scd2_sql
from pejo.adapters.dynamics import apply_enum_mappings
from pejo.schemas import load_metadata_from_yaml


@dataclass(frozen=True)
class DomainRunResult:
    domain: str
    tables: list[str]


class Engine:
    """Execution engine for table and domain runs."""

    def __init__(self, spark, metadata: dict[str, dict[str, Any]], adapter):
        self.spark = spark
        self.metadata = metadata
        self.adapter = adapter

    @classmethod
    def from_yaml_dir(cls, spark, adapter, schema_dir: str | Path) -> "Engine":
        metadata = load_metadata_from_yaml(schema_dir)
        return cls(spark=spark, metadata=metadata, adapter=adapter)

    def run(self, table_name: str) -> None:
        logger = RunLogger(self.spark)
        logger.start(table_name)

        try:
            config = self.metadata[table_name]

            df = self.spark.table(config["bronze"])
            df = self.adapter.transform(df)

            enum_mappings = config.get("enums") or []
            if enum_mappings:
                df = apply_enum_mappings(self.spark, df, enum_mappings)

            df = apply_hashing_strategy(df, config)

            df.createOrReplaceTempView("source_view")

            columns = df.columns
            keys = config.get("primary_key") or self.adapter.default_primary_key()

            load_type = str(config.get("load_type", "delta_merge")).lower()
            if load_type != "delta_merge":
                raise ValueError(f"Unsupported load_type '{load_type}'. Only 'delta_merge' is supported.")

            scd_type = str(config.get("scdtype", "SCD1")).upper()
            if scd_type == "SCD2":
                update_sql, insert_sql = build_scd2_sql(
                    target=config["silver"],
                    source_view="source_view",
                    keys=keys,
                    columns=columns,
                )
                self.spark.sql(update_sql)
                self.spark.sql(insert_sql)
            elif scd_type == "SCD1":
                merge_sql = build_delta_merge_sql(
                    target=config["silver"],
                    source_view="source_view",
                    keys=keys,
                    columns=columns,
                    soft_delete=config.get("soft_delete"),
                )
                self.spark.sql(merge_sql)
            else:
                raise ValueError(f"Unsupported scdtype '{scd_type}'. Use 'SCD1' or 'SCD2'.")
            logger.end(status="SUCCESS", rows_source=df.count())
        except Exception as exc:
            logger.end(status="FAILED", error_message=str(exc))
            raise

    def run_domain(self, domain: str) -> DomainRunResult:
        tables = [
            table_name
            for table_name, config in self.metadata.items()
            if str(config.get("domain", "")).lower() == str(domain).lower()
        ]

        if not tables:
            raise ValueError(f"No tables found for domain '{domain}'")

        for table_name in tables:
            self.run(table_name)

        return DomainRunResult(domain=domain, tables=tables)
