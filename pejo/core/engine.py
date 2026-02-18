from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pejo.core.logging import RunLogger
from pejo.core.merge_builder import build_merge_sql
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

            df.createOrReplaceTempView("source_view")

            columns = df.columns
            keys = config.get("primary_key") or self.adapter.default_primary_key()

            merge_sql = build_merge_sql(
                target=config["silver"],
                source_view="source_view",
                keys=keys,
                columns=columns,
                soft_delete=config.get("soft_delete"),
            )

            self.spark.sql(merge_sql)
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
