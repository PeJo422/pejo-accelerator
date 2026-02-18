from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pejo.core.logging import RunLogger
from pejo.core.hashing import apply_hashing_strategy
from pejo.core.merge_builder import build_delta_merge_sql, build_scd2_sql
from pejo.schemas import load_metadata_from_yaml


@dataclass(frozen=True)
class DomainRunResult:
    domain: str
    tables: list[str]


@dataclass(frozen=True)
class TableListRunResult:
    tables: list[str]


@dataclass(frozen=True)
class DryRunResult:
    table: str
    sql_statements: list[str]


@dataclass(frozen=True)
class ValidationResult:
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
            df, sql_statements = self._plan_for_table(table_name)

            for statement in sql_statements:
                self.spark.sql(statement)

            logger.end(status="SUCCESS", rows_source=df.count())
        except Exception as exc:
            logger.end(status="FAILED", error_message=str(exc))
            raise

    def run_domain(self, domain: str) -> DomainRunResult:
        tables = self._resolve_tables(domain=domain)

        for table_name in tables:
            self.run(table_name)

        return DomainRunResult(domain=domain, tables=tables)

    def run_table_list(self, table_names: list[str]) -> TableListRunResult:
        tables = self._resolve_tables(table_names=table_names)
        for table_name in tables:
            self.run(table_name)
        return TableListRunResult(tables=tables)

    def dry_run(self, table_name: str) -> DryRunResult:
        _df, sql_statements = self._plan_for_table(table_name)
        return DryRunResult(table=table_name, sql_statements=sql_statements)

    def validate_only(
        self,
        table_name: str | None = None,
        domain: str | None = None,
        table_names: list[str] | None = None,
    ) -> ValidationResult:
        tables = self._resolve_tables(table_name=table_name, domain=domain, table_names=table_names)
        for name in tables:
            self._plan_for_table(name)
        return ValidationResult(tables=tables)

    def _resolve_tables(
        self,
        table_name: str | None = None,
        domain: str | None = None,
        table_names: list[str] | None = None,
    ) -> list[str]:
        specified = [table_name is not None, domain is not None, table_names is not None]
        if sum(specified) != 1:
            raise ValueError("Specify exactly one of: table_name, domain, table_names")

        if table_name is not None:
            if table_name not in self.metadata:
                raise ValueError(f"Unknown table '{table_name}'")
            return [table_name]

        if table_names is not None:
            if not table_names:
                raise ValueError("table_names cannot be empty")
            unknown = [name for name in table_names if name not in self.metadata]
            if unknown:
                raise ValueError(f"Unknown table(s): {', '.join(unknown)}")
            return table_names

        tables = [
            name
            for name, config in self.metadata.items()
            if str(config.get("domain", "")).lower() == str(domain).lower()
        ]
        if not tables:
            raise ValueError(f"No tables found for domain '{domain}'")
        return tables

    def _plan_for_table(self, table_name: str) -> tuple[Any, list[str]]:
        config = self.metadata[table_name]

        df = self.spark.table(config["bronze"])
        df = self.adapter.transform(df)
        df = self.adapter.apply_features(self.spark, df, config)
        df = apply_hashing_strategy(df, config)
        df.createOrReplaceTempView("source_view")

        columns = df.columns
        keys = config.get("primary_key") or self.adapter.default_primary_key()
        column_aliases = {
            str(column_name): str(settings.get("alias"))
            for column_name, settings in (config.get("columns") or {}).items()
            if settings.get("alias")
        }

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
                column_aliases=column_aliases,
            )
            return df, [update_sql, insert_sql]
        if scd_type == "SCD1":
            merge_sql = build_delta_merge_sql(
                target=config["silver"],
                source_view="source_view",
                keys=keys,
                columns=columns,
                soft_delete=config.get("soft_delete"),
                column_aliases=column_aliases,
            )
            return df, [merge_sql]

        raise ValueError(f"Unsupported scdtype '{scd_type}'. Use 'SCD1' or 'SCD2'.")
