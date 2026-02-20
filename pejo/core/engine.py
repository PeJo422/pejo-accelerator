from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from pyspark.sql import functions as F

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
    SCD2_REQUIRED_COLUMNS = {
        "valid_from": "TIMESTAMP",
        "valid_to": "TIMESTAMP",
        "is_current": "BOOLEAN",
        "row_hash": "STRING",
        "business_key_hash": "STRING",
    }

    def __init__(
        self,
        spark,
        metadata: dict[str, dict[str, Any]],
        adapter,
        bronze_lakehouse: str | None = None,
        lakehouse_id: str | None = None,
    ):
        self.spark = spark
        self.metadata = metadata
        self.adapter = adapter
        self.bronze_lakehouse = bronze_lakehouse or lakehouse_id

    @classmethod
    def from_yaml_dir(
        cls,
        spark,
        adapter,
        schema_dir: str | Path,
        bronze_lakehouse: str | None = None,
        lakehouse_id: str | None = None,
    ) -> "Engine":
        metadata = load_metadata_from_yaml(schema_dir)
        return cls(
            spark=spark,
            metadata=metadata,
            adapter=adapter,
            bronze_lakehouse=bronze_lakehouse,
            lakehouse_id=lakehouse_id,
        )

    def run(self, table_name: str) -> None:
        logger = RunLogger(self.spark)
        logger.start(table_name)
        sql_statements: list[str] = []

        try:
            df, sql_statements, plan_status = self._plan_for_table(table_name)
            if plan_status == "SOURCE_NOT_FOUND":
                logger.end(
                    status="SKIPPED",
                    error_message="Source table not found",
                    rows_source=None,
                )
                return

            config = self.metadata[table_name]
            self._ensure_target_table(config["silver"], df)

            if str(config.get("scdtype", "SCD1")).upper() == "SCD2":
                self._ensure_scd2_target_columns(config["silver"])

            for statement in sql_statements:
                self.spark.sql(statement)

            logger.end(
                    status="SUCCESS", 
                    rows_source=df.count(),
                    executed_sql=";\n".join(sql_statements)
                    )
        except Exception as exc:
            logger.end(
                    status="FAILED", 
                    error_message=str(exc),
                    executed_sql=";\n".join(sql_statements)
                    )
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
        _df, sql_statements, plan_status = self._plan_for_table(table_name)
        if plan_status == "SOURCE_NOT_FOUND":
            raise ValueError(f"Source table not found for '{table_name}'")
        return DryRunResult(table=table_name, sql_statements=sql_statements)

    def validate_only(
        self,
        table_name: str | None = None,
        domain: str | None = None,
        table_names: list[str] | None = None,
    ) -> ValidationResult:
        tables = self._resolve_tables(table_name=table_name, domain=domain, table_names=table_names)
        for name in tables:
            _df, _sql, plan_status = self._plan_for_table(name)
            if plan_status == "SOURCE_NOT_FOUND":
                raise ValueError(f"Source table not found for '{name}'")
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

    def _plan_for_table(self, table_name: str) -> tuple[Any | None, list[str] | None, str]:
        config = self.metadata[table_name]

        bronze_table = self._resolve_bronze_table(config)
        if not self.spark.catalog.tableExists(bronze_table):
            return None, None, "SOURCE_NOT_FOUND"

        df = self.spark.table(bronze_table)
        scd_type = str(config.get("scdtype", "SCD1")).upper()

        # Incremental filtering (optional): derive watermark from silver, never from run logs.
        incremental_cfg = config.get("incremental") or {}
        incremental_enabled = bool(incremental_cfg.get("enabled", False))
        incremental_column = incremental_cfg.get("column")
        if incremental_enabled:
            if not incremental_column:
                raise ValueError(
                    f"Missing required incremental.column for table '{table_name}' when incremental.enabled=true"
                )
            incremental_column = str(incremental_column)
            if incremental_column not in df.columns:
                raise ValueError(
                    f"Incremental column '{incremental_column}' not found in source table '{bronze_table}'"
                )

            silver_table = config["silver"]
            watermark = None
            watermark = None

            if self.spark.catalog.tableExists(silver_table):
                silver_df = self.spark.table(silver_table)

                if "_pl_updated_at" in silver_df.columns:
                    watermark = (
                        silver_df
                        .agg(F.max("_pl_updated_at").alias("watermark"))
                        .collect()[0]["watermark"]
                    )

            if watermark is not None:
                df = df.filter(F.col(incremental_column) > F.lit(watermark))


        df = self.adapter.transform(df)
        df = self.adapter.apply_features(self.spark, df, config)
        df = apply_hashing_strategy(df, config)
        df.createOrReplaceTempView("source_view")

        columns = df.columns
        # Exclude source-only incremental watermark column from silver payload
        incremental_cfg = config.get("incremental") or {}
        incremental_enabled = bool(incremental_cfg.get("enabled", False))
        incremental_column = incremental_cfg.get("column")

        if incremental_enabled and incremental_column:
            incremental_column = str(incremental_column)
        if incremental_column in columns:
            columns = [c for c in columns if c != incremental_column]

        keys = config.get("primary_key") or []
        if not keys:
            raise ValueError(
            f"Missing required `primary_key` for table '{table_name}'. "
            "Define `primary_key` in schema."
            )
        column_aliases = {
            str(column_name): str(settings.get("alias"))
            for column_name, settings in (config.get("columns") or {}).items()
            if settings.get("alias")
        }

        load_type = str(config.get("load_type", "delta_merge")).lower()
        if load_type != "delta_merge":
            raise ValueError(f"Unsupported load_type '{load_type}'. Only 'delta_merge' is supported.")

        if scd_type == "SCD2":
            merge_sql = build_scd2_sql(
                target=config["silver"],
                source_view="source_view",
                keys=keys,
                columns=columns,
                column_aliases=column_aliases,
            )
            return df, [merge_sql], "OK"
        if scd_type == "SCD1":
            merge_sql = build_delta_merge_sql(
                target=config["silver"],
                source_view="source_view",
                keys=keys,
                columns=columns,
                soft_delete=config.get("soft_delete"),
                column_aliases=column_aliases,
            )
            return df, [merge_sql], "OK"

        raise ValueError(f"Unsupported scdtype '{scd_type}'. Use 'SCD1' or 'SCD2'.")

    def _resolve_bronze_table(self, config: dict[str, Any]) -> str:
        bronze_table = str(config["bronze"])
        if "." in bronze_table:
            return bronze_table

        lakehouse = self.bronze_lakehouse or config.get("bronze_lakehouse")
        if not lakehouse:
            return bronze_table

        return f"{lakehouse}.{bronze_table}"

    def _ensure_scd2_target_columns(self, target_table: str) -> None:
        target_df = self.spark.table(target_table)
        existing_columns: set[str] = set()

        schema = getattr(target_df, "schema", None)
        fields = getattr(schema, "fields", None) if schema is not None else None
        if fields:
            existing_columns = {str(field.name).lower() for field in fields}
        elif hasattr(target_df, "columns"):
            existing_columns = {str(column).lower() for column in target_df.columns}

        missing = [
            (column_name, data_type)
            for column_name, data_type in self.SCD2_REQUIRED_COLUMNS.items()
            if column_name.lower() not in existing_columns
        ]
        if not missing:
            return

        add_columns_sql = ", ".join([f"{column_name} {data_type}" for column_name, data_type in missing])
        self.spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({add_columns_sql})")

    def _ensure_target_table(self, target_table: str, df) -> None:
        if self.spark.catalog.tableExists(target_table):
            return

        bootstrap_df = self._build_target_bootstrap_df(df).limit(0)
        bootstrap_df.write.format("delta").mode("overwrite").saveAsTable(target_table)


    def _build_target_bootstrap_df(self, df):
        result = df
        if "business_key_hash" not in result.columns:
            result = result.withColumn("business_key_hash", F.lit(None).cast("string"))
        if "row_hash" not in result.columns:
            result = result.withColumn("row_hash", F.lit(None).cast("string"))
        if "valid_from" not in result.columns:
            result = result.withColumn("valid_from", F.current_timestamp())
        if "valid_to" not in result.columns:
            result = result.withColumn("valid_to", F.lit(None).cast("timestamp"))
        if "is_current" not in result.columns:
            result = result.withColumn("is_current", F.lit(True).cast("boolean"))
        if "_pl_updated_at" not in result.columns:
            result = result.withColumn("_pl_updated_at", F.current_timestamp())
        return result
