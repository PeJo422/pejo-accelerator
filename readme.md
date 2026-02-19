# PEJO Fabric Accelerator

Metadata-driven Spark/Delta framework for loading from Bronze to Silver.

## What Works
- Metadata loading from YAML (`schema_dir`) with recursive file discovery.
- Source/target full table paths built from `config.yml` + table names in YAML.
- SCD1 via Delta `MERGE`.
- SCD2 via single Delta `MERGE` (matched update + not matched insert).
- Enum enrichment via metadata mappings.
- Hashing (`business_key_hash`, `row_hash`) from global hashing config.
- Run logging to `pejo_run_log`.
- Source-not-found handling: run is logged as `SKIPPED`.

## Installation
Project dependency definition is in `pyproject.toml`.

Core dependencies:
- `pyspark`
- `pyyaml`

## Project Structure
- `pejo/`: framework code
- `metadata/`: platform config + table metadata
- `tests/`: test suite

## Configuration (`metadata/config.yml`)
Example:

```yaml
bronze_lakehouse: lh_bronze
silver_lakehouse: lh_silver
bronze_schema: dbo
silver_schema: dbo

hashing:
  algorithm: sha2_256
  separator: "||"
```

Path resolution:
- Bronze: `<bronze_lakehouse>.<bronze_schema>.<bronze_tablename>`
- Silver: `<silver_lakehouse>.<silver_schema>.<silver_tablename>`

## Table Metadata YAML
Required keys per table:
- `table`
- `domain`
- `bronze_tablename` (or legacy `bronze`)
- `silver_tablename` (or legacy `silver`)

Common keys:
- `primary_key`: string or list
- `scdtype`: `SCD1` or `SCD2`
- `business_key`: string or list
- `hash_columns`: string or list
- `soft_delete.enabled`, `soft_delete.column`
- `enum` / `enums` / `enum_columns`
- `columns` (null handling + alias)

Example:

```yaml
table: custtable
domain: masterdata
bronze_tablename: custtable
silver_tablename: custtable

primary_key:
  - recid
  - dataareaid

scdtype: scd2

business_key:
  - accountnum
  - dataareaid

hash_columns:
  - currency
  - blocked
```

## Runtime Behavior

### Source missing
If source table does not exist:
- run is not failed
- log row is written with:
  - `status = SKIPPED`
  - `error_message = Source table not found`
  - `rows_source = null`

### Target bootstrap
If target table does not exist:
- table is auto-created from source DataFrame
- bootstrap includes business columns + technical columns:
  - `business_key_hash`
  - `row_hash`
  - `valid_from`
  - `valid_to`
  - `is_current`

### SCD2 required columns
For `SCD2`, target is checked and missing columns are added via `ALTER TABLE ADD COLUMNS`:
- `valid_from TIMESTAMP`
- `valid_to TIMESTAMP`
- `is_current BOOLEAN`
- `row_hash STRING`
- `business_key_hash STRING`

## Supported Engine Commands

```python
from pejo import Engine, PEJOAdapter

engine = Engine.from_yaml_dir(
    spark=spark,
    adapter=PEJOAdapter(),
    schema_dir="./metadata",
)
```

Supported methods:
- `engine.run(table_name)`
- `engine.run_domain(domain)`
- `engine.run_table_list(table_names)`
- `engine.validate_only(table_name=..., domain=..., table_names=...)`
- `engine.dry_run(table_name)`

## Logging
Run log table: `pejo_run_log`

Columns:
- `run_id`
- `table_name`
- `start_time`
- `end_time`
- `rows_source`
- `status`
- `error_message`
- `executed_sql`

## Metadata Validation and Inspection
See `metadata/README.md` for a focused guide on checking metadata and running supported validation commands.
