# Metadata Guide

This file describes how to validate and inspect metadata used by the PEJO engine.

## Files
- Platform config: `metadata/config.yml`
- Table metadata: `metadata/**/*.yml`

## Required per table
- `table`
- `domain`
- `bronze_tablename` (or legacy `bronze`)
- `silver_tablename` (or legacy `silver`)

## Recommended checks before running
1. `primary_key` is present and correct.
2. `scdtype` is `SCD1` or `SCD2`.
3. `business_key`/`hash_columns` are aligned with model requirements.
4. Enum mappings point to valid metadata table/columns.
5. `config.yml` has expected lakehouse/schema values per environment.

## Supported validation/run commands

### In notebook / Python

```python
from pejo import Engine, PEJOAdapter, load_metadata_from_yaml

# Load and inspect normalized metadata
metadata = load_metadata_from_yaml("./metadata")
print(metadata["custtable"]["bronze"])
print(metadata["custtable"]["silver"])

engine = Engine.from_yaml_dir(spark=spark, adapter=PEJOAdapter(), schema_dir="./metadata")

# Validate only (no writes)
engine.validate_only(table_name="custtable")
engine.validate_only(domain="sales")
engine.validate_only(table_names=["custtable", "salestable"])

# Dry-run SQL
dry = engine.dry_run("custtable")
for sql in dry.sql_statements:
    print(sql)

# Execute
engine.run("custtable")
engine.run_domain("sales")
engine.run_table_list(["custtable", "salestable"])
```

### In terminal (metadata inspection)

```powershell
# List metadata files
Get-ChildItem metadata -Recurse -Filter *.yml

# Find all tablename keys
rg -n "bronze_tablename|silver_tablename" metadata

# Find legacy keys still in use
rg -n "^bronze:|^silver:" metadata

# Find SCD2 tables
rg -n "scdtype:\s*scd2|scdtype:\s*SCD2" metadata

# Find tables without primary_key
rg -n "^primary_key:" metadata
```

## Environment-specific behavior
Full table paths are resolved from `metadata/config.yml`:
- Bronze path: `<bronze_lakehouse>.<bronze_schema>.<bronze_tablename>`
- Silver path: `<silver_lakehouse>.<silver_schema>.<silver_tablename>`

This allows same tablename in Bronze and Silver as long as lakehouse/schema differ.

## Operational notes
- Missing source table => run is logged as `SKIPPED`.
- Missing target table => auto-bootstrap from source DataFrame.
- For `SCD2`, required technical columns are auto-added if missing.
