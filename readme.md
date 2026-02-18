# PEJO Fabric Accelerator

PEJO acceleratorn kör metadata-styrda merge-flöden i Fabric/Spark.

## YAML-scheman
Lägg schemafiler i en katalog, t.ex. `schemas/`:

```yaml
# schemas/sales.yml
tables:
  - table: CustTable
    domain: Sales
    bronze: bronze.sales.custtable
    silver: silver.sales.custtable
    primary_key: [recid, dataareaid]

  - table: SalesTable
    domain: Sales
    bronze: bronze.sales.salestable
    silver: silver.sales.salestable
    primary_key:
      - recid
      - dataareaid
    soft_delete:
      enabled: true
      column: isdeleted
```

## Notebook-användning

```python
from pejo import Engine, PEJOAdapter

adapter = PEJOAdapter()
engine = Engine.from_yaml_dir(spark=spark, adapter=adapter, schema_dir="./schemas")

# Kör en tabell
engine.run("CustTable")

# Kör en hel domän
engine.run_domain("Sales")
```
