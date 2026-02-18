# PEJO Fabric Accelerator

PEJO acceleratorn kör metadata-styrda **Delta MERGE**-flöden i Fabric/Spark.

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
    load_type: delta_merge
    scdtype: SCD2

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
    load_type: delta_merge
    scdtype: SCD2
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


## Egna fält i YAML
Du kan lägga till extra metadata-fält per tabell (t.ex. `scdtype: SCD2`).
Loadern behåller fälten i metadata, så de kan användas senare för t.ex. SCD-strategier.

> Nuvarande körning använder Delta MERGE och accepterar `scdtype` som metadata.


## Dynamics enums (GlobalOptionSetMetadata)
För att mappa enum-koder till labels från Dynamics, lägg till `enums` i tabellens YAML:

```yaml
tables:
  - table: SalesOrder
    domain: Sales
    bronze: bronze.sales.salesorder
    silver: silver.sales.salesorder
    primary_key: [recid]
    load_type: delta_merge
    enums:
      - column: salesstatus
        optionset: SalesStatus
        metadata_table: bronze.crm.globaloptionsetmetadata
        option_name_column: optionsetname
        option_value_column: optionvalue
        option_label_column: label
        output_column: salesstatus_label
```

Engine gör då en lookup mot metadata-tabellen och skapar label-kolumnen
(t.ex. `salesstatus_label`) innan MERGE körs.
