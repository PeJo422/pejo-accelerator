# PEJO Fabric Accelerator

PEJO acceleratorn kör metadata-styrda **Delta MERGE**-flöden i Fabric/Spark.

## YAML-scheman
Lägg schemafiler i en katalog, t.ex. `schemas/`.

### Exempel

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
```

## YAML-fält som stöds i koden

| Fält | Typ | Obligatorisk | Beteende |
|---|---|---:|---|
| `table` | `string` | Ja | Logiskt tabellnamn (nyckel i metadata). |
| `domain` | `string` | Ja | Används av `engine.run_domain(...)`. |
| `bronze` | `string` | Ja | Källa, läses via `spark.table(...)`. |
| `silver` | `string` | Ja | Target för Delta MERGE. |
| `primary_key` | `string` eller `list[string]` | Nej | Normaliseras till lista och används i MERGE `ON`. Om saknas används adapterns `default_primary_key()`. |
| `load_type` | `string` | Nej | Endast `delta_merge` stöds. Annat värde ger fel. |
| `scdtype` | `string` | Nej | Metadata-fält som normaliseras till uppercase (t.ex. `SCD2`). |
| `soft_delete.enabled` | `bool` | Nej | Om `true` läggs `WHEN MATCHED AND s.<column> = true THEN DELETE` till MERGE. |
| `soft_delete.column` | `string` | Nej | Kolumn för soft delete-villkoret. |
| `enums` | `list[mapping]` | Nej | Dynamics enum-lookup (GlobalOptionSetMetadata) före MERGE. |

## Dynamics enums (GlobalOptionSetMetadata)
Enum-logiken ligger i `pejo/adapters/dynamics.py` eftersom detta är Dynamics-specifik funktionalitet (inte generell core-logik).

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

### `enums`-fält
- `column` (obligatorisk): kolumn i källdatan med enumvärde.
- `optionset` (obligatorisk): Dynamics option set-namn.
- `metadata_table` (valfri, default `globaloptionsetmetadata`).
- `option_name_column` (valfri, default `optionsetname`).
- `option_value_column` (valfri, default `optionvalue`).
- `option_label_column` (valfri, default `label`).
- `output_column` (valfri, default `<column>_label`).

Engine gör då lookup mot metadata-tabellen och skapar label-kolumnen innan MERGE.

## Business key och hashing (status i nuvarande kod)
`business_key` och `hashing` stöds **inte** i nuvarande implementation.

Det betyder:
- YAML-fält som `business_key`, `hashes`, `hashing`, `hash_algorithm` används inte av engine i dagsläget.
- Inga hash-kolumner byggs automatiskt före MERGE.

Om ni vill kan vi lägga till detta i nästa steg med tydlig standard i YAML.

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

## Körningsloggar
Engine skriver körningsloggar till tabellen `pejo_run_log`.
Om tabellen saknas skapas den automatiskt (`CREATE TABLE IF NOT EXISTS ... USING DELTA`) innan körningen startar.

## Egna fält i YAML
Du kan lägga till extra metadata-fält per tabell.
Loadern behåller fälten i metadata, så de kan användas senare för t.ex. SCD-strategier eller framtida features.
