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
| `scdtype` | `string` | Nej | Styr beteende: `SCD1` (default, Delta MERGE) eller `SCD2` (historisering med `valid_from`, `valid_to`, `is_current`). |
| `soft_delete.enabled` | `bool` | Nej | Om `true` läggs `WHEN MATCHED AND s.<column> = true THEN DELETE` till MERGE. |
| `soft_delete.column` | `string` | Nej | Kolumn för soft delete-villkoret. |
| `enums` | `list[mapping]` | Nej | Dynamics enum-lookup (GlobalOptionSetMetadata) före MERGE. |
| `enum_columns` | `mapping` | Nej | Kortform för enum-mappningar per kolumn (normaliseras till `enums`). |
| `business_key` | `string` eller `list[string]` | Nej | Kolumner för business key-hash (`business_key_hash`). |
| `hash_columns` | `string` eller `list[string]` | Nej | Kolumner för rad-hash (`row_hash`). |
| `hash_algorithm` | `string` | Nej | `sha2_256` (default), `sha2_512` eller `md5`. |


## SCD-beteende (implementerat)

### `scdtype: SCD1` (default)
- Kör vanlig Delta MERGE (`WHEN MATCHED UPDATE`, `WHEN NOT MATCHED INSERT`).
- `soft_delete` stöds i detta läge.

### `scdtype: SCD2`
- Kör två SQL-steg:
  1. `UPDATE` som stänger nuvarande version (`is_current=false`, `valid_to=current_timestamp()`) när någon tracked kolumn ändrats.
  2. `INSERT` av ny version med `valid_from=current_timestamp()`, `valid_to=NULL`, `is_current=true` för nya eller ändrade rader.
- Förändringsdetektion görs på alla icke-nyckelkolumner i källdatan.

### Krav för SCD2-target
Target-tabellen behöver ha kolumnerna:
- `valid_from` (timestamp)
- `valid_to` (timestamp, nullable)
- `is_current` (boolean)

## Dynamics enums (GlobalOptionSetMetadata)
Enum-logiken ligger i `pejo/adapters/dataverse.py` eftersom detta är Dynamics-specifik funktionalitet (inte generell core-logik).

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

## Business key och hashing (implementerat)

Ni kan nu konfigurera hash-strategi i YAML, t.ex.:

```yaml
business_key:
  - salesid
  - dataareaid

hash_columns:
  - salesid
  - custaccount
  - salesstatus
  - invoiceaccount
  - modifieddatetime

hash_algorithm: sha2_256   # valfritt (sha2_256|sha2_512|md5)
hash_separator: "||"        # valfritt
hash_null_replacement: ""   # valfritt
```

Engine skapar då:
- `business_key_hash` från `business_key`
- `row_hash` från `hash_columns`

Detta görs efter enum-berikning och före MERGE/SCD2-SQL.

## Exempel enligt Finance-mönstret

```yaml
table: SalesTable
domain: finance
bronze: bronze_salestable
silver: silver_salestable

primary_key:
  - recid
  - dataareaid

business_key:
  - salesid
  - dataareaid

hash_columns:
  - salesid
  - custaccount
  - salesstatus
  - invoiceaccount
  - modifieddatetime

enum_columns:
  salesstatus:
    metadata_table: bronze_enum_metadata
    optionset: salesstatus
    key_column: option
    label_column: localizedlabel

soft_delete:
  enabled: true
  column: isdeleted
```

`enum_columns` är en kortform som normaliseras till vanliga `enums` internt.


## Exempelmetadata i repo
Det finns färdiga YAML-exempel i `metadata/finance/`:
- `custtable.yml` (dimension/SCD2 + hashing)
- `fact_salestable.yml` (fakta/SCD1 + enums + soft delete)

Använd `schema_dir="./metadata/finance"` i `Engine.from_yaml_dir(...)`.

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
