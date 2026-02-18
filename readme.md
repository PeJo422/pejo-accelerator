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
| `enum` | `list[mapping]` eller `mapping` | Nej | Enum-lookup före MERGE. Stödjer flera mappingar. |
| `enums`/`enum_columns` | legacy | Nej | Bakåtkompatibla alias till `enum`. |
| `business_key` | `string` eller `list[string]` | Nej | Kolumner för business key-hash (`business_key_hash`). |
| `hash_columns` | `string` eller `list[string]` | Nej | Kolumner för rad-hash (`row_hash`). |
| `hash_algorithm` | `string` | Nej | **Ej tillåten per tabell**. Sätts globalt i `config.yml` (eller `platform.yaml`) under `hashing.algorithm`. |
| `columns` | `list[mapping]` | Nej | Kolumnregler, t.ex. `null_handling: error|warning|replace` och `null_replacement` för `replace`. |


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

## Enums via YAML
Enum-logiken appliceras av den globala adaptern `PEJOAdapter` och styrs helt via YAML.

För att mappa enum-koder till labels, lägg till `enum` i tabellens YAML:

```yaml
tables:
  - table: SalesOrder
    domain: Sales
    bronze: bronze.sales.salesorder
    silver: silver.sales.salesorder
    primary_key: [recid]
    load_type: delta_merge
    enum:
      - column: salesstatus
        enum: SalesStatus
        mapping:
          table: bronze.crm.globaloptionsetmetadata
          enum_column: optionsetname
          key_column: optionvalue
          label_column: label
          output_column: salesstatus_label
```

### `enum`-fält
- `column` (obligatorisk): kolumn i källdatan med enumvärde.
- `enum` (obligatorisk): enum/optionset-namn.
- `mapping.table` (valfri, default `globaloptionsetmetadata`).
- `mapping.enum_column` (valfri, default `optionsetname`).
- `mapping.key_column` (valfri, default `optionvalue`).
- `mapping.label_column` (valfri, default `label`).
- `mapping.output_column` (valfri, default `<column>_label`).

Engine gör då lookup mot metadata-tabellen och skapar label-kolumnen innan MERGE.

## Business key och hashing (implementerat)

Hash-strategin styrs nu globalt i `config.yml` (inte per tabell):

```yaml
# config.yml
hashing:
  algorithm: sha2_256
  separator: "||"
```

Per tabell anger ni endast vilka kolumner som ska hash:as:

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
```

Engine skapar då:
- `business_key_hash` från `business_key`
- `row_hash` från `hash_columns`

Detta görs efter enum-berikning och före MERGE/SCD2-SQL.

Om en tabell försöker sätta `hash_algorithm`, `hash_separator` eller `hash_null_replacement` så kastas ett fel (fail fast).

`null_replacement` stöds inte globalt utan konfigureras per kolumn i respektive tabell-YAML under `columns`.

## Exempel enligt metadata-mönstret

```yaml
# config.yml
hashing:
  algorithm: sha2_256
  separator: "||"
```

```yaml
# fact_salestable.yml
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

columns:
  - custaccount:
      null_handling: error
  - salesstatus:
      null_handling: replace
      null_replacement: "Unknown"

enum:
  - column: salesstatus
    enum: salesstatus
    mapping:
      table: bronze_enum_metadata
      key_column: option
      label_column: localizedlabel

soft_delete:
  enabled: true
  column: isdeleted
```

Flera enum-mappningar stöds via en lista i `enum`.


## Exempelmetadata i repo
Det finns färdiga YAML-exempel i `metadata/sales/` och global config i `metadata/config.yml`.

Använd `schema_dir="./metadata"` i `Engine.from_yaml_dir(...)`.

## Notebook-användning

```python
from pejo import Engine, PEJOAdapter

adapter = PEJOAdapter()
engine = Engine.from_yaml_dir(spark=spark, adapter=adapter, schema_dir="./metadata")

# Kör en tabell
engine.run("custtable")

# Kör en hel domän
engine.run_domain("sales")

# Kör en lista av tabeller
engine.run_table_list(["custtable", "salestable"])

# Validera metadata/SQL-plan utan att skriva till target
engine.validate_only(table_name="custtable")
engine.validate_only(domain="sales")
engine.validate_only(table_names=["custtable", "salestable"])

# Dry-run för en tabell (returnerar SQL som skulle köras)
dry = engine.dry_run("custtable")
for sql in dry.sql_statements:
    print(sql)

```

## Körningsloggar
Engine skriver körningsloggar till tabellen `pejo_run_log`.
Om tabellen saknas skapas den automatiskt (`CREATE TABLE IF NOT EXISTS ... USING DELTA`) innan körningen startar.

## Egna fält i YAML
Du kan lägga till extra metadata-fält per tabell.
Loadern behåller fälten i metadata, så de kan användas senare för t.ex. SCD-strategier eller framtida features.
