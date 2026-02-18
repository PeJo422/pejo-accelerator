# Finance metadata

Exempelfiler för PEJO acceleratorn:

- `platform.yaml`: global hashing-standard för alla tabeller i mappen.
- `custtable.yml`: dimensionslik tabell med `SCD2` + hashing.
- `fact_salestable.yml`: faktatabell med `SCD1`, enum-mappning och soft delete.

Kör exempelvis i notebook:

```python
from pejo import Engine, PEJOAdapter

engine = Engine.from_yaml_dir(spark=spark, adapter=PEJOAdapter(), schema_dir="./metadata/finance")
engine.run("CustTable")
engine.run("FactSalesTable")
```
