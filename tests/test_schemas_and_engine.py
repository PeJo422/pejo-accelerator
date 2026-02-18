from __future__ import annotations

from pathlib import Path

from pejo.adapters.base import BaseAdapter
import pejo.adapters.fo as adapter_module
from pejo.adapters.fo import PEJOAdapter
from pejo.core.engine import Engine
from pejo.schemas import load_metadata_from_yaml


class DummyDataFrame:
    def __init__(self):
        self.columns = ["recid", "dataareaid", "name"]

    def withColumnRenamed(self, old: str, new: str):
        self.columns = [new if c == old else c for c in self.columns]
        return self

    def createOrReplaceTempView(self, _name: str):
        return None

    def count(self):
        return 3


class DummySpark:
    def __init__(self):
        self.sql_calls = []
        self.saved_rows = []

    def table(self, _name: str):
        return DummyDataFrame()

    def sql(self, query: str):
        self.sql_calls.append(query)

    def createDataFrame(self, rows, _schema):
        self.saved_rows.extend(rows)

        class _Writer:
            def mode(self, _m):
                return self

            def saveAsTable(self, _table):
                return None

        class _DF:
            @property
            def write(self):
                return _Writer()

        return _DF()


class DummyAdapter(BaseAdapter):
    def transform(self, df):
        return df

    def default_primary_key(self):
        return ["recid", "dataareaid"]


def test_load_metadata_from_yaml(tmp_path: Path):
    schema_file = tmp_path / "sales.yml"
    schema_file.write_text(
        """
tables:
  - table: CustTable
    domain: Sales
    bronze: bronze.sales.custtable
    silver: silver.sales.custtable
    primary_key: [recid, dataareaid]
""".strip(),
        encoding="utf-8",
    )

    metadata = load_metadata_from_yaml(tmp_path)
    assert "CustTable" in metadata
    assert metadata["CustTable"]["domain"] == "Sales"


def test_engine_run_domain(tmp_path: Path):
    (tmp_path / "sales.yml").write_text(
        """
tables:
  - table: CustTable
    domain: Sales
    bronze: bronze.sales.custtable
    silver: silver.sales.custtable
  - table: SalesTable
    domain: Sales
    bronze: bronze.sales.salestable
    silver: silver.sales.salestable
""".strip(),
        encoding="utf-8",
    )

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    result = engine.run_domain("Sales")
    assert set(result.tables) == {"CustTable", "SalesTable"}
    merge_calls = [q for q in engine.spark.sql_calls if "MERGE INTO" in q]
    assert len(merge_calls) == 2


def test_engine_run_table_list(tmp_path: Path):
    (tmp_path / "sales.yml").write_text(
        """
tables:
  - table: CustTable
    domain: Sales
    bronze: bronze.sales.custtable
    silver: silver.sales.custtable
  - table: SalesTable
    domain: Sales
    bronze: bronze.sales.salestable
    silver: silver.sales.salestable
""".strip(),
        encoding="utf-8",
    )

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    result = engine.run_table_list(["SalesTable", "CustTable"])
    assert result.tables == ["SalesTable", "CustTable"]
    merge_calls = [q for q in engine.spark.sql_calls if "MERGE INTO" in q]
    assert len(merge_calls) == 2


def test_yaml_allows_custom_fields_and_scdtype(tmp_path: Path):
    (tmp_path / "custom.yml").write_text(
        """
table: DimCustomer
domain: Sales
bronze: bronze.sales.dimcustomer
silver: silver.sales.dimcustomer
primary_key: recid
scdtype: SCD2
watermark_column: modifieddatetime
""".strip(),
        encoding="utf-8",
    )

    metadata = load_metadata_from_yaml(tmp_path)
    cfg = metadata["DimCustomer"]
    assert cfg["scdtype"] == "SCD2"
    assert cfg["watermark_column"] == "modifieddatetime"



def test_loads_enum_mapping_from_yaml(tmp_path: Path):
    (tmp_path / "enum.yml").write_text(
        """
table: SalesOrder
domain: Sales
bronze: bronze.sales.salesorder
silver: silver.sales.salesorder
primary_key: recid
enum:
  - column: salesstatus
    enum: SalesStatus
    mapping:
      table: bronze.crm.globaloptionsetmetadata
      enum_column: optionsetname
      key_column: optionvalue
      label_column: label
      output_column: salesstatus_label
""".strip(),
        encoding="utf-8",
    )

    metadata = load_metadata_from_yaml(tmp_path)
    enums = metadata["SalesOrder"]["enums"]
    assert len(enums) == 1
    assert enums[0]["column"] == "salesstatus"
    assert enums[0]["optionset"] == "SalesStatus"
    assert enums[0]["output_column"] == "salesstatus_label"


def test_engine_applies_enum_mappings(tmp_path: Path, monkeypatch):
    (tmp_path / "enum.yml").write_text(
        """
table: SalesOrder
domain: Sales
bronze: bronze.sales.salesorder
silver: silver.sales.salesorder
primary_key: recid
enum:
  - column: salesstatus
    enum: SalesStatus
""".strip(),
        encoding="utf-8",
    )

    called = {"value": False}

    def _fake_apply(spark, df, enum_mappings):
        called["value"] = True
        assert len(enum_mappings) == 1
        return df

    monkeypatch.setattr(adapter_module, "apply_enum_mappings", _fake_apply)

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=PEJOAdapter(),
        schema_dir=tmp_path,
    )

    engine.run("SalesOrder")
    assert called["value"] is True



def test_engine_ensures_log_table_exists(tmp_path: Path):
    (tmp_path / "sales.yml").write_text(
        """
table: CustTable
domain: Sales
bronze: bronze.sales.custtable
silver: silver.sales.custtable
primary_key: recid
""".strip(),
        encoding="utf-8",
    )

    spark = DummySpark()
    engine = Engine.from_yaml_dir(
        spark=spark,
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    engine.run("CustTable")

    assert any("CREATE TABLE IF NOT EXISTS pejo_run_log" in q for q in spark.sql_calls)



def test_engine_runs_scd2_update_and_insert(tmp_path: Path):
    (tmp_path / "dim.yml").write_text(
        """
table: DimCustomer
domain: Sales
bronze: bronze.sales.dimcustomer
silver: silver.sales.dimcustomer
primary_key: recid
scdtype: SCD2
""".strip(),
        encoding="utf-8",
    )

    spark = DummySpark()
    engine = Engine.from_yaml_dir(
        spark=spark,
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    engine.run("DimCustomer")

    update_calls = [q for q in spark.sql_calls if "UPDATE silver.sales.dimcustomer" in q]
    insert_calls = [q for q in spark.sql_calls if "INSERT INTO silver.sales.dimcustomer" in q]

    assert len(update_calls) == 1
    assert len(insert_calls) == 1




def test_engine_applies_column_aliases_in_scd2_insert_select(tmp_path: Path):
    (tmp_path / "dim.yml").write_text(
        """
table: DimCustomer
domain: Sales
bronze: bronze.sales.dimcustomer
silver: silver.sales.dimcustomer
primary_key: recid
scdtype: SCD2
columns:
  - column: name
    alias: CustomerName
""".strip(),
        encoding="utf-8",
    )

    spark = DummySpark()
    engine = Engine.from_yaml_dir(
        spark=spark,
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    engine.run("DimCustomer")

    insert_call = next(q for q in spark.sql_calls if "INSERT INTO silver.sales.dimcustomer" in q)
    assert "INSERT INTO silver.sales.dimcustomer (recid, dataareaid, CustomerName, valid_from, valid_to, is_current)" in insert_call
    assert "SELECT s.recid, s.dataareaid, s.name AS CustomerName, current_timestamp()" in insert_call
def test_engine_dry_run_returns_sql_without_execution(tmp_path: Path):
    (tmp_path / "sales.yml").write_text(
        """
table: CustTable
domain: Sales
bronze: bronze.sales.custtable
silver: silver.sales.custtable
primary_key: recid
""".strip(),
        encoding="utf-8",
    )

    spark = DummySpark()
    engine = Engine.from_yaml_dir(
        spark=spark,
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    dry_run = engine.dry_run("CustTable")
    assert dry_run.table == "CustTable"
    assert len(dry_run.sql_statements) == 1
    assert "MERGE INTO" in dry_run.sql_statements[0]
    assert not any("MERGE INTO" in q for q in spark.sql_calls)


def test_engine_validate_only_domain_without_execution(tmp_path: Path):
    (tmp_path / "sales.yml").write_text(
        """
tables:
  - table: CustTable
    domain: Sales
    bronze: bronze.sales.custtable
    silver: silver.sales.custtable
  - table: SalesTable
    domain: Sales
    bronze: bronze.sales.salestable
    silver: silver.sales.salestable
""".strip(),
        encoding="utf-8",
    )

    spark = DummySpark()
    engine = Engine.from_yaml_dir(
        spark=spark,
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    result = engine.validate_only(domain="Sales")
    assert set(result.tables) == {"CustTable", "SalesTable"}
    assert not any("MERGE INTO" in q for q in spark.sql_calls)


def test_engine_validate_only_requires_single_selector(tmp_path: Path):
    (tmp_path / "sales.yml").write_text(
        """
table: CustTable
domain: Sales
bronze: bronze.sales.custtable
silver: silver.sales.custtable
""".strip(),
        encoding="utf-8",
    )

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    try:
        engine.validate_only()
        assert False, "Expected ValueError when no selector is supplied"
    except ValueError as exc:
        assert "Specify exactly one" in str(exc)



def test_loads_hashing_and_enum_columns_from_yaml(tmp_path: Path):
    (tmp_path / "config.yml").write_text(
        """
hashing:
  algorithm: sha2_512
  separator: "||"
""".strip(),
        encoding="utf-8",
    )

    (tmp_path / "hash.yml").write_text(
        """
table: SalesTable
domain: Finance
bronze: bronze_salestable
silver: silver_salestable
primary_key: [recid, dataareaid]
business_key: [salesid, dataareaid]
hash_columns: [salesid, custaccount, salesstatus, invoiceaccount, modifieddatetime]
enum:
  - column: salesstatus
    enum: salesstatus
    mapping:
      table: bronze_enum_metadata
      key_column: option
      label_column: localizedlabel
""".strip(),
        encoding="utf-8",
    )

    metadata = load_metadata_from_yaml(tmp_path)
    cfg = metadata["SalesTable"]

    assert cfg["business_key"] == ["salesid", "dataareaid"]
    assert cfg["hash_columns"][0] == "salesid"
    assert cfg["hashing"].algorithm == "sha2_512"

    assert cfg["columns"] == {}

    assert len(cfg["enums"]) == 1
    assert cfg["enums"][0]["column"] == "salesstatus"
    assert cfg["enums"][0]["option_value_column"] == "option"
    assert cfg["enums"][0]["option_label_column"] == "localizedlabel"



def test_engine_applies_hashing_strategy(tmp_path: Path, monkeypatch):
    (tmp_path / "config.yml").write_text(
        """
hashing:
  algorithm: sha2_256
  separator: "||"
""".strip(),
        encoding="utf-8",
    )

    (tmp_path / "hash.yml").write_text(
        """
table: SalesTable
domain: Finance
bronze: bronze_salestable
silver: silver_salestable
primary_key: [recid, dataareaid]
business_key: [salesid, dataareaid]
hash_columns: [salesid, custaccount]
""".strip(),
        encoding="utf-8",
    )

    calls = {"value": 0}

    def _fake_hash(df, config):
        calls["value"] += 1
        assert config["business_key"] == ["salesid", "dataareaid"]
        assert config["hash_columns"] == ["salesid", "custaccount"]
        return df

    import pejo.core.engine as engine_module

    monkeypatch.setattr(engine_module, "apply_hashing_strategy", _fake_hash)

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    engine.run("SalesTable")
    assert calls["value"] == 1


def test_load_metadata_recursively_from_metadata_root(tmp_path: Path):
    (tmp_path / "config.yml").write_text(
        """
hashing:
  algorithm: sha2_512
""".strip(),
        encoding="utf-8",
    )

    (tmp_path / "sales").mkdir(parents=True, exist_ok=True)
    (tmp_path / "sales" / "salestable.yml").write_text(
        """
table: salestable
domain: sales
bronze: bronze_salestable
silver: silver_salestable
hash_columns: [salesid]
""".strip(),
        encoding="utf-8",
    )

    metadata = load_metadata_from_yaml(tmp_path)
    assert "salestable" in metadata
    assert metadata["salestable"]["hashing"].algorithm == "sha2_512"



def test_table_level_hashing_overrides_are_rejected(tmp_path: Path):
    (tmp_path / "platform.yaml").write_text(
        """
hashing:
  algorithm: sha2_256
  separator: "||"
""".strip(),
        encoding="utf-8",
    )

    (tmp_path / "bad.yml").write_text(
        """
table: SalesTable
domain: Finance
bronze: bronze_salestable
silver: silver_salestable
primary_key: recid
hash_algorithm: sha2_512
""".strip(),
        encoding="utf-8",
    )

    try:
        load_metadata_from_yaml(tmp_path)
        assert False, "Expected ValueError for table-level hashing override"
    except ValueError as exc:
        assert "Table-level hashing overrides are not allowed" in str(exc)


def test_loads_columns_null_handling_config(tmp_path: Path):
    (tmp_path / "config.yml").write_text(
        """
hashing:
  algorithm: sha2_256
""".strip(),
        encoding="utf-8",
    )

    (tmp_path / "cust.yml").write_text(
        """
table: custtable
domain: masterdata
bronze: bronze_finance_custtable
silver: silver_masterdata_dim_custtable
columns:
  - accountnum:
      null_handling: error
  - currency:
      null_handling: replace
      null_replacement: Unknown
""".strip(),
        encoding="utf-8",
    )

    cfg = load_metadata_from_yaml(tmp_path)["custtable"]
    assert cfg["columns"]["accountnum"]["null_handling"] == "error"
    assert cfg["columns"]["currency"]["null_handling"] == "replace"
    assert cfg["columns"]["currency"]["null_replacement"] == "Unknown"


def test_global_null_replacement_is_rejected(tmp_path: Path):
    (tmp_path / "config.yml").write_text(
        """
hashing:
  algorithm: sha2_256
  null_replacement: ""
""".strip(),
        encoding="utf-8",
    )

    (tmp_path / "table.yml").write_text(
        """
table: CustTable
domain: Sales
bronze: bronze.sales.custtable
silver: silver.sales.custtable
""".strip(),
        encoding="utf-8",
    )

    try:
        load_metadata_from_yaml(tmp_path)
        assert False, "Expected ValueError for global null_replacement"
    except ValueError as exc:
        assert "Global hashing.null_replacement is not supported" in str(exc)
