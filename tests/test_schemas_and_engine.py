from __future__ import annotations

from pathlib import Path

from pejo.adapters.base import BaseAdapter
import pejo.adapters.dataverse as dataverse_module
from pejo.adapters.dataverse import DataverseAdapter
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
enums:
  - column: salesstatus
    optionset: SalesStatus
    metadata_table: bronze.crm.globaloptionsetmetadata
    option_name_column: optionsetname
    option_value_column: optionvalue
    option_label_column: label
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
enums:
  - column: salesstatus
    optionset: SalesStatus
""".strip(),
        encoding="utf-8",
    )

    called = {"value": False}

    def _fake_apply(spark, df, enum_mappings):
        called["value"] = True
        assert len(enum_mappings) == 1
        return df

    monkeypatch.setattr(dataverse_module, "apply_enum_mappings", _fake_apply)

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=DataverseAdapter(),
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



def test_loads_hashing_and_enum_columns_from_yaml(tmp_path: Path):
    (tmp_path / "hash.yml").write_text(
        """
table: SalesTable
domain: Finance
bronze: bronze_salestable
silver: silver_salestable
primary_key: [recid, dataareaid]
business_key: [salesid, dataareaid]
hash_columns: [salesid, custaccount, salesstatus, invoiceaccount, modifieddatetime]
hash_algorithm: sha2_512
enum_columns:
  salesstatus:
    metadata_table: bronze_enum_metadata
    optionset: salesstatus
    key_column: option
    label_column: localizedlabel
""".strip(),
        encoding="utf-8",
    )

    metadata = load_metadata_from_yaml(tmp_path)
    cfg = metadata["SalesTable"]

    assert cfg["business_key"] == ["salesid", "dataareaid"]
    assert cfg["hash_columns"][0] == "salesid"
    assert cfg["hash_algorithm"] == "sha2_512"

    assert len(cfg["enums"]) == 1
    assert cfg["enums"][0]["column"] == "salesstatus"
    assert cfg["enums"][0]["option_value_column"] == "option"
    assert cfg["enums"][0]["option_label_column"] == "localizedlabel"



def test_engine_applies_hashing_strategy(tmp_path: Path, monkeypatch):
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

