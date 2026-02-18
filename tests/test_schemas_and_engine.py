from __future__ import annotations

from pathlib import Path

from pejo.adapters.base import BaseAdapter
import pejo.core.engine as engine_module
from pejo.core.engine import Engine
from pejo.schemas import load_metadata_from_yaml


class DummyDataFrame:
    def __init__(self):
        self.columns = ["recid", "dataareaid", "name"]

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
    assert len(engine.spark.sql_calls) == 2
    assert all("MERGE INTO" in q for q in engine.spark.sql_calls)


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

    monkeypatch.setattr(engine_module, "apply_enum_mappings", _fake_apply)

    engine = Engine.from_yaml_dir(
        spark=DummySpark(),
        adapter=DummyAdapter(),
        schema_dir=tmp_path,
    )

    engine.run("SalesOrder")
    assert called["value"] is True

