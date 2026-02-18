from __future__ import annotations

from pathlib import Path

from pejo.adapters.base import BaseAdapter
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
