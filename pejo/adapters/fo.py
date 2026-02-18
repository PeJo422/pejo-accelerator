from __future__ import annotations

from typing import Any

from pejo.features.enums import apply_enum_mappings

from .base import BaseAdapter


class PEJOAdapter(BaseAdapter):

    def transform(self, df):
        # Standardisera kolumnnamn
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower())

        return df

    def default_primary_key(self):
        return ["recid", "dataareaid"]

    def apply_features(self, spark, df, config: dict[str, Any]):
        enum_mappings = config.get("enums") or []
        if enum_mappings:
            return apply_enum_mappings(spark, df, enum_mappings)
        return df
