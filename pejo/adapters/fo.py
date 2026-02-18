from __future__ import annotations

from .base import BaseAdapter


class PEJOAdapter(BaseAdapter):

    def transform(self, df):
        # Standardisera kolumnnamn
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower())

        return df

    def default_primary_key(self):
        return ["recid", "dataareaid"]
