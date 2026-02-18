from __future__ import annotations

from typing import Any

from pejo.features.enums import apply_enum_mappings

from .fo import PEJOAdapter


class DataverseAdapter(PEJOAdapter):
    """Dataverse adapter: base normalization + optional enum enrichment."""

    def apply_features(self, spark, df, config: dict[str, Any]):
        enum_mappings = config.get("enums") or []
        if enum_mappings:
            return apply_enum_mappings(spark, df, enum_mappings)
        return df
