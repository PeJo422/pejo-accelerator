from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAdapter(ABC):
    """Base contract for source-system specific transforms."""

    @abstractmethod
    def transform(self, df):
        """Apply adapter-specific transformations to the DataFrame."""
        raise NotImplementedError

    def apply_features(self, spark, df, config: dict[str, Any]):
        """Apply source-specific feature enrichments (default no-op)."""
        return df

    def default_primary_key(self) -> list[str]:
        """Legacy hook retained for compatibility; engine requires schema primary_key."""
        return []
