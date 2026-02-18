from __future__ import annotations

from abc import ABC, abstractmethod


class BaseAdapter(ABC):
    """Base contract for source-system specific transforms."""

    @abstractmethod
    def transform(self, df):
        """Apply adapter-specific transformations to the DataFrame."""
        raise NotImplementedError

    def default_primary_key(self) -> list[str]:
        """Fallback primary key used when schema omits `primary_key`."""
        return []
