from __future__ import annotations

from pathlib import Path
from typing import Any

from pejo.features.enums import normalize_enum_mappings
from pejo.core.hashing import normalize_global_hash_config, normalize_hashing_config

try:
    import yaml as _pyyaml
except ModuleNotFoundError:
    _pyyaml = None

REQUIRED_FIELDS = {"table", "domain", "bronze", "silver"}


class _MiniYamlParser:
    """Very small YAML subset parser for repo configuration files."""

    def __init__(self, text: str):
        self.lines = self._preprocess(text)
        self.index = 0

    @staticmethod
    def _preprocess(text: str) -> list[tuple[int, str]]:
        processed: list[tuple[int, str]] = []
        for raw_line in text.splitlines():
            line = raw_line.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            indent = len(line) - len(line.lstrip(" "))
            processed.append((indent, line.strip()))
        return processed

    def parse(self) -> Any:
        if not self.lines:
            return None
        return self._parse_block(self.lines[self.index][0])

    def _parse_block(self, indent: int) -> Any:
        if self.index >= len(self.lines):
            return None

        _, content = self.lines[self.index]
        if content.startswith("- "):
            return self._parse_list(indent)
        return self._parse_map(indent)

    def _parse_list(self, indent: int) -> list[Any]:
        items: list[Any] = []
        while self.index < len(self.lines):
            line_indent, content = self.lines[self.index]
            if line_indent < indent or not content.startswith("- "):
                break

            item_content = content[2:].strip()
            self.index += 1

            if not item_content:
                items.append(self._parse_block(indent + 2))
                continue

            if ":" in item_content and not item_content.startswith("["):
                key, value = item_content.split(":", 1)
                key = key.strip()
                value = value.strip()
                item: dict[str, Any] = {key: self._parse_scalar(value) if value else None}
                if item[key] is None and self._peek_indent() > line_indent:
                    item[key] = self._parse_block(line_indent + 2)

                while self.index < len(self.lines):
                    next_indent, next_content = self.lines[self.index]
                    if next_indent <= line_indent:
                        break
                    sub_key, sub_value = next_content.split(":", 1)
                    sub_key = sub_key.strip()
                    sub_value = sub_value.strip()
                    self.index += 1
                    if sub_value:
                        item[sub_key] = self._parse_scalar(sub_value)
                    else:
                        item[sub_key] = self._parse_block(next_indent + 2)
                items.append(item)
            else:
                items.append(self._parse_scalar(item_content))

        return items

    def _parse_map(self, indent: int) -> dict[str, Any]:
        result: dict[str, Any] = {}
        while self.index < len(self.lines):
            line_indent, content = self.lines[self.index]
            if line_indent < indent or content.startswith("- "):
                break

            key, value = content.split(":", 1)
            key = key.strip()
            value = value.strip()
            self.index += 1

            if value:
                result[key] = self._parse_scalar(value)
            else:
                if self.index < len(self.lines) and self.lines[self.index][0] > line_indent:
                    result[key] = self._parse_block(self.lines[self.index][0])
                else:
                    result[key] = None

        return result

    def _peek_indent(self) -> int:
        if self.index >= len(self.lines):
            return -1
        return self.lines[self.index][0]

    @staticmethod
    def _parse_scalar(value: str) -> Any:
        if value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            if not inner:
                return []
            return [_MiniYamlParser._parse_scalar(part.strip()) for part in inner.split(",")]

        lowered = value.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        if lowered == "null":
            return None

        if value.startswith(('"', "'")) and value.endswith(('"', "'")):
            return value[1:-1]

        return value


def _load_yaml_text(text: str) -> Any:
    if _pyyaml is not None:
        return _pyyaml.safe_load(text)
    return _MiniYamlParser(text).parse()


def _load_platform_config(schema_dir: Path) -> dict[str, Any]:
    for name in ("config.yml", "config.yaml", "platform.yaml", "platform.yml"):
        candidates = [schema_dir / name, *schema_dir.glob(f"**/{name}")]
        for path in candidates:
            if path.exists():
                raw = _load_yaml_text(path.read_text(encoding="utf-8"))
                if raw is None:
                    return {}
                if not isinstance(raw, dict):
                    raise ValueError(f"Platform config must be a mapping in {path}")
                return raw
    return {}




def _normalize_primary_key(schema: dict[str, Any]) -> list[str]:
    primary_key = schema.get("primary_key") or []

    if isinstance(primary_key, str):
        return [primary_key]
    if not isinstance(primary_key, list):
        raise ValueError("`primary_key` must be a string or a list of strings")

    return [str(item) for item in primary_key]




def _normalize_scd_type(schema: dict[str, Any]) -> str:
    scd_type = schema.get("scdtype", "SCD1")
    return str(scd_type).upper()



def _normalize_enums(schema: dict[str, Any]) -> list[dict[str, str]]:
    return normalize_enum_mappings(schema)



def _normalize_enum_columns(schema: dict[str, Any]) -> list[dict[str, str]]:
    """Backward-compatible parser for legacy `enum_columns` format."""
    enum_columns = schema.get("enum_columns") or {}
    if not enum_columns:
        return []
    if not isinstance(enum_columns, dict):
        raise ValueError("`enum_columns` must be a mapping")

    normalized: list[dict[str, str]] = []
    for column, cfg in enum_columns.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"`enum_columns.{column}` must be a mapping")

        optionset = cfg.get("enum", cfg.get("optionset"))
        if not optionset:
            raise ValueError(f"`enum_columns.{column}` requires `optionset`")

        normalized.append(
            {
                "column": str(column),
                "optionset": str(optionset),
                "metadata_table": str(cfg.get("metadata_table", "globaloptionsetmetadata")),
                "option_name_column": str(cfg.get("option_name_column", "optionsetname")),
                "option_value_column": str(cfg.get("key_column", cfg.get("option_value_column", "optionvalue"))),
                "option_label_column": str(cfg.get("label_column", cfg.get("option_label_column", "label"))),
                "output_column": str(cfg.get("output_column", f"{column}_label")),
            }
        )

    return normalized


def _validate_schema(schema: dict[str, Any], schema_path: Path) -> None:
    missing = REQUIRED_FIELDS.difference(schema.keys())
    if missing:
        missing_csv = ", ".join(sorted(missing))
        raise ValueError(f"Missing required field(s) [{missing_csv}] in {schema_path}")


def load_metadata_from_yaml(schema_dir: str | Path) -> dict[str, dict[str, Any]]:
    """Load table metadata from one or many YAML files.

    Supports either:
      - a single table per file
      - multi-table file with top-level key `tables: [...]`
    """

    directory = Path(schema_dir)
    if not directory.exists():
        raise FileNotFoundError(f"Schema directory does not exist: {directory}")

    metadata: dict[str, dict[str, Any]] = {}
    platform_config = _load_platform_config(directory)
    global_hashing = normalize_global_hash_config(platform_config)

    for schema_path in sorted(directory.glob("**/*.y*ml")):
        if schema_path.name in {"platform.yaml", "platform.yml", "config.yaml", "config.yml"}:
            continue
        raw = _load_yaml_text(schema_path.read_text(encoding="utf-8"))
        if raw is None:
            continue

        entries = raw.get("tables", []) if isinstance(raw, dict) and "tables" in raw else [raw]

        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"Invalid schema entry in {schema_path}: expected mapping")

            _validate_schema(entry, schema_path)
            normalized = dict(entry)
            normalized["primary_key"] = _normalize_primary_key(normalized)
            normalized["scdtype"] = _normalize_scd_type(normalized)
            normalized["enums"] = _normalize_enums(normalized) + _normalize_enum_columns(normalized)
            normalized.update(normalize_hashing_config(normalized, global_hashing))

            table_name = str(normalized["table"])
            metadata[table_name] = normalized

    if not metadata:
        raise ValueError(f"No schema definitions found in {directory}")

    return metadata
