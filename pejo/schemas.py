from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from pejo.core.hashing import normalize_global_hash_config, normalize_hashing_config
from pejo.features.enums import normalize_enum_mappings

try:
    import yaml as _pyyaml
except ModuleNotFoundError:
    _pyyaml = None

REQUIRED_FIELDS = {"table", "domain"}
_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


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


def _normalize_layer_lakehouse(platform_config: dict[str, Any], layer: str) -> str | None:
    value = platform_config.get(f"{layer}_lakehouse")
    if value:
        return str(value)

    lakehouse_cfg = platform_config.get("lakehouse")
    if isinstance(lakehouse_cfg, dict):
        nested = lakehouse_cfg.get(f"{layer}_lakehouse")
        if nested:
            return str(nested)
    return None


def _normalize_layer_schema(platform_config: dict[str, Any], layer: str) -> str | None:
    value = platform_config.get(f"{layer}_schema", platform_config.get("schema"))
    if value:
        return str(value)
    return None


def _build_fqn(
    table: str,
    bronze_lakehouse: str | None,
    default_schema: str | None,
) -> str:
    """
    Build fully qualified name: lakehouse.schema.table.
    If table already contains '.', assume it is already qualified.
    """
    if not table or "." in table:
        return table

    parts: list[str] = []
    if bronze_lakehouse:
        parts.append(bronze_lakehouse)
    if default_schema:
        parts.append(default_schema)

    parts.append(table)
    return ".".join(parts)


def _resolve_placeholders(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            key = match.group(1)
            replacement = context.get(key)
            return str(replacement) if replacement is not None else match.group(0)

        return _PLACEHOLDER_PATTERN.sub(_replace, value)

    if isinstance(value, list):
        return [_resolve_placeholders(item, context) for item in value]

    if isinstance(value, dict):
        return {k: _resolve_placeholders(v, context) for k, v in value.items()}

    return value


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


def _extract_enum_mappings_from_columns(schema: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Supports column entries like:
      - salesstatus:
        enum: salesstatus
        mapping: {...}
    and keeps only null/alias rules under columns for hashing/null handling.
    """
    raw_columns = schema.get("columns") or []
    if not raw_columns:
        return [], []
    if not isinstance(raw_columns, list):
        raise ValueError("`columns` must be a list")

    enum_raw: list[dict[str, Any]] = []
    normalized_columns: list[dict[str, Any]] = []
    reserved = {
        "column",
        "enum",
        "optionset",
        "mapping",
        "metadata_table",
        "option_name_column",
        "option_value_column",
        "option_label_column",
        "output_column",
        "null_handling",
        "null_replacement",
        "alias",
    }

    for idx, entry in enumerate(raw_columns):
        if not isinstance(entry, dict):
            raise ValueError(f"`columns[{idx}]` must be a mapping")

        if "column" in entry:
            column_name = str(entry["column"])
            cfg = {k: v for k, v in entry.items() if k != "column"}
        elif len(entry) == 1:
            column_name, cfg = next(iter(entry.items()))
            column_name = str(column_name)
            if cfg is None:
                cfg = {}
            if not isinstance(cfg, dict):
                raise ValueError(f"`columns[{idx}].{column_name}` must be a mapping")
        else:
            candidates = [k for k in entry.keys() if k not in reserved]
            if len(candidates) != 1:
                raise ValueError(
                    f"`columns[{idx}]` must define exactly one column key when using inline enum format"
                )
            column_name = str(candidates[0])
            cfg = {}
            nested_cfg = entry.get(column_name)
            if nested_cfg is not None:
                if not isinstance(nested_cfg, dict):
                    raise ValueError(f"`columns[{idx}].{column_name}` must be a mapping")
                cfg.update(nested_cfg)
            cfg.update({k: v for k, v in entry.items() if k != column_name})

        if any(key in cfg for key in ("enum", "optionset", "mapping", "metadata_table")):
            enum_entry: dict[str, Any] = {"column": column_name}
            for key in (
                "enum",
                "optionset",
                "mapping",
                "metadata_table",
                "option_name_column",
                "option_value_column",
                "option_label_column",
                "output_column",
            ):
                if key in cfg:
                    enum_entry[key] = cfg[key]
            enum_raw.append(enum_entry)

        column_rules: dict[str, Any] = {}
        for key in ("null_handling", "null_replacement", "alias"):
            if key in cfg:
                column_rules[key] = cfg[key]
        if column_rules:
            normalized_columns.append({column_name: column_rules})

    return enum_raw, normalized_columns


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
    if "bronze_tablename" not in schema and "bronze" not in schema:
        missing = missing.union({"bronze_tablename"})
    if "silver_tablename" not in schema and "silver" not in schema:
        missing = missing.union({"silver_tablename"})
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
    bronze_lakehouse = _normalize_layer_lakehouse(platform_config, "bronze")
    bronze_schema = _normalize_layer_schema(platform_config, "bronze")
    silver_lakehouse = _normalize_layer_lakehouse(platform_config, "silver")
    silver_schema = _normalize_layer_schema(platform_config, "silver")

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
            normalized = _resolve_placeholders(normalized, normalized)

            bronze_table_name = normalized.get("bronze_tablename", normalized.get("bronze"))
            silver_table_name = normalized.get("silver_tablename", normalized.get("silver"))
            if not bronze_table_name or not silver_table_name:
                raise ValueError(
                    f"`bronze_tablename`/`silver_tablename` (or legacy bronze/silver) must be set in {schema_path}"
                )

            normalized["bronze_tablename"] = str(bronze_table_name)
            normalized["silver_tablename"] = str(silver_table_name)
            normalized["bronze"] = _build_fqn(str(bronze_table_name), bronze_lakehouse, bronze_schema)
            normalized["silver"] = _build_fqn(str(silver_table_name), silver_lakehouse, silver_schema)

            if bronze_lakehouse:
                normalized["bronze_lakehouse"] = bronze_lakehouse
            if silver_lakehouse:
                normalized["silver_lakehouse"] = silver_lakehouse

            normalized["primary_key"] = _normalize_primary_key(normalized)
            normalized["scdtype"] = _normalize_scd_type(normalized)

            column_enum_raw, filtered_columns = _extract_enum_mappings_from_columns(normalized)
            if filtered_columns or "columns" in normalized:
                normalized["columns"] = filtered_columns

            enum_from_config = _normalize_enums(normalized)
            enum_from_columns = (
                normalize_enum_mappings({"enum": column_enum_raw})
                if column_enum_raw
                else []
            )

            enum_mappings = enum_from_config + enum_from_columns + _normalize_enum_columns(normalized)
            for mapping in enum_mappings:
                table_name = mapping.get("metadata_table")
                if table_name:
                    mapping["metadata_table"] = _build_fqn(
                        table_name,
                        bronze_lakehouse,
                        bronze_schema,
                    )
            normalized["enums"] = enum_mappings

            normalized.update(normalize_hashing_config(normalized, global_hashing))

            table_name = str(normalized["table"])
            metadata[table_name] = normalized

    if not metadata:
        raise ValueError(f"No schema definitions found in {directory}")

    return metadata
