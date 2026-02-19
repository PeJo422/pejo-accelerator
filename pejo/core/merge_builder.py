from __future__ import annotations


def _target_column(column: str, column_aliases: dict[str, str] | None) -> str:
    if not column_aliases:
        return column
    return column_aliases.get(column, column)


def build_delta_merge_sql(target, source_view, keys, columns, soft_delete=None, column_aliases=None):
    if not keys:
        raise ValueError("Delta merge requires at least one primary key column")

    join_condition = " AND ".join([f"t.{_target_column(k, column_aliases)} = s.{k}" for k in keys])

    update_clause = ",\n        ".join([f"t.{_target_column(c, column_aliases)} = s.{c}" for c in columns])

    insert_columns = ", ".join([_target_column(c, column_aliases) for c in columns])
    insert_values = ", ".join([f"s.{c}" for c in columns])

    merge_sql = f"""
    MERGE INTO {target} t
    USING {source_view} s
    ON {join_condition}
    """

    if soft_delete and soft_delete["enabled"]:
        delete_col = soft_delete["column"]
        merge_sql += f"""
        WHEN MATCHED AND s.{delete_col} = true THEN DELETE
        """

    merge_sql += f"""
        WHEN MATCHED THEN UPDATE SET
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
    """

    return merge_sql


def build_scd2_sql(target, source_view, keys, columns, column_aliases=None):
    if not keys:
        raise ValueError("SCD2 requires at least one primary key column")

    scd2_technical_columns = {"valid_from", "valid_to", "is_current"}
    payload_columns = [c for c in columns if c not in scd2_technical_columns]
    if "row_hash" not in columns:
        raise ValueError("SCD2 requires 'row_hash' column for hash-based change detection")

    key_join_si = " AND ".join([f"t2.{_target_column(k, column_aliases)} = s.{k}" for k in keys])
    merge_on = " AND ".join([f"t.{_target_column(k, column_aliases)} = u.__merge_key_{k}" for k in keys])

    

    target_row_hash = _target_column("row_hash", column_aliases)
    change_condition_tu = f"NOT (t.{target_row_hash} <=> u.row_hash)"
    change_condition_t2s = f"NOT (t2.{target_row_hash} <=> s.row_hash)"

    base_columns = ", ".join([_target_column(c, column_aliases) for c in payload_columns])
    base_values = ", ".join([f"u.{c}" for c in payload_columns])

    merge_key_columns_update = ", ".join([f"s.{k} AS __merge_key_{k}" for k in keys])
    merge_key_columns_insert = ", ".join([f"NULL AS __merge_key_{k}" for k in keys])

    merge_sql = f"""
    MERGE INTO {target} t
    USING (
      SELECT {", ".join([f"s.{c}" for c in payload_columns])}, {merge_key_columns_update}, 'U' AS __op
      FROM {source_view} s
      UNION ALL
      SELECT {", ".join([f"s.{c}" for c in payload_columns])}, {merge_key_columns_insert}, 'I' AS __op
      FROM {source_view} s
      LEFT JOIN {target} t2
        ON {key_join_si}
       AND t2.is_current = true
      WHERE t2.{_target_column(keys[0], column_aliases)} IS NULL
         OR ({change_condition_t2s})
    ) u
    ON {merge_on}
    AND t.is_current = true
    WHEN MATCHED AND u.__op = 'U' AND ({change_condition_tu}) THEN UPDATE SET
      t.is_current = false,
      t.valid_to = current_timestamp()
    WHEN NOT MATCHED AND u.__op = 'I' THEN INSERT ({base_columns}, valid_from, valid_to, is_current)
    VALUES ({base_values}, current_timestamp(), CAST(NULL AS TIMESTAMP), true)
    """

    return merge_sql


def build_merge_sql(target, source_view, keys, columns, soft_delete=None, column_aliases=None):
    """Backward compatible alias. Kept to avoid breaking callers."""

    return build_delta_merge_sql(
        target=target,
        source_view=source_view,
        keys=keys,
        columns=columns,
        soft_delete=soft_delete,
        column_aliases=column_aliases,
    )
