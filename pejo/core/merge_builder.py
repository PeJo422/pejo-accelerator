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

    tracked_columns = [c for c in columns if c not in keys]
    if not tracked_columns:
        raise ValueError("SCD2 requires at least one non-key column to detect changes")

    key_join_ts = " AND ".join([f"t.{_target_column(k, column_aliases)} = s.{k}" for k in keys])
    key_join_tt = " AND ".join([f"t.{_target_column(k, column_aliases)} = s.{k}" for k in keys])

    def _cmp(col: str) -> str:
        target_col = _target_column(col, column_aliases)
        return f"COALESCE(CAST(t.{target_col} AS STRING), '__NULL__') <> COALESCE(CAST(s.{col} AS STRING), '__NULL__')"

    change_condition = " OR ".join([_cmp(c) for c in tracked_columns])

    base_columns = ", ".join([_target_column(c, column_aliases) for c in columns])
    base_values = ", ".join(
        [
            f"s.{c} AS {_target_column(c, column_aliases)}"
            if _target_column(c, column_aliases) != c
            else f"s.{c}"
            for c in columns
        ]
    )

    update_sql = f"""
    UPDATE {target} t
    SET
      t.is_current = false,
      t.valid_to = current_timestamp()
    FROM {source_view} s
    WHERE {key_join_ts}
      AND t.is_current = true
      AND ({change_condition})
    """

    insert_sql = f"""
    INSERT INTO {target} ({base_columns}, valid_from, valid_to, is_current)
    SELECT {base_values}, current_timestamp(), CAST(NULL AS TIMESTAMP), true
    FROM {source_view} s
    LEFT JOIN {target} t
      ON {key_join_tt}
     AND t.is_current = true
    WHERE t.{_target_column(keys[0], column_aliases)} IS NULL
       OR ({change_condition})
    """

    return update_sql, insert_sql


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
