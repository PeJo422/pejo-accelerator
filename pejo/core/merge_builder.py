def build_merge_sql(target, source_view, keys, columns, soft_delete=None):
    join_condition = " AND ".join(
        [f"t.{k} = s.{k}" for k in keys]
    )

    update_clause = ",\n        ".join(
        [f"t.{c} = s.{c}" for c in columns]
    )

    insert_columns = ", ".join(columns)
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