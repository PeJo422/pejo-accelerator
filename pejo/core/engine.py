def run(self, table_name):

    logger = RunLogger(self.spark)
    run_id = logger.start(table_name)

    try:
        config = self.metadata[table_name]

        df = self.spark.table(config["bronze"])
        df = self.adapter.transform(df)

        df.createOrReplaceTempView("source_view")

        columns = df.columns

        merge_sql = build_merge_sql(
            target=config["silver"],
            source_view="source_view",
            keys=config["primary_key"],
            columns=columns,
            soft_delete=config.get("soft_delete")
        )

        self.spark.sql(merge_sql)

        logger.end(status="SUCCESS", rows_source=df.count())

    except Exception as e:
        logger.end(status="FAILED", error_message=str(e))
        raise