class Engine:

    def __init__(self, spark, metadata, adapter):
        self.spark = spark
        self.metadata = metadata
        self.adapter = adapter

    def run(self, table_name):
        config = self.metadata[table_name]

        bronze = config["bronze"]
        silver = config["silver"]

        df = self.spark.table(bronze)
        df = self.adapter.transform(df)

        df.createOrReplaceTempView("source_view")

        merge_sql = build_merge_sql(table_name, config)

        self.spark.sql(merge_sql)
