import uuid
from datetime import datetime

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

class RunLogger:

    def __init__(self, spark, log_table: str = "pejo_run_log"):
        self.spark = spark
        self.log_table = log_table

    def _ensure_log_table(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.log_table} (
                run_id STRING,
                table_name STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_source BIGINT,
                status STRING,
                error_message STRING,
                executed_sql STRING
            )
            USING DELTA
            """.strip()
        )

    def start(self, table_name):
        self._ensure_log_table()
        self.run_id = str(uuid.uuid4())
        self.table_name = table_name
        self.start_time = datetime.utcnow()
        return self.run_id

    def end(
            self, 
            status="SUCCESS", 
            error_message=None, 
            rows_source=None,
            executed_sql=None,
            ):
        end_time = datetime.utcnow()

        schema = StructType(
            [
                StructField("run_id", StringType(), False),
                StructField("table_name", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("end_time", TimestampType(), False),
                StructField("rows_source", LongType(), True),
                StructField("status", StringType(), False),
                StructField("error_message", StringType(), True),
                StructField("executed_sql", StringType(), True),
            ]
        )

        log_df = self.spark.createDataFrame(
            [
                (
                    self.run_id,
                    self.table_name,
                    self.start_time,
                    end_time,
                    rows_source,
                    status,
                    error_message,
                    executed_sql
                )
            ],
            schema=schema,
        )

        log_df.write.mode("append").saveAsTable(self.log_table)
