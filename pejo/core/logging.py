import uuid
from datetime import datetime


class RunLogger:

    def __init__(self, spark):
        self.spark = spark

    def start(self, table_name):
        self.run_id = str(uuid.uuid4())
        self.table_name = table_name
        self.start_time = datetime.utcnow()
        return self.run_id

    def end(self, status="SUCCESS", error_message=None, rows_source=None):
        end_time = datetime.utcnow()

        log_df = self.spark.createDataFrame(
            [
                (
                    self.run_id,
                    self.table_name,
                    self.start_time,
                    end_time,
                    rows_source,
                    status,
                    error_message
                )
            ],
            [
                "run_id",
                "table_name",
                "start_time",
                "end_time",
                "rows_source",
                "status",
                "error_message"
            ]
        )

        log_df.write.mode("append").saveAsTable("pejo_run_log")