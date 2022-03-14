import time
from datetime import datetime
from pyspark.sql import SparkSession
from constants import ConfigConstants as CC
from factory import IngestionFactory


class AuditDecorator(object):
    def __init__(self):
        self._start_time = time.time()
        self._end_time = None
        self._count = None
        self._audit_destination = None
        self._audit_status = CC.AUDIT_SUCCESS_STATUS
        self._audit_message = None

    def call(self, ingestion_job):
        def wrapper(spark, context):
            self._audit_destination = IngestionFactory.get_audit_destination(context)
            try:
                self._count = ingestion_job(spark, context)
            except Exception as e:
                self._audit_message = str(e)
                self._audit_status = CC.AUDIT_FAILURE_STATUS
                raise e
            finally:
                self._end_time = time.time()
                self.write_audit(spark, context)

        return wrapper

    def write_audit(self, spark: SparkSession, context):
        audit_id = context.get_args[CC.AUDIT_ID]
        process_name = context.get_args[CC.PROCESS_NAME]
        sub_process_name = context.get_args[CC.SUB_PROCESS_NAME]
        object_name = context.get_args[CC.OBJECT_NAME]

        log_data = [{CC.AUDIT_ID: audit_id,
                     CC.AUDIT_COUNT_COLUMN: self._count,
                     CC.AUDIT_START_DT_COLUMN: datetime.fromtimestamp(self._start_time),
                     CC.AUDIT_END_DT_COLUMN: datetime.fromtimestamp(self._end_time),
                     CC.PROCESS_NAME: process_name,
                     CC.SUB_PROCESS_NAME: sub_process_name,
                     CC.OBJECT_NAME: object_name
                     }]

        log_df = spark.createDataFrame(log_data).coalesce(1)

        self._audit_destination.write(log_df, context)
