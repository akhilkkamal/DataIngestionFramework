import time
from datetime import datetime
from constants import config_constants as CC
from factory import ingestion_factory as IngestionFactory


class JobRunner:
    def __init__(self, context):
        self._context = context
        self._spark = context.get_session
        self._start_time = time.time()
        self._end_time = None
        self._count = -1
        self._audit_destination = None
        self._audit_status = CC.AUDIT_SUCCESS_STATUS
        self._audit_message = "Succeeded"
        self._audit_id = context.get_args[CC.AUDIT_ID]
        self._process_name = context.get_args[CC.PROCESS_NAME]
        self._sub_process_name = context.get_args[CC.SUB_PROCESS_NAME]
        self._object_name = context.get_args[CC.OBJECT_NAME]
        self._audit_destination = IngestionFactory.get_audit_destination(self._context)

    def call(self, ingestion_job):
        def wrapper():
            self._audit_destination = IngestionFactory.get_audit_destination(self._context)
            try:
                self._count = ingestion_job(self._context)
            except Exception as e:
                self._audit_message = str(e)
                self._audit_status = CC.AUDIT_FAILURE_STATUS
                raise e
            finally:
                self._end_time = time.time()
                self.write_audit()

        return wrapper

    def write_audit(self):

        log_data = [{CC.AUDIT_ID: self._audit_id,
                     CC.AUDIT_COUNT_COLUMN: self._count,
                     CC.AUDIT_START_DT_COLUMN: datetime.fromtimestamp(self._start_time),
                     CC.AUDIT_END_DT_COLUMN: datetime.fromtimestamp(self._end_time),
                     CC.PROCESS_NAME: self._process_name,
                     CC.SUB_PROCESS_NAME: self._sub_process_name,
                     CC.OBJECT_NAME: self._object_name,
                     CC.STATUS: self._audit_status,
                     CC.AUDIT_MESSAGE: self._audit_message
                     }]

        log_df = self._spark.createDataFrame(log_data).coalesce(1)

        self._audit_destination.write(log_df)
