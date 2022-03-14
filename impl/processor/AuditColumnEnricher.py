from api.IProcessor import IProcessor
from pyspark.sql import SparkSession
import constants.IngestionConstants as const
from pyspark.sql.functions import current_timestamp


class AuditColumnEnricher(IProcessor):
    def process(self, context, *df):
        base_df = df[0]
        return base_df.withColumn(const.EXTRACT_DT_COLUMN_NAME, current_timestamp())

