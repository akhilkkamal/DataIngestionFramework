from api.IProcessor import IProcessor
import constants.IngestionConstants as const
from pyspark.sql.functions import current_timestamp


class ExtractDateEnrichProcessor(IProcessor):
    def __init__(self, context):
        pass

    def process(self, *df):
        base_df = df[0]
        return base_df.withColumn(const.EXTRACT_DT_COLUMN_NAME, current_timestamp())

