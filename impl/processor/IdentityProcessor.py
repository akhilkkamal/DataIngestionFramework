from api.IProcessor import IProcessor
import constants.IngestionConstants as const
from pyspark.sql.functions import current_timestamp


class ExtractDateEnrichProcessor(IProcessor):
    def __init__(self, context):
        pass

    def process(self, *df):
        return df[0]

