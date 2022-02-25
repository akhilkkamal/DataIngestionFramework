from api.IProcessor import IProcessor
from pyspark.sql import SparkSession



class AuditColumnEnricher(IProcessor):
    def process(self,spark,  df, config):
        return df

