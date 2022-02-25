from api.IConfigurator import IConfigurator
from pyspark.sql import SparkSession


class FileConfigurator(IConfigurator):
    def get_configuration(self, spark: SparkSession, args):
        df = spark.read.json(args['path'])
        config_id = args['config_id']
        df = df.filter(df.config_id == config_id).rdd.map(lambda row: row.asDict(True))
        return df.collect()
