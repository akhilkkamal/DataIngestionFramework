from api.IConfigurator import IConfigurator
from pyspark.sql import SparkSession
from constants import ConfigConstants as CC


class FileConfigurator(IConfigurator):

    def get_configuration(self, spark: SparkSession, args):
        df = spark.read.option('multiline', 'true').json(args['path'])
        config_id = args[CC.CONFIG_ID]
        df = df.filter(df.config_id.isin(config_id.split(CC.CONFIG_ID_DELIMITER))).rdd.map(lambda row: row.asDict(True))
        return self.enrich_general_configuration(spark, args, df.collect())

    def enrich_general_configuration(self, spark, args, configs):
        df = spark.read.option('multiline', 'true').json(args['application_path'])
        df = df.rdd.map(lambda row: row.asDict(True))
        general_config_dict = df.collect()[0]
        for config in configs:
            config[CC.GENERAL_CONFIG] = general_config_dict
        return configs
        return df.collect()

    def enrich_general_config(self, config, args):
        pass

