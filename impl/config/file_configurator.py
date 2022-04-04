from api.IConfigurator import IConfigurator
from pyspark.sql import SparkSession
from constants import ConfigConstants as CC


class FileConfigurator(IConfigurator):
    def __init__(self, args, spark: SparkSession):
        self._args = args
        self._spark = spark
        self._config_id = args[CC.CONFIG_ID]
        self._project_config_path = args[CC.PROJECT_CONFIG_PATH]
        self._job_config_path = args[CC.JOB_CONFIG_PATH]

    def get_configuration(self):
        df = self._spark.read.option('multiline', 'true').json(self._job_config_path)
        df = df.filter(df.config_id.isin(self._config_id.split(CC.DELIMITER))). \
            rdd.map(lambda row: row.asDict(True))
        return self.enrich_general_configuration(df.collect())

    def enrich_general_configuration(self, configs):
        df = self._spark.read.option('multiline', 'true').json(self._project_config_path)
        df = df.rdd.map(lambda row: row.asDict(True))
        general_config_dict = df.collect()[0]
        for config in configs:
            config[CC.GENERAL_CONFIG] = general_config_dict
        return configs
