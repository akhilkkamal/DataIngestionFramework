from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from api.ISource import ISource
from pyspark.sql import SparkSession
from constants import ConfigConstants as CC
from factory import IngestionFactory


class FileSource(ISource):

    def read_as_full_load(self, spark, config_dict):
        read_config_dict=config_dict[CC.READ_CONFIG]
        read_format = read_config_dict[CC.FORMAT]
        read_path = read_config_dict[CC.PATH]
        read_options = read_config_dict[CC.OPTIONS]
        return spark.read.options(**read_options).format(read_format).load(read_path)

    def read_as_incremental(self, spark, config_dict):
        return self.read_as_full_load(spark, config_dict)

    def read(self, spark: SparkSession, config_dict):
        if config_dict[CC.INGESTION_TYPE] == CC.FULL_LOAD:
            return self.read_as_full_load(spark, config_dict)
        else:
            return self.read_as_incremental(spark, config_dict)
