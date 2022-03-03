from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from api.ISource import ISource
from pyspark.sql import SparkSession
from constants import ConfigConstants as CC
from factory import IngestionFactory


class FileSource(ISource):

    def read_as_full_load(self, spark, config_dict):
        read_format = config_dict[CC.READ_FORMAT]
        read_path = config_dict[CC.READ_PATH]
        read_options = config_dict[CC.READ_OPTIONS]
        return spark.read.options(**read_options).format(read_format).load(read_path)

    def read_as_incremental(self, spark, config_dict):
        self.read_as_full_load(spark, config_dict)

    def read(self, spark: SparkSession, config_dict):
        if config_dict[CC.INGESTION_TYPE] == CC.FULL_LOAD:
            self.read_as_full_load(spark, config_dict)
        else:
            self.read_as_incremental(spark, config_dict)
