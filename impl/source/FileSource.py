from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from api.ISource import ISource
from pyspark.sql import SparkSession


class FileSource(ISource):
    def read(self, spark: SparkSession, config_dict):
        read_format = config_dict['read_format']
        read_path = config_dict['read_path']
        read_options = config_dict['read_options']
        return spark.read.options(**read_options).format(read_format).load(read_path)

    def read_as_full_load(self, spark, config_dict):
        """Load the configurations."""
        pass

    def read_as_incremental(self, spark, config_dict):
        """Load the configurations."""
        pass
