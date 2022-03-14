from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from context.IngestionContext import IngestionContext
from api.ISource import ISource
from constants import ConfigConstants as CC


class FileSource(ISource):

    def read_as_full_load(self, context, read_config):
        spark = context.get_session
        read_config_dict = read_config

        read_format = read_config_dict[CC.FORMAT]
        read_path = read_config_dict[CC.PATH]
        read_options = read_config_dict[CC.OPTIONS]
        return spark.read.options(**read_options).format(read_format).load(read_path)

    def read_as_incremental(self, context, read_config):
        return self.read_as_full_load(context, read_config)

    def read(self, context, read_config):
        config_dict = context.get_config

        if config_dict[CC.INGESTION_TYPE] == CC.FULL_LOAD:
            return self.read_as_full_load(context, read_config)
        else:
            return self.read_as_incremental(context, read_config)
