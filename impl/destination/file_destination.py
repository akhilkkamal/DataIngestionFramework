from api.destination import IDestination
from pyspark.sql import DataFrame
from constants import config_constants as CC


class FileDestination(IDestination):
    def __init__(self, context):
        self._partition_column = None
        self._context = context
        write_config_dict = context.get_config[CC.WRITE_CONFIG]
        self._write_mode = write_config_dict[CC.MODE]
        self._write_format = write_config_dict[CC.FORMAT]
        self._write_path = write_config_dict[CC.PATH]
        self._write_options = write_config_dict[CC.OPTIONS]
        if CC.PARTITION_COLUMNS in write_config_dict:
            self._partition_column = write_config_dict[CC.PARTITION_COLUMNS]

    def write(self, df: DataFrame):
        if self._partition_column:
            df.write.options(**self._write_options). \
                mode(self._write_mode). \
                format(self._write_format). \
                partitionBy(*self._partition_column.split(CC.DELIMITER)). \
                save(self._write_path)
        else:
            df.write.options(**self._write_options). \
                mode(self._write_mode). \
                format(self._write_format). \
                save(self._write_path)
