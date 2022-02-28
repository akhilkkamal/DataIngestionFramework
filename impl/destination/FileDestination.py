from api.IDestination import IDestination
from pyspark.sql import DataFrame
from constants import ConfigConstants as CC


class FileDestination(IDestination):
    def write(self, df: DataFrame, config_dict):
        write_mode = config_dict[CC.WRITE_MODE]
        write_format = config_dict[CC.WRITE_FORMAT]
        write_path = config_dict[CC.WRITE_PATH]
        write_options = config_dict[CC.WRITE_OPTIONS]
        df.write.options(**write_options).mode(write_mode).format(write_format).save(write_path)

    def write_as_full_load(self, df, config_dict):
        """Load the configurations."""
        pass

    def write_as_incremental(self, df, config_dict):
        """Load the configurations."""
        pass
