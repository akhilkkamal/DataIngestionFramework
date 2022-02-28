from api.IDestination import IDestination
from pyspark.sql import DataFrame


class FileDestination(IDestination):
    def write(self, df: DataFrame, config_dict):
        write_mode = config_dict['write_mode']
        write_format = config_dict['write_format']
        write_path = config_dict['write_path']
        df.write.mode(write_mode).format(write_format).save(write_path)

    def write_as_full_load(self, df, config_dict):
        """Load the configurations."""
        pass

    def write_as_incremental(self, df, config_dict):
        """Load the configurations."""
        pass
