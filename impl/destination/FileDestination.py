from api.IDestination import IDestination
from pyspark.sql import DataFrame
from constants import ConfigConstants as CC


class FileDestination(IDestination):
    def write(self, df: DataFrame, context):
        write_config_dict = context.get_config[CC.WRITE_CONFIG]
        self.write_with_config(df, write_config_dict)

    def write_with_config(self, df, write_config_dict):
        write_mode = write_config_dict[CC.MODE]
        write_format = write_config_dict[CC.FORMAT]
        write_path = write_config_dict[CC.PATH]
        write_options = write_config_dict[CC.OPTIONS]
        df.write.options(**write_options).mode(write_mode).format(write_format).save(write_path)

    def write_as_full_load(self, df, context):
        """Load the configurations."""
        pass

    def write_as_incremental(self, df, context):
        """Load the configurations."""
        pass
