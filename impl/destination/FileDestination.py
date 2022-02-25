from api.IDestination import IDestination
from pyspark.sql import SparkSession



class FileDestination(IDestination):
    def write(self, df,config_dict):
        pass

    def write_as_full_load(self, df, config_dict):
        """Load the configurations."""
        pass

    def write_as_incremental(self,  df, config_dict):
        """Load the configurations."""
        pass
