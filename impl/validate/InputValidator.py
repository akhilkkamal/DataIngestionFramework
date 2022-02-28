from pyspark.sql import SparkSession

from api.IValidator import IValidator


class InputValidator(IValidator):
    def validate(self,spark,  df1, config):
        """Load the configurations."""
        return  df1

