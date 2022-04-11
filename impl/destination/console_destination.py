from api.destination import IDestination
from pyspark.sql import DataFrame


# This implementation is used for displaying processed data frame in console
# Use this for testing and debugging
class ConsoleDestination(IDestination):
    def __init__(self, context):
        self._context = context

    def write(self, df: DataFrame):
        df.show()
