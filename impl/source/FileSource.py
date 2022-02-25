from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from api.ISource import ISource
from pyspark.sql import SparkSession


class FileSource(ISource):
    def read(self, spark: SparkSession, config_dict):
        data2 = [("James", "", "Smith", "36636", "M", 3000),
                 ("Michael", "Rose", "", "40288", "M", 4000),
                 ("Robert", "", "Williams", "42114", "M", 4000),
                 ("Maria", "Anne", "Jones", "39192", "F", 4000),
                 ("Jen", "Mary", "Brown", "", "F", -1)
                 ]

        schema = StructType([
            StructField("firstname", StringType(), True),
            StructField("middlename", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("id", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", IntegerType(), True)
        ])

        return spark.createDataFrame(data=data2, schema=schema)

    def read_as_full_load(self, spark, config_dict):
        """Load the configurations."""
        pass

    def read_as_incremental(self, spark, config_dict):
        """Load the configurations."""
        pass
