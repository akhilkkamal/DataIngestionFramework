from api.IDestination import IDestination
from pyspark.sql import DataFrame
from constants import ConfigConstants as CC
from constants import IngestionConstants as IC
from utils import IngestionUtils


class JdbcDestination(IDestination):

    def write(self, df: DataFrame, context):
        write_config_dict = context.get_config[CC.WRITE_CONFIG]
        self.write_with_config(df, write_config_dict)

    def write_with_config(self, df, write_config_dict):
        write_mode = write_config_dict[CC.MODE]
        write_path = write_config_dict[CC.PATH]
        url = IngestionUtils.get_secret(IC.REGION, write_config_dict[CC.CREDENTIALS], IC.URL)
        username = IngestionUtils.get_secret(IC.REGION, write_config_dict[CC.CREDENTIALS], IC.USER)
        password = IngestionUtils.get_secret(IC.REGION, write_config_dict[CC.CREDENTIALS], IC.PASSWORD)

        df.write.format("jdbc"). \
            option("url", url). \
            option("dbtable", write_path). \
            option("user", username). \
            option("password", password). \
            mode(write_mode).save()

    def write_as_full_load(self, df, context):
        """Load the configurations."""
        pass

    def write_as_incremental(self, df, context):
        """Load the configurations."""
        pass
