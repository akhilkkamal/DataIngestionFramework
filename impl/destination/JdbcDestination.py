from api.IDestination import IDestination
from pyspark.sql import DataFrame
from constants import ConfigConstants as CC
from constants import IngestionConstants as IC
from utils import IngestionUtils


class JdbcDestination(IDestination):

    def __init__(self, context):
        self._context = context
        write_config_dict = context.get_config[CC.WRITE_CONFIG]
        self._write_mode = write_config_dict[CC.MODE]
        self._write_path = write_config_dict[CC.PATH]
        self._write_mode = write_config_dict[CC.MODE]
        self._write_path = write_config_dict[CC.PATH]
        self._url = IngestionUtils.get_secret(IC.REGION, write_config_dict[CC.CREDENTIALS], IC.URL)
        self._username = IngestionUtils.get_secret(IC.REGION, write_config_dict[CC.CREDENTIALS], IC.USER)
        self._password = IngestionUtils.get_secret(IC.REGION, write_config_dict[CC.CREDENTIALS], IC.PASSWORD)

    def write(self, df: DataFrame):
        df.write.format("jdbc"). \
            option("url", self._url). \
            option("dbtable", self._write_path). \
            option("user", self._username). \
            option("password", self._password). \
            mode(self._write_mode).save()
