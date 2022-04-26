from api.destination import IDestination
from pyspark.sql import DataFrame
from constants import config_constants as CC
from constants import ingestion_constants as IC
from utils import ingestion_utils as IngestionUtils


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
