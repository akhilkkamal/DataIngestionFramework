from api.audit import IAudit
from impl.destination.jdbc_destination import JdbcDestination
from constants import config_constants as CC
from utils import ingestion_utils
from constants import ingestion_constants as IC


class JdbcAudit(IAudit, JdbcDestination):
    def __init__(self, context):
        #JdbcDestination.__init__(self, context)
        audit_config_dict = context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG]
        self._write_mode = audit_config_dict[CC.MODE]
        self._write_path = audit_config_dict[CC.PATH]
        self._write_mode = audit_config_dict[CC.MODE]
        self._write_path = audit_config_dict[CC.PATH]
        self._url = IngestionUtils.\
            get_secret(IC.REGION, audit_config_dict[CC.CREDENTIALS], IC.URL)
        self._username = IngestionUtils.\
            get_secret(IC.REGION, audit_config_dict[CC.CREDENTIALS], IC.USER)
        self._password = IngestionUtils.\
            get_secret(IC.REGION, audit_config_dict[CC.CREDENTIALS], IC.PASSWORD)

    def write(self, df):
        JdbcDestination.write(self, df)
