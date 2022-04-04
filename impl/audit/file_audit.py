from api.audit import IAudit

from impl.destination.file_destination import FileDestination
from constants import config_constants as CC


class FileAudit(IAudit, FileDestination):
    def __init__(self, context):

        audit_config_dict = context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG]
        self._write_mode = audit_config_dict[CC.MODE]
        self._write_format = audit_config_dict[CC.FORMAT]
        self._write_path = audit_config_dict[CC.PATH]
        self._write_options = audit_config_dict[CC.OPTIONS]

    def write(self, df):
        FileDestination.write(self, df)
