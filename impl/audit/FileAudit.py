from api.IAudit import IAudit

from impl.destination.FileDestination import FileDestination
from constants import ConfigConstants as CC


class FileAudit(IAudit, FileDestination):
    def write(self, df, context):
        FileDestination.write_with_config(self, df, context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG])


class FileAudit(IAudit):
    def audit_log(self,  df,  config_dict, start_time, end_time, audit_id, process_name, message, status):
        """Audit logging"""
        pass

