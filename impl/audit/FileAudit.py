from api.IAudit import IAudit


class FileAudit(IAudit):
    def audit_log(self,  df,  config_dict, start_time, end_time, audit_id, process_name, message, status):
        """Audit logging"""
        pass
