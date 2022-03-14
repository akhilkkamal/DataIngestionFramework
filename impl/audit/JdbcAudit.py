from api.IAudit import IAudit
from impl.destination.JdbcDestination import JdbcDestination
from constants import ConfigConstants as CC


class JdbcAudit(IAudit, JdbcDestination):
    def write(self, df, context):
        JdbcDestination.write_with_config(self, df, context.get_config[CC.GENERAL_CONFIG][CC.AUDIT_CONFIG])
