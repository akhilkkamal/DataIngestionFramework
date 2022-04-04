from api.source import ISource
from constants import config_constants as CC


class FileSource(ISource):
    def __init__(self, context, read_config):
        self._spark = context.get_session
        self._read_config = read_config
        self._read_format = read_config[CC.FORMAT]
        self._read_path = read_config[CC.PATH]
        self._read_options = read_config[CC.OPTIONS]

    def read(self):
        return self._spark.read. \
            options(**self._read_options). \
            format(self._read_format). \
            load(self._read_path)
