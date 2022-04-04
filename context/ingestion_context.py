class IngestionContext:
    _config = None
    _session = None
    _logger = None
    _args = None

    def __init__(self, args, config, session, logger):
        self._args = args
        self._config = config
        self._session = session
        self._logger = logger

    @property
    def get_config(self):
        return self._config

    @property
    def get_session(self):
        return self._session

    @property
    def get_logger(self):
        return self._logger

    @property
    def get_args(self):
        return self._args
