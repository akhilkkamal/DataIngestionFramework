from api.ISource import ISource


class FileSource(ISource):
    def read(self, config_dict):
        """Load the configurations."""
        pass

    def read_as_full_load(self, config_dict):
        """Load the configurations."""
        pass

    def read_as_incremental(self, config_dict):
        """Load the configurations."""
        pass
