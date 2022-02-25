from api.IDestination import IDestination


class FileDestination(IDestination):
    def write(self, config_dict):
        """Load the configurations."""
        pass

    def write_as_full_load(self, df, config_dict):
        """Load the configurations."""
        pass

    def write_as_incremental(self, df, config_dict):
        """Load the configurations."""
        pass
