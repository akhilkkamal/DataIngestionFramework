class ISource:
    def read(self, context, read_config):
        """Load the configurations."""
        pass

    def read_as_full_load(self, context, read_config):
        """Load the configurations."""
        pass

    def read_as_incremental(self, context, read_config):
        """Load the configurations."""
        pass
