class IDestination:
    def write(self, df,  context):
        """Load the configurations."""
        pass

    def write_as_full_load(self,  df, context):
        """Load the configurations."""
        pass

    def write_as_incremental(self,  df, context):
        """Load the configurations."""
        pass
