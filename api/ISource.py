class ISource:
    def read(self, spark, config_dict):
        """Load the configurations."""
        pass

    def read_as_full_load(self, spark,config_dict):
        """Load the configurations."""
        pass

    def read_as_incremental(self,spark, config_dict):
        """Load the configurations."""
        pass
