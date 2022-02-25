from api.IOffsetTracker import IOffsetTracker


class FileOffsetTracker(IOffsetTracker):
    def get_offset(self, spark, config):
        """Load the configurations."""
        pass

    def put_offset(self,spark, df,  config):
        """Load the configurations."""
        pass


