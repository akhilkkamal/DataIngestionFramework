from api.IOffsetTracker import IOffsetTracker


class FileOffsetTracker(IOffsetTracker):
    def get_offset(self,  config):
        """Load the configurations."""
        pass

    def put_offset(self, df,  config):
        """Load the configurations."""
        pass


