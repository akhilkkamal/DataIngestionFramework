from api.IOffsetTracker import IOffsetTracker
from constants import ConfigConstants as CC


class FileOffsetTracker(IOffsetTracker):
    def get_offset(self, spark, config):
        spark.read.json(config[CC.OFFSET_PATH]).rdd.map(lambda row: row.asDict(True)).collect()

    def put_offset(self,spark, df,  config):
        """Load the configurations."""
        spark.write.json(config[CC.OFFSET_PATH])


