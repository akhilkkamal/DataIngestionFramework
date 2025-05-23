
class Logger(object):
    """Wrapper class for Log4j JVM object.
    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        app_id = spark.sparkContext.getConf().get('spark.app.id')
        app_name = spark.sparkContext.getConf().get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'

        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log an warning.
        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None

    def debug(self, message):
        """Log information.
        :param: debug message to write to log
        :return: None
        """
        self.logger.debug(message)
        return None