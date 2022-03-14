import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from context.IngestionContext import IngestionContext
from factory import IngestionFactory
from utils.AuditDecorator import AuditDecorator
from utils.Logging import Log4j


def execute_ingestion(arguments, spark):
    # Get Configurator
    configurator = IngestionFactory.get_configurator(arguments)
    config_list = configurator.get_configuration(spark, arguments)
    logger = Log4j(spark)

    for config in config_list:
        # Execution with auditing enabled
        context = IngestionContext(args, config, spark, logger)
        executor_with_audit = AuditDecorator().call(execute)
        executor_with_audit(spark, context)

    pass


def execute(spark, context: IngestionContext):
    # creating instances
    logger = context.get_logger
    logger.info("ingestion started")
    source = IngestionFactory.get_source(context)
    processor = IngestionFactory.get_processor(context)
    destination = IngestionFactory.get_destination(context)

    df = source.read(context)
    df = processor.process(df, context)
    destination.write(df, context)
    count = df.count()
    logger.info("ingestion finished count:" + str(count))

    return count


# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'type', 'path', 'config_id', 'process_name',
                           'sub_process_name', 'object_name', 'application_path', 'audit_id'])
sc = SparkContext()
glue_context = GlueContext(sc)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

execute_ingestion(args, session)

job.commit()
