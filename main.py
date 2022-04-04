import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from context.ingestion_context import IngestionContext
from factory import ingestion_factory as IngestionFactory
from handler.job_runner import JobRunner
from utils.logger import Logger


def execute_ingestion(arguments, spark):
    # Get Configurator
    configurator = IngestionFactory.get_configurator(arguments, spark)
    config_list = configurator.get_configuration()
    logger = Logger(spark)

    for config in config_list:
        # Execution with auditing enabled
        context = IngestionContext(args, config, spark, logger)
        executor_with_audit = JobRunner(context).call(execute)
        executor_with_audit()


def execute(context: IngestionContext):
    # creating instances
    logger = context.get_logger
    logger.info("ingestion started")
    sources = IngestionFactory.get_source(context)
    processor = IngestionFactory.get_processor(context)
    destination = IngestionFactory.get_destination(context)
    df_array = [source.read() for source in sources]
    df = processor.process(*df_array)
    destination.write(df)
    count = df.count()
    logger.info("ingestion finished count:" + str(count))

    return count


# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'type', 'job_config_path', 'config_id', 'process_name',
                           'sub_process_name', 'object_name', 'project_config_path', 'audit_id'])
sc = SparkContext()
glue_context = GlueContext(sc)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

execute_ingestion(args, session)

job.commit()

# TODO: For Streamsets 
# @params: [JOB_NAME]
# args = {'JOB_NAME': '${JOB_NAME}', 'type': '${type}', 'path': '${path}', 'config_id': '${config_id}',
#         'process_name': '${process_name}', 'sub_process_name': '${sub_process_name}',
#         'object_name': '${object_name}', 'application_path': '${application_path}',
#         'audit_id': '${audit_id}'}
# 
# execute_ingestion(args, spark)
