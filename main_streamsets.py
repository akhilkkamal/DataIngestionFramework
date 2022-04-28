import os
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from context.ingestion_context import IngestionContext
from factory import ingestion_factory as IngestionFactory
from handler.job_runner import JobRunner
from utils.logger import Logger
import argparse


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
args = {'JOB_NAME': '${JOB_NAME}', 'type': '${type}', 'project_config_path': '${project_config_path}', 'config_id': '${config_id}',
        'process_name': '${process_name}', 'sub_process_name': '${sub_process_name}',
        'object_name': '${object_name}', 'application_path': '${application_path}',
        'audit_id': '${audit_id}','job_config_path':'${job_config_path}'}

execute_ingestion(args, spark)

