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


parser = argparse.ArgumentParser()
parser.add_argument('--JOB_NAME', help="job name")
parser.add_argument('--type', help="config type")
parser.add_argument('--job_config_path', help="job config path")
parser.add_argument('--config_id', help="config id")
parser.add_argument('--process_name', help="process name")
parser.add_argument('--sub_process_name', help="sub process name")
parser.add_argument('--object_name', help="object name")
parser.add_argument('--project_config_path', help="project config path")
parser.add_argument('--audit_id', help="audit id")
parser.add_argument('--python_path', help="python path eg: C:/Users/user_name/python/")
parser.add_argument('--py4j_path',
                    help="py4j path eg: C:/Users/user_name/Downloads/spark-3.2.1-bin-hadoop3.2/python/lib/py4j-0.10.9.3-src.zip")
parser.add_argument('--pyspark_path',
                    help="pyspark path eg: C:/Users/user_name/Downloads/spark-3.2.1-bin-hadoop3.2/python/pyspark/")

# Parse and print the results
parsed_args = parser.parse_args()
args = vars(parsed_args)


sys.path.append(args['python_path'])
sys.path.append(args['py4j_path'])
sys.path.append(args['pyspark_path'])

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

session = SparkSession.builder.master("local[*]").appName(args['JOB_NAME']).getOrCreate()
execute_ingestion(args, session)
