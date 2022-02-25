from factory import IngestionFactory
import argparse
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def execute_ingestion(spark, args):
    configurator = IngestionFactory.get_configurator(args)
    config_list = configurator.get_configuration(spark, args)
    for config in config_list:
        # Create Instances
        source = IngestionFactory.get_source(config)
        validator = IngestionFactory.get_validator(config)
        processor = IngestionFactory.get_processor(config)
        destination = IngestionFactory.get_destination(config)

        # Execution
        df = source.read(spark, config)
        df = validator.validate(spark, df, config)
        df = processor.process(spark,df, config)
        destination.write(df, config)
    pass


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'type', 'path', 'config_id'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

execute_ingestion(spark, args)

job.commit()
