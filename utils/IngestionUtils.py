import boto3
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *

"""This will read the secrets from secret manager"""


def get_secret(region_name, secret_name, secret_key):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    return secret.get(secret_key)


def get_logger(name, mode):
    logging.basicConfig(level=mode)
    logger = logging.getLogger(name)
    return logger


def get_empty_df(spark):
    emp_rdd = spark.sparkContext.emptyRDD()
    # Create empty schema
    columns = StructType([])
    # Create an empty RDD with empty schema
    return spark.createDataFrame(data=emp_rdd,
                                 schema=columns)
