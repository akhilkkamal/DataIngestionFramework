import boto3
import json

"""This will read the secrets from secret manager"""


def get_secret(region_name, secret_name, secret_key):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    secret = json.loads(secret)
    return secret.get(secret_key)
