import os
import boto3
import logging

log = logging.getLogger(__name__)

def get_dynamodb_table(table_name, region_name=None):
    region = region_name or os.environ.get("AWS_REGION", "us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name=region)
    return dynamodb.Table(table_name)
