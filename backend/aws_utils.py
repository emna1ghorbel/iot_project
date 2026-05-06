import os
import boto3
import logging
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

def get_boto3_resource(service_name, region_name=None):
    """
    Returns a boto3 resource for the specified service.
    """
    region = region_name or os.environ.get("AWS_REGION", "us-east-1")
    try:
        return boto3.resource(service_name, region_name=region)
    except Exception as e:
        log.error(f"Failed to create Boto3 resource for {service_name}: {e}")
        raise

def get_boto3_client(service_name, region_name=None, endpoint_url=None):
    """
    Returns a boto3 client for the specified service.
    """
    region = region_name or os.environ.get("AWS_REGION", "us-east-1")
    try:
        return boto3.client(service_name, region_name=region, endpoint_url=endpoint_url)
    except Exception as e:
        log.error(f"Failed to create Boto3 client for {service_name}: {e}")
        raise

def get_dynamodb_table(table_name, region_name=None):
    """
    Returns a DynamoDB Table object.
    """
    dynamodb = get_boto3_resource("dynamodb", region_name=region_name)
    return dynamodb.Table(table_name)

def get_iot_data_client(region_name=None, endpoint_url=None):
    """
    Returns an AWS IoT Data Plane client.
    """
    # Use default endpoint if not provided
    endpoint = endpoint_url or os.environ.get("IOT_ENDPOINT")
    if endpoint and not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    
    return get_boto3_client("iot-data", region_name=region_name, endpoint_url=endpoint)
