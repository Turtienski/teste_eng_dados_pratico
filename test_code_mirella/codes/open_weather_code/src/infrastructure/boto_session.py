import boto3


def get_boto_session():
    return boto3.client(service_name='s3')
