# -*- coding: utf-8 -*-
import boto3
from botocore.exceptions import ClientError
from chaoslib.exceptions import FailedActivity
from logzero import logger

from chaosaws.types import AWSResponse


def describe_dynamo_table(client: boto3.client,
                          table_name: str) -> AWSResponse:
    try:
        return client.describe_table(TableName=table_name)
    except ClientError as e:
        logger.exception(e.response['Error']['Message'])
        raise FailedActivity(e.response['Error']['Message'])


def describe_dynamo_backup(client: boto3.client,
                           backup_arn: str) -> AWSResponse:
    try:
        return client.describe_backup(
            BackupArn=backup_arn)['BackupDescription']
    except ClientError as e:
        logger.exception(e.response['Error']['Message'])
        raise FailedActivity(e.response['Error']['Message'])
