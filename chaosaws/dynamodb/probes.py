# -*- coding: utf-8 -*-
from botocore.exceptions import ClientError
from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets
from logzero import logger

from chaosaws import aws_client
from chaosaws.dynamodb.shared import (
    describe_dynamo_table, describe_dynamo_backup)
from chaosaws.types import AWSResponse

__all__ = ['describe_table', 'describe_continuous_backups', 'describe_backup']


def describe_table(table_name: str,
                   configuration: Configuration = None,
                   secrets: Secrets = None) -> AWSResponse:
    """
    Describe a DynamoDB table

    :param table_name: The name of the DynamoDB table
    :param configuration:
    :param secrets:
    :return: Dict[str, Any]
    """
    client = aws_client('dynamodb', configuration, secrets)
    return describe_dynamo_table(client, table_name)


def describe_continuous_backups(table_name: str,
                                configuration: Configuration = None,
                                secrets: Secrets = None) -> AWSResponse:
    """
    Get the status of continuous backups for a DynamoDB table

    :param table_name: The name of the DynamoDB table
    :param configuration:
    :param secrets:
    :return: Dict[str, Any]
    """
    client = aws_client('dynamodb', configuration, secrets)
    try:
        return client.describe_continuous_backups(TableName=table_name)
    except ClientError as e:
        logger.exception(e.response['Error']['Message'])
        raise FailedActivity(e.response['Error']['Message'])


def describe_backup(backup_arn: str,
                    configuration: Configuration = None,
                    secrets: Secrets = None) -> AWSResponse:
    """
    Get information about a specific DynamoDB table backup

    :param backup_arn: The ARN of the backup
    :param configuration:
    :param secrets:
    :return: Dict[str, Any]
    """
    client = aws_client('dynamodb', configuration, secrets)
    return describe_dynamo_backup(client, backup_arn)
