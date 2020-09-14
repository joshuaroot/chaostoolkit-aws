# -*- coding: utf-8 -*-
from time import sleep

import boto3
from botocore.exceptions import ClientError
from chaoslib.exceptions import FailedActivity
from chaoslib.types import Configuration, Secrets
from logzero import logger

from chaosaws import aws_client
from chaosaws.dynamodb.shared import describe_dynamo_backup
from chaosaws.types import AWSResponse

__all__ = ['delete_table', 'backup_table', 'set_continuous_backups']


def delete_table(table_name: str,
                 create_backup: bool = False,
                 backup_name: str = None,
                 configuration: Configuration = None,
                 secrets: Secrets = None) -> AWSResponse:
    """
    Delete a DynamoDB table and all the items contained within

    :param table_name: The name of the DynamoDB table
    :param create_backup: Create a backup of the table before deleting
    :param backup_name: The name of the backup to create
    :param configuration:
    :param secrets:
    :return: Dict[str, Any]
    """
    client = aws_client('dynamodb', configuration, secrets)
    if create_backup:
        if not backup_name:
            raise FailedActivity(
                '"backup_name" required when creating a backup')
        backup_table(
            table_name, backup_name, configuration, secrets)

    try:
        return client.delete_table(TableName=table_name)
    except ClientError as e:
        logger.exception(e.response['Error']['Message'])
        raise FailedActivity(e.response['Error']['Message'])


def backup_table(table_name: str,
                 backup_name: str,
                 configuration: Configuration = None,
                 secrets: Secrets = None) -> AWSResponse:
    """
    Create a backup for an existing DynamoDB table

    :param table_name: The name of the DynamoDB table
    :param backup_name: The name of the backup to create
    :param configuration:
    :param secrets:
    :return: Dict[str, Any]
    """
    client = aws_client('dynamodb', configuration, secrets)
    try:
        response = client.create_backup(
            TableName=table_name, BackupName=backup_name)

        __wait_for_backup_to_complete(
            client, response['BackupDetails']['BackupArn'])

        return response
    except ClientError as e:
        logger.exception(e.response['Error']['Message'])
        raise FailedActivity(e.response['Error']['Message'])


def set_continuous_backups(table_name: str,
                           point_in_time_enabled: bool,
                           configuration: Configuration = None,
                           secrets: Secrets = None) -> AWSResponse:
    """
    Toggle point in time recovery for the specified table

    :param table_name: The name of DynamoDB table
    :param point_in_time_enabled: Enable/disable recovery
    :param configuration:
    :param secrets:
    :return: Dict[str, Any]
    """
    client = aws_client('dynamodb', configuration, secrets)
    try:
        return client.update_continuous_backups(
            TableName=table_name,
            PointInTimeRecoverySpecification={
                'PointInTimeRecoveryEnabled': point_in_time_enabled})
    except ClientError as e:
        logger.exception(e.response['Error']['Message'])
        raise FailedActivity(e.response['Error']['Message'])


###############################################################################
# Private Functions ###########################################################
###############################################################################
def __wait_for_backup_to_complete(client: boto3.client, backup_arn: str):
    backup = describe_dynamo_backup(client, backup_arn)
    while backup['BackupDetails']['BackupStatus'] != 'AVAILABLE':
        sleep(5)
        backup = describe_dynamo_backup(client, backup_arn)
