# -*- coding: utf-8 -*-
import os
from json import loads
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from chaoslib.exceptions import FailedActivity

from chaosaws.dynamodb.actions import (
    delete_table, backup_table, set_continuous_backups)

data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def read_config(filename):
    with open(os.path.join(data_path, filename), 'r') as fh:
        return loads(fh.read())


def mock_client_error(*args, **kwargs):
    return ClientError(
        operation_name=kwargs['op'],
        error_response={'Error': {
            'Code': kwargs['Code'], 'Message': kwargs['Message']}})


@patch('chaosaws.dynamodb.actions.aws_client', autospec=True)
class TestDynamoDbActionsMock:
    def test_delete_table_no_backup(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.delete_table.return_value = read_config('delete_table_1.json')

        delete_table(table_name='MyTestDynamoTable')
        client.delete_table.assert_called_with(TableName='MyTestDynamoTable')

    def test_delete_table_with_backup(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.delete_table.return_value = read_config('delete_table_1.json')
        client.create_backup.return_value = read_config('create_backup_1.json')
        client.describe_backup.side_effect = [
            read_config('describe_backup_1.json'),
            read_config('describe_backup_2.json')
        ]

        delete_table(
            table_name='MyTestDynamoTable',
            create_backup=True,
            backup_name='MyTestDynamoTableBackup')

        client.create_backup.assert_called_with(
            TableName='MyTestDynamoTable',
            BackupName='MyTestDynamoTableBackup')
        client.describe_backup.assert_called_with(
            BackupArn='arn:aws:dynamodb:us-east-1:123456789012:table/'
                      'MyTestDynamoTable/backup/01489602797149-73d8d5bc')
        client.delete_table.assert_called_with(TableName='MyTestDynamoTable')

    def test_backup_table(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.create_backup.return_value = read_config('create_backup_1.json')
        client.describe_backup.side_effect = [
            read_config('describe_backup_1.json'),
            read_config('describe_backup_2.json')
        ]

        backup_table('MyTestDynamoTable', 'MyTestDynamoTableBackup')

        client.create_backup.assert_called_with(
            TableName='MyTestDynamoTable',
            BackupName='MyTestDynamoTableBackup')
        client.describe_backup.assert_called_with(
            BackupArn='arn:aws:dynamodb:us-east-1:123456789012:table/'
                      'MyTestDynamoTable/backup/01489602797149-73d8d5bc')

    def test_set_continuous_backups(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.delete_table.return_value = read_config(
            'update_continuous_backups_1.json')

        set_continuous_backups('MyTestDynamoTable', True)

        client.update_continuous_backups.assert_called_with(
            TableName='MyTestDynamoTable',
            PointInTimeRecoverySpecification={
                'PointInTimeRecoveryEnabled': True})

    def test_delete_table_exception_not_found(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.delete_table.side_effect = mock_client_error(
            op='DeleteTable',
            Code='ResourceNotFoundException',
            Message=''
        )

        with pytest.raises(FailedActivity) as e:
            delete_table('MyInvalidTable')
        assert '' in str(e)

    def test_delete_table_exception_no_backup_name(self, aws_client):
        with pytest.raises(FailedActivity) as e:
            delete_table(table_name='MyTestDynamoTable', create_backup=True)
        assert '"backup_name" required when creating a backup' in str(e)

    def test_backup_table_exception_not_found(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.create_backup.side_effect = mock_client_error(
            op='CreateBackup',
            Code='TableNotFoundException',
            Message=''
        )

        with pytest.raises(FailedActivity) as e:
            backup_table('MyInvalidTable', 'MyTestDynamoTableBackup')
        assert '' in str(e)

    def test_set_continuous_backups_exception_not_found(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.update_continuous_backups.side_effect = mock_client_error(
            op='UpdateContinuousBackups',
            Code='TableNotFoundException',
            Message=''
        )

        with pytest.raises(FailedActivity) as e:
            set_continuous_backups('MyInvalidTable', True)
        assert '' in str(e)
