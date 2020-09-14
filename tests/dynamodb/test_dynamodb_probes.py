# -*- coding: utf-8 -*-
import os
from json import loads
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from chaoslib.exceptions import FailedActivity

from chaosaws.dynamodb.probes import (
    describe_table, describe_continuous_backups, describe_backup)

data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def read_config(filename):
    with open(os.path.join(data_path, filename), 'r') as fh:
        return loads(fh.read())


def mock_client_error(*args, **kwargs):
    return ClientError(
        operation_name=kwargs['op'],
        error_response={'Error': {
            'Code': kwargs['Code'], 'Message': kwargs['Message']}})


@patch('chaosaws.dynamodb.probes.aws_client', autospec=True)
class TestDynamoDbProbesMock:
    def setup(self):
        self.table_name = 'MyTestDynamoTable'
        self.backup_arn = 'arn:aws:dynamodb:us-east-1:123456789012:table/' \
                          'MyTestDynamoTable/backup/01489602797149-73d8d5bc'

    def test_describe_table(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.describe_table.return_value = read_config(
            'describe_table_1.json')

        describe_table(table_name=self.table_name)
        client.describe_table.assert_called_with(TableName=self.table_name)

    def test_describe_table_exception_not_found(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.describe_table.side_effect = mock_client_error(
            op='DescribeTable',
            Code='ResourceNotFoundException',
            Message=''
        )

        with pytest.raises(FailedActivity) as e:
            describe_table(table_name=self.table_name)
        assert '' in str(e)

    def test_describe_continuous_backups(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.describe_continuous_backups.return_value = read_config(
            'describe_continuous_backups_1.json')

        describe_continuous_backups(self.table_name)
        client.describe_continuous_backups.assert_called_with(
            TableName=self.table_name)

    def test_describe_continuous_backups_exception_not_found(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.describe_continuous_backups.side_effect = mock_client_error(
            op='DescribeContinuousBackups',
            Code='TableNotFoundException',
            Message=''
        )

        with pytest.raises(FailedActivity) as e:
            describe_continuous_backups(self.table_name)
        assert '' in str(e)

    def test_describe_backup(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.describe_backup.return_value = read_config(
            'describe_backup_1.json')

        describe_backup(self.backup_arn)
        client.describe_backup.assert_called_with(BackupArn=self.backup_arn)

    def test_describe_backup_exception_not_found(self, aws_client):
        client = MagicMock()
        aws_client.return_value = client
        client.describe_backup.side_effect = mock_client_error(
            op='DescribeBackup',
            Code='BackupNotFoundException',
            Message=''
        )

        with pytest.raises(FailedActivity) as e:
            describe_backup('arn:invalid')
        assert '' in str(e)
