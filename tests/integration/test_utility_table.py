import pytest
import unittest
import logging
import time

from azure.cosmosdb.table import TableService

from config import get_storage_account
from tests.mocked_entities import mock_entity_1, mock_entity_2, mock_entity_3
from utility_table import query_between_rowkey

account = get_storage_account()
table_service = TableService(account_name=account.get_name(), account_key=account.get_key())

logger = logging.getLogger(__name__)

TEST_TABLE         = 'testtable'
TEST_QUERY_TABLE   = 'testquerytable'

class TableIntegrationTests(unittest.TestCase):
    """ Integration tests that access to Azure tables storage"""

    def setUp(self):
        self.table_service = TableService(account_name=account.get_name(), account_key=account.get_key())

        table_exists = self.table_service.exists(TEST_TABLE)
        if table_exists:
            logger.info(f'Table = {TEST_TABLE} exists already.')
        else:
            logger.info(f'Table = {TEST_TABLE} does not exist. Creating it.')
            self.table_service.create_table(TEST_TABLE)
            time.sleep(20)  # Allow 20 seconds for table to be created on Azure.

    def tearDown(self):
        exists_requests_table = self.table_service.delete_table(TEST_TABLE, fail_not_exist=False)
        time.sleep(20)
        if exists_requests_table:
            logger.info(f'Deleting tables again')
            self.table_service.delete_table(TEST_TABLE, fail_not_exist=False)
            time.sleep(20)  # Allow 20 seconds for table to be deleted on Azure.

    def test_query_between_rowkey(self):
        """
        This test verifies that the method query_between_rowkey return the expected values
        :return:
        """
        table_service.insert_or_replace_entity(TEST_QUERY_TABLE, mock_entity_1)
        # table_service.insert_or_replace_entity(TEST_QUERY_TABLE, mock_entity_2)
        # table_service.insert_or_replace_entity(TEST_QUERY_TABLE, mock_entity_3)

        MIN_ROW_KEY = '1111111111'
        MAX_ROW_KEY = '8888888888'
        PARTITION_KEY = 'partitionvalue'
        num_results = 10

        results = query_between_rowkey(TEST_QUERY_TABLE, PARTITION_KEY, MIN_ROW_KEY, MAX_ROW_KEY, num_results, val_marker=None)

        entity_result = results.items[0]
        #removing fields added during the DB insertion that are not meaningful for test validation
        del entity_result['etag']
        del entity_result['Timestamp']
        # Of the three entities inserted in the table the expected result from the query is the ByID entity
        compare_result = entity_result == mock_entity_1

        assert len(results.items) == 1
        assert compare_result is True