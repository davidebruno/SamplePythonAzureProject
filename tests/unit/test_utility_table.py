import pytest
import unittest
from unittest import mock
import logging

from tests.mocked_entities import mock_entity_1
from utility_table import ValidationError, validate_required_fields, get_entity

logger = logging.getLogger(__name__)

TEST_TABLE         = 'testtable'

class TestTable(unittest.TestCase):
    """
    Unit tests for the table module
    """

    def test_validate_required_fields_failure(self):
        entity1 = {}
        with pytest.raises(ValidationError) as exc_info:
            validate_required_fields(entity1)
        expected_msg = "One of the required fields is missing. Expected=FieldName1 FieldName2 FieldName3 missing=FieldName1 FieldName2 FieldName3"
        assert expected_msg in str(exc_info.value)

    @mock.patch('utility_table.get_entity')
    def test_get_one_entity(self, get_entity_mocked):
        """"
        Sample test that shows the use of mock tests to use when we want (or cannot ) avoid to access the storage system
        """
        get_entity_mocked.return_value = mock_entity_1
        PARTITION_KEY = 'partitionvalue'
        ROW_KEY = '1111111111'
        # field_name1 = 'ValField1Entity1'
        result = get_entity(TEST_TABLE, PARTITION_KEY, ROW_KEY)
        entity_1 = result[0]

        assert len(result) == 1
        compare_result       = result == mock_entity_1

        assert compare_result  is True
