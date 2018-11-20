from azure.cosmosdb.table.tableservice import TableService

import logging
from config import get_storage_account

logger = logging.getLogger(__name__)

account = get_storage_account()
table_service = TableService(account_name=account.get_name(), account_key=account.get_key())

class ValidationError(Exception):
    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

def create_table(table_name):
    """
    Create a table

    :param table_name: Name of the table
    :return: is_created: True if the table is created, False otherwise
    """
    table_exists = table_service.exists(table_name)
    if not table_exists:
       table_exists = table_service.create_table(table_name)
       logger.info(f'Created table with name = {table_name}')
    return table_exists

def delete_table(table_name):
    """
    Delete a table

    :param table_name: Name of the table
    :return: is_deleted: True if the table is deleted, False otherwise
    """
    table_exists = table_service.exists(table_name)
    if table_exists:
        is_deleted = table_service.delete_table(table_name)
        logger.info(f'Deleted Table name = {table_name}')

    return is_deleted

def validate_required_fields(entity):
    """
    Validates that the required fields are present in the entity.
    A ValidationError is raised if the validation fails.

    :param entity: The entity to be validated
    :returns: ValidationError if the required fields are not present
    """
    keys_found = set(entity.keys())
    keys_expected = {'FieldName1', 'FieldName2', 'FieldName3'}
    missing_keys = keys_expected - keys_found
    if missing_keys:
        error_message = f"One of the required fields is missing. Expected={keys_expected} missing={missing_keys}"
        raise ValidationError(error_message)

    for key in keys_expected:
        value = entity[key]
        if not value:
            raise ValidationError(f'The required field {key} is not populated')

def insert_entity(table_name, entity):
    """
    Insert an entity in the table

    :param table_name: Name of the table
    :param entity: entity to insert in the table
    :return: etag: string value of the entity's etag
    """
    if (validate_required_fields(entity)):
        etag = table_service.insert_entity(table_name, entity)
        logger.info(f'Entity inserted into table name = {table_name}, entity value = {entity}')
    return etag


def get_entity(table_name, partition_key, row_key):
    """
    Read an entity on the table

    :param table_name:
    :param partition_key:
    :param row_key:
    :return: entity_read: the entity read or throws exception if the entity is not present
    """
    entity_read = table_service.get_entity(table_name, partition_key, row_key)
    logger.info(f'Entity read from table name = {table_name}, entity value = {entity_read}')
    return entity_read

def update_entity(table_name, entity):
    """
    Updates an existing entity in a table. Throws if the entity does not exist.
    The update_entity operation replaces the entire entity and can be used to
    remove properties.

    :param table_name: Name of the table
    :param entity: entity to insert in the table
    :return: etag: string value of the entity's etag
    """
    if (validate_required_fields(entity)):
        etag = table_service.update_entity(table_name, entity)
        logger.info(f'Entity updated in table name = {table_name}, entity value = {entity_read}')
    return etag

def delete_entity(table_name, partition_key, row_key):
    """
    Delete an entity from the table, throws exception if the entity does not exist

    :param table_name: Name of the table
    :partition_key: patition key value
    :param row_key: row key value
    """
    table_service.delete_entity(table_name, partition_key, row_key)
    logger.info(f'Entity deleted, table name = {table_name}, partition key = {partition_key}, row key = {row_key}')

def query(table_name, partition_key, operator):
    """
    Returns a generator to list the entities in the table specified.

    :param table_name: Name of the table
    :param partition_key: the name of the partition
    :param operator: the operator to use in the query
    :return: result: ListGenerator containing the entity/ies
    """
    logger.info(f'table_query - Querying the table name={table_name}, PartitionKey={partition_key}, operator={operator}')
    result = table_service.query_entities(table_name,filter=f"PartitionKey {operator} '{partition_key}'")
    return result

def query_between_rowkey(table_name, partition_key, min_rowkey, max_rowkey, num_results, val_marker=None):
    """
    Returns a generator to list the entities in the table specified. The
    generator will lazily follow the continuation tokens returned by the
    service and stop when all entities have been returned or num_results is
    reached.

    :param table_name: Name of the table
    :param partition_key: the name of the partition
    :param min_rowkey: the RowKey value for which greather RowKeys values are considered
    :param max_rowkey: the RowKey value for which smaller RowKeys values are considered
    :param num_results: the maximum number of entities retrieved from the query
    :param val_marker: the marker value, default value is None
    :return: result: ListGenerator containing the entity/ies
    """
    logger.info(f'table_query - Querying the table name={table_name}, PartitionKey={partition_key}, '
                    f' min_rowkey={min_rowkey}, max_rowkey={max_rowkey},num results={num_results}')
    filter_by = f"PartitionKey eq '{partition_key}' and (RowKey gt '{min_rowkey}' and RowKey lt '{max_rowkey}')"
    result = table_service.query_entities(table_name, num_results=num_results, marker=val_marker, filter = filter_by)
    return result


