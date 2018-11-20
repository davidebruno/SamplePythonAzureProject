import logging
import logging.config

from config import SAMPLE_TABLE
from utility_table import get_entity, query_between_rowkey

from azure.storage.common.models import ListGenerator

logger = logging.getLogger(__name__)
logging.getLogger('azure.storage.common.storageclient').setLevel(logging.WARNING)

PARTITION_KEY = 'partitionvalue'
MIN_ROWKEY = ''
MAX_ROWKEY = ''
NUM_ENTITIES_PER_BLOCK = 10

def main():
        done = False
        while not done:
            list_entities: ListGenerator = query_between_rowkey(SAMPLE_TABLE, PARTITION_KEY, MIN_ROWKEY, MAX_ROWKEY, NUM_ENTITIES_PER_BLOCK, next_marker)
            if len(list_entities.items) > 0:
                logger.info(f'Sample Table - processing entities retrieved')

            for entity in list_entities:
                check_entities = get_entity(SAMPLE_TABLE, PARTITION_KEY, entity['ID'])
                if not check_entities:
                    logger.debug(f"Request entity not found, skipped call - request_id={entity['ID']}")
                    continue

            next_marker = list_entities.next_marker
            if not next_marker:
                done = True

if __name__ == '__main__':
    main()
