import os
import logging

# Store the Azure account info into environment variables
AZURE_ACCOUNT_NAME = os.environ.get('AZURE_ACCOUNT_NAME')
AZURE_ACCOUNT_KEY = os.environ.get('AZURE_ACCOUNT_KEY')

SAMPLE_TABLE = 'sampletable'

logger = logging.getLogger(__name__)

logger.info(f'Using AZURE_ACCOUNT_NAME={AZURE_ACCOUNT_NAME}')
logger.info(f'Using AZURE_ACCOUNT_KEY={AZURE_ACCOUNT_KEY}')


class Account:
    name: str
    key: str

    def __init__(self, name, key):
        self.name = name
        self.key  = key

    def get_name(self):
        return self.name

    def get_key(self):
        return self.key

def get_storage_account():
    """
    Wrapper function used to manage the account

    :return: acc_storage: the instance of Account class
    """
    acc_storage = Account(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY)

    return acc_storage

