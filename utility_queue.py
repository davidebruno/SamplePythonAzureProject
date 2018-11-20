
import logging

from azure.storage.queue import QueueService
from config import get_storage_account

logger = logging.getLogger(__name__)

account = get_storage_account()
queue_service = QueueService(account_name=account.get_name(), account_key=account.get_key())


def create_queue(queue_name):
    """
    Create the queues that will be used from all others component
    to avoid that a queue is called before being created
    """
    queue_exists = queue_service.exists(queue_name)
    if not queue_exists:
        logger.info(f'Creating queue name={queue_name}')
        queue_service.create_queue(queue_name)


def put(queue_name, message):
    logger.info(f'Adding the message = {message} to queue = {queue_name}')
    queue_service.put_message(queue_name, message)


def get(queue_name, num_messages=1):
    """
    Returns a generator which provides messages from the queue with the specified name

    :param queue_name: Name of the queue
    :param num_messages: The number of message to read from the queue. Default=1
    :return:
    """
    messages = queue_service.get_messages(queue_name, num_messages=num_messages)
    for message in messages:
        logger.info(f'id={message.id}, content={message.content}')
        yield message

def delete_message(queue_name, message_id, pop_receipt):
    """
    Delete a single message from the queue

    :param queue_name:
    :param message_id:
    :param pop_receipt:
    :return:
    """
    logger.info(f'Delete message id={message_id}')
    queue_service.delete_message(queue_name, message_id, pop_receipt)



