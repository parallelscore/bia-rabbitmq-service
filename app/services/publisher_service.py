from typing import Any, Dict
from pika.exceptions import ChannelClosed, ConnectionClosed

from app.utils.logging_util import setup_logger
from app.utils.rabbitmq_util import rabbitmq_util


class PublisherService:

    def __init__(self):

        self.rabbitmq_util = rabbitmq_util
        self.logger = setup_logger(__name__)

    async def safe_publish(self, queue_name: str, message: Dict[str, Any]) -> None:

        """
        Safely publish a message to a queue, reopening the channel if necessary.

        Args:
            queue_name (str): The name of the queue.
            message (Dict[str, Any]): The message to be published.
        """

        try:
            await self.rabbitmq_util.publish_message(queue_name, message)
        except (ChannelClosed, ConnectionClosed) as e:
            self.logger.error(f'Channel/Connection closed, attempting to reopen: {e}')
            await self.rabbitmq_util.reopen_channel()
            await self.rabbitmq_util.publish_message(queue_name, message)

        except Exception as e:
            self.logger.error(f'Failed to publish message: {str(e)}')

    async def publish_message(self, queue_name: str, message: Dict[str, Any]) -> None:

        """
        Publish messages to the  queue.
        """

        try:

            await self.rabbitmq_util.declare_queue(queue_name)

            self.logger.info(f'Starting to publish messages to the queue: {queue_name}')

            await self.safe_publish(queue_name, message)

        except Exception as e:
            self.logger.error(f'Error publishing message: {e}')
            raise e


publisher = PublisherService()
