import json
import aiohttp
from aio_pika import IncomingMessage
from starlette.responses import JSONResponse
from pika.exceptions import ChannelClosed, ConnectionClosed

from app.utils.logging_util import setup_logger
from app.utils.rabbitmq_util import rabbitmq_util
from app.utils.constants import RABBITMQ_QUEUES
from app.services.publisher_service import publisher


class QueueMessageForwader:

    def __init__(self):

        self.rabbitmq_util = rabbitmq_util
        self.logger = setup_logger(__name__)
        self.queue_name = ''


    async def consume_and_forward_callback(self, message: IncomingMessage): #-> JSONResponse:

        """
        Callback function for processing document upload messages.

        Args:
            message (IncomingMessage): The incoming RabbitMQ message.

        Returns:
            JSONResponse: The response indicating the status of message processing.
        """

        data = json.loads(message.body)

        if self.queue_name in RABBITMQ_QUEUES:
            for subscriber in RABBITMQ_QUEUES[self.queue_name]['subscribers']:
                match subscriber['method']:
                    case 'queue':
                        self.logger.info('Forwarding message to queue...')
                        self.logger.info(f'MEssage: {data}')
                        await message.ack()
                        # await self.forward_to_queue(subscriber['address'], data)
                    
                    case 'endpoint':
                        self.logger.info('Forwarding message to endpoint...')
                        await self.forward_to_endpoint(self, subscriber, data)


    async def consume_and_forward(self, queue_name) -> None:

        """
        Consume messages from a given queue and forward it to the subscribers based on what's registered
        """

        self.queue_name = queue_name
        await self.rabbitmq_util.ensure_connection()

        try:
            await self.rabbitmq_util.consume_message(
                queue_name=self.queue_name,
                callback=self.consume_and_forward_callback
            )

        except KeyboardInterrupt:
            self.logger.info('Message consumption stopped by user.')

        except (ConnectionClosed, ChannelClosed) as e:
            self.logger.error(f'Error consuming message: {e}')
            self.logger.info('Reconnecting to RabbitMQ...')
            await self.rabbitmq_util.setup_connection()
            await self.consume_ai_analysis_extraction_result()

        except Exception as e:
            self.logger.error(f"Unexpected error consuming messages: {e}")


    async def consume_and_forward_to_ai_service(self, queue_name) -> None:

        """
        Consume messages from a given queue and forward it to the subscribers based on what's registered
        """

        self.queue_name = queue_name
        await self.rabbitmq_util.ensure_connection()

        try:
            await self.rabbitmq_util.consume_message(
                queue_name=self.queue_name,
                callback=self.consume_and_forward_callback
            )

        except KeyboardInterrupt:
            self.logger.info('Message consumption stopped by user.')

        except (ConnectionClosed, ChannelClosed) as e:
            self.logger.error(f'Error consuming message: {e}')
            self.logger.info('Reconnecting to RabbitMQ...')
            await self.rabbitmq_util.setup_connection()
            await self.consume_ai_analysis_extraction_result()

        except Exception as e:
            self.logger.error(f"Unexpected error consuming messages: {e}")


    async def forward_to_queue(self, queue_name, message):
        try:
            await publisher.publish_message(queue_name, message)
            # submit to an endpoint instead

        except Exception as e:
            self.logger.error(f'Error while trying to forward message to queue {queue_name}: {e}')

    
    async def forward_to_endpoint(self, endpoint_info, message):
        
        url = endpoint_info['address'].split(' ')
        self.logger.info(f'URL {url}')

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:

            try:
                # Post request to extract overview endpoint
                self.logger.info(f'Sending {url[0]} request to endpoint: {url[1]}')
                
                async with getattr(session, url[0].lower())({url[1]}, json={"result_json": message}) as response:

                    self.logger.info(f'Request sent, with response status: {response.status}')

                    if response.status in {200, 201}:
                        await message.ack()

                    else:
                        await message.nack(requeue=True)

            except aiohttp.ClientError as e:
                self.logger.info(f'Failed to forward message to endpoint. Reason: {e}')

                await message.nack(requeue=True)

            except Exception as e:
                self.logger.error(f'Unexpected error occurred forwarding message to endpoint: {e}')

                await message.nack(requeue=True)


queue_message_forwader = QueueMessageForwader()
