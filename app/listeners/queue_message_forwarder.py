import json
import aiohttp
import asyncio
from datetime import datetime
from aio_pika import IncomingMessage
from starlette.responses import JSONResponse
from aio_pika.exceptions import ChannelClosed, ConnectionClosed

from app.utils.logging_util import setup_logger
from app.utils.rabbitmq_util import rabbitmq_util
from app.utils.constants import RABBITMQ_QUEUES
from app.services.publisher_service import publisher
from app.core.config import settings


class QueueMessageForwarder:

    def __init__(self):

        self.rabbitmq_util = rabbitmq_util
        self.logger = setup_logger(__name__)
        self.queue_name = ''

        # Add consumer health tracking
        self.consumer_start_time = {}
        self.last_message_processed = {}
        self.message_count = {}
        self.consumer_status = {}

    async def consume_and_forward_callback(self, message: IncomingMessage): #-> JSONResponse:

        """
        Callback function for processing document upload messages.

        Args:
            message (IncomingMessage): The incoming RabbitMQ message.

        Returns:
            JSONResponse: The response indicating the status of message processing.
        """

        # Track message processing
        queue_name = self.queue_name
        self.last_message_processed[queue_name] = datetime.utcnow()
        self.message_count[queue_name] = self.message_count.get(queue_name, 0) + 1

        data = json.loads(message.body)
        self.logger.info(f'Processing message #{self.message_count[queue_name]} for queue {queue_name}: {data}')
        # await message.ack()
        # return

        if self.queue_name in RABBITMQ_QUEUES['listen_queues']:
            for subscriber in RABBITMQ_QUEUES['listen_queues'][self.queue_name]:
                if subscriber['method'] == 'queue':
                    # match subscriber['method']:
                    #     case 'queue':
                    try:
                        self.logger.info('Forwarding message to queue...')
                        await message.ack()
                    except Exception as e:
                        self.logger.error(f"Unexpected error forwading message: {e}")
                        # publish to error queue
                        await self.forward_to_error_queue_and_nack(message, e)


                elif subscriber['method'] == 'endpoint':
                    # case 'endpoint':
                    self.logger.info('Forwarding message to endpoint...')
                    try:
                        to_print = data["data"]
                        print(f"Data property of message: {to_print}")
                        await self.forward_to_endpoint(subscriber, data["data"])
                        await message.ack()
                    except Exception as e:
                        self.logger.error(f"Unexpected error forwading message: {e}")
                        # publish to error queue
                        await self.forward_to_error_queue_and_nack(message, e)

    async def consume_and_forward(self, queue_name) -> None:

        """
        Consume messages from a given queue and forward it to the subscribers based on what's registered
        """

        self.queue_name = queue_name

        # Initialize consumer tracking
        self.consumer_start_time[queue_name] = datetime.utcnow()
        self.message_count[queue_name] = 0
        self.consumer_status[queue_name] = "starting"

        self.logger.info(f"Starting consumer for queue: {queue_name}")

        await self.rabbitmq_util.ensure_connection()

        try:
            self.consumer_status[queue_name] = "active"
            await self.rabbitmq_util.consume_message(
                queue_name=self.queue_name,
                callback=self.consume_and_forward_callback
            )

        except KeyboardInterrupt:
            self.logger.info('Message consumption stopped by user.')
            self.consumer_status[queue_name] = "stopped_by_user"

        except (ConnectionClosed, ChannelClosed) as e:
            self.logger.error(f'Connection error for queue {queue_name}: {e}')
            self.consumer_status[queue_name] = "connection_error"
            await self._handle_connection_error(queue_name)

        except Exception as e:
            self.logger.error(f"Unexpected error consuming messages from {queue_name}: {e}")
            self.consumer_status[queue_name] = "error"
            # Add retry logic with exponential backoff
            await asyncio.sleep(5)
            await self.consume_and_forward(queue_name)

    async def _handle_connection_error(self, queue_name):
        """Handle connection errors with proper reconnection logic"""
        self.logger.info(f'Handling connection error for queue {queue_name}...')
        self.consumer_status[queue_name] = "reconnecting"

        print('====ConnectionClosed encountered!===')
        self.logger.error(f'Error consuming message from queue {queue_name}')
        self.logger.info('Reconnecting to RabbitMQ...')
        await self.rabbitmq_util.setup_connection()

        if self.rabbitmq_util.connection and not self.rabbitmq_util.connection.is_closed:
            await asyncio.sleep(2)  # Brief delay before restart
            await self.consume_and_forward(queue_name)

    # async def consume_and_forward_to_ai_service(self, queue_name) -> None:

    #     """
    #     Consume messages from a given queue and forward it to the subscribers based on what's registered
    #     """

    #     self.queue_name = queue_name
    #     await self.rabbitmq_util.ensure_connection()

    #     try:
    #         await self.rabbitmq_util.consume_message(
    #             queue_name=self.queue_name,
    #             callback=self.consume_and_forward_callback
    #         )

    #     except KeyboardInterrupt:
    #         self.logger.info('Message consumption stopped by user.')

    #     except (ConnectionClosed, ChannelClosed) as e:
    #         self.logger.error(f'Error consuming message: {e}')
    #         self.logger.info('Reconnecting to RabbitMQ...')
    #         await self.rabbitmq_util.setup_connection()
    #         await self.consume_and_forward(queue_name)

    #     except Exception as e:
    #         self.logger.error(f"Unexpected error consuming messages: {e}")

    async def forward_to_queue(self, queue_name, message):
        try:
            await publisher.publish_message(queue_name, message)
            # submit to an endpoint instead

        except Exception as e:
            self.logger.error(f'Error while trying to forward message to queue {queue_name}: {e}')

    async def forward_to_endpoint(self, endpoint_info, message):

        method, url = await self.extract_url(endpoint_info['address'], message)

        self.logger.info(f'URL {url}')

        if url != '':

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                print('Session started')
                try:
                    # Post request to extract overview endpoint
                    self.logger.info(f'Sending {method} request to endpoint: {url}')
                    self.logger.info(f'Endpoint payload: {message}')

                    response = await getattr(session, method.lower())(url, json=message)

                    # async with getattr(session, method.lower())(url, json={"result_json": message}) as response:
                    print('Request sent')
                    self.logger.info(f'Request sent, with response status: {response.status}')

                    if response.status not in {200, 201}:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f'Got response {response.status} from endpoint'
                        )

                except aiohttp.ClientError as e:
                    self.logger.info(f'Failed to forward message to endpoint. Reason: {e}')
                    raise aiohttp.ClientError(f'Failed to forward message to endpoint. Reason: {e}')

                except Exception as e:
                    self.logger.error(f'Unexpected error occurred forwarding message to endpoint: {e}')
                    raise

    async def extract_url(self, address, data):

        method = ''
        url = ''

        if isinstance(address, str):
            url_array = address.split(' ')
            method = url_array[0]
            url = url_array[1]

        else:
            for conditional_address in address:
                condition = conditional_address['condition'].split(' == ')

                if data[condition[0]] == condition[1]:
                    url_array = conditional_address['address'].split(' ')
                    method = url_array[0]
                    url = url_array[1]
                    break

        return method, url

    async def forward_to_error_queue_and_nack(self, message, err):
        decoded_msg = json.loads(message.body)
        data = {
            'payload': decoded_msg["data"],
            'error': f'{err}'
        }
        try:
            await publisher.publish_message(settings.ERROR_QUEUE, data)

        except Exception as e:
            self.logger.error(f'Unexpected error occurred forwarding message to error queue: {e}')

        await message.nack(requeue=False)

    def get_consumer_status(self, queue_name: str) -> dict:
        """Get consumer health status"""
        now = datetime.utcnow()
        start_time = self.consumer_start_time.get(queue_name)
        last_message = self.last_message_processed.get(queue_name)

        status = {
            "queue_name": queue_name,
            "start_time": start_time.isoformat() if start_time else None,
            "uptime_seconds": (now - start_time).total_seconds() if start_time else 0,
            "last_message_time": last_message.isoformat() if last_message else None,
            "idle_seconds": (now - last_message).total_seconds() if last_message else None,
            "messages_processed": self.message_count.get(queue_name, 0),
            "status": self.consumer_status.get(queue_name, "unknown")
        }

        return status

    def get_all_consumer_statuses(self) -> list:
        """Get status for all consumers"""
        statuses = []
        for queue_name in RABBITMQ_QUEUES['listen_queues']:
            statuses.append(self.get_consumer_status(queue_name))
        return statuses


queue_message_forwarder = QueueMessageForwarder()