import json
import asyncio
from typing import Any
from aio_pika.exceptions import AMQPError
from aio_pika.abc import AbstractRobustConnection
from aio_pika import connect_robust, Message, DeliveryMode

from app.core.config import settings
from app.utils.logging_util import setup_logger


class RabbitMQUtil:

    def __init__(self):
        self.server = settings.RABBITMQ_URL
        self.connection: AbstractRobustConnection = None
        self.channel = None

        self.logger = setup_logger(__name__)

    async def setup_connection(self, retries=5, delay=5) -> None:
        for attempt in range(retries):
            try:
                self.connection = await connect_robust(
                    self.server,
                    heartbeat=60,
                    timeout=30,
                    client_properties={"connection_name": "bia project"}
                )
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)
                self.logger.info('RabbitMQ connection established')
                return
            except AMQPError as e:
                self.logger.error('Failed to connect to RabbitMQ (attempt %d/%d): %s', attempt + 1, retries, e)
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    raise e

    async def declare_queue(self, queue_name) -> Any:
        await self.ensure_connection()
        try:
            queue = await self.channel.declare_queue(queue_name)
            self.logger.info("Declared queue '%s'", queue_name)
            return queue
        except AMQPError as e:
            self.logger.error("Failed to declare queue '%s': %s", queue_name, e)
            raise e

    async def publish_message(self, queue_name, message) -> None:
        await self.ensure_connection()
        try:
            await self.channel.default_exchange.publish(
                Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT
                ),
                routing_key=queue_name
            )
            self.logger.info("Message published to queue '%s'", queue_name)
        except AMQPError as e:
            self.logger.error("Failed to publish message to queue '%s': %s", queue_name, e)
            raise e

        finally:
            try:
                # Ensure the channel or connection is properly closed if necessary
                if self.channel:
                    # await self.channel.close()
                    await self.close_connection()
            except Exception as cleanup_error:
                self.logger.error("Failed to properly close the channel: %s", cleanup_error)
        # finally:
        #     try:
        #         # Ensure the channel or connection is properly closed if necessary
        #         if self.channel:
        #             await self.channel.close()
        #     except Exception as cleanup_error:
        #         self.logger.error("Failed to properly close the channel: %s", cleanup_error)

    async def consume_message(self, queue_name, callback) -> None:
        await self.ensure_connection()
        try:
            queue = await self.declare_queue(queue_name)
            await queue.consume(callback)
            self.logger.info("Started consuming message from queue '%s'", queue_name)
            
        except AMQPError as e:
            self.logger.error("Failed to consume from queue '%s': %s", queue_name, e)
            raise e

    async def close_connection(self) -> None:
        if self.connection and not self.connection.is_closed:
            try:
                await self.connection.close()
                self.logger.info('RabbitMQ connection closed')
            except AMQPError as e:
                self.logger.error('Failed to close RabbitMQ connection: %s', e)
                raise e

    async def ensure_connection(self) -> None:
        if not self.connection or self.connection.is_closed:
            self.logger.warning('RabbitMQ connection lost, reconnecting...')

            # Attempt to cleanly close the existing connection if it exists and is open
            if self.connection and not self.connection.is_closed:
                try:
                    await self.connection.close()
                except Exception as e:
                    self.logger.error("Error while closing the old connection: %s", e)

            # Set up a new connection
            await self.setup_connection()

    async def initialize(self):
        await self.setup_connection()


rabbitmq_util = RabbitMQUtil()
