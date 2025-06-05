import json
import asyncio
from typing import Any
from datetime import datetime
from aio_pika import ExchangeType
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

        # Add health monitoring attributes
        self.last_activity = None
        self.connection_established_at = None
        self.health_check_interval = getattr(settings, 'RABBITMQ_HEALTH_CHECK_INTERVAL', 3600)  # 1 hour
        self.max_idle_time = getattr(settings, 'RABBITMQ_MAX_IDLE_TIME', 21600)  # 6 hours
        self.max_connection_age = getattr(settings, 'RABBITMQ_MAX_CONNECTION_AGE', 86400)  # 24 hours
        self._health_check_task = None

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

                # Add health monitoring setup
                self.connection_established_at = datetime.utcnow()
                self.last_activity = datetime.utcnow()

                # Start health check task
                if self._health_check_task:
                    self._health_check_task.cancel()
                self._health_check_task = asyncio.create_task(self._periodic_health_check())

                return
            except AMQPError as e:
                self.logger.error('Failed to connect to RabbitMQ (attempt %d/%d): %s', attempt + 1, retries, e)
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    raise e

    async def _periodic_health_check(self):
        """Periodically check connection health and reconnect if stale"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)

                if await self._should_reconnect():
                    self.logger.warning("Connection is stale, forcing reconnection...")
                    await self._force_reconnection()
                else:
                    # Test connection health with a lightweight operation
                    await self._test_connection_health()

            except asyncio.CancelledError:
                self.logger.info("Health check task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Health check error: {e}")

    async def _should_reconnect(self) -> bool:
        """Determine if connection should be refreshed"""
        now = datetime.utcnow()

        # Check connection age
        if self.connection_established_at:
            connection_age = (now - self.connection_established_at).total_seconds()
            if connection_age > self.max_connection_age:
                self.logger.info(f"Connection age ({connection_age}s) exceeds limit ({self.max_connection_age}s)")
                return True

        # Check idle time
        if self.last_activity:
            idle_time = (now - self.last_activity).total_seconds()
            if idle_time > self.max_idle_time:
                self.logger.info(f"Connection idle time ({idle_time}s) exceeds limit ({self.max_idle_time}s)")
                return True

        return False

    async def _test_connection_health(self):
        """Test connection with lightweight operation"""
        try:
            if self.connection and not self.connection.is_closed:
                # Simple channel operation to test health
                test_channel = await self.connection.channel()
                await test_channel.close()
                self.logger.debug("Connection health check passed")
        except Exception as e:
            self.logger.warning(f"Connection health check failed: {e}")
            await self._force_reconnection()

    async def _force_reconnection(self):
        """Force connection reconnection"""
        try:
            # Cancel health check task
            if self._health_check_task:
                self._health_check_task.cancel()
                self._health_check_task = None

            if self.connection and not self.connection.is_closed:
                await self.connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing old connection: {e}")

        # Reset connection state
        self.connection = None
        self.channel = None

        await self.setup_connection()

    def _update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.utcnow()

    async def declare_queue(self, queue_name) -> Any:
        await self.ensure_connection()
        try:
            queue = await self.channel.declare_queue(queue_name, durable=True, auto_delete=False)
            self.logger.info("Declared queue '%s'", queue_name)
            self._update_activity()  # Track activity
            return queue
        except AMQPError as e:
            self.logger.error("Failed to declare queue '%s': %s", queue_name, e)
            raise e

    async def publish_message(self, queue_name, message, routing_key=None, pattern=None) -> None:
        await self.ensure_connection()
        try:
            actual_routing_key = routing_key or queue_name
            # exchange = await self.channel.declare_exchange('topic_exchange', ExchangeType.TOPIC) #NOSONAR
            # headers = {"pattern": pattern} if pattern else {} #NOSONAR
            await self.channel.default_exchange.publish(
                Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    # headers=headers #NOSONAR
                ),
                routing_key=actual_routing_key
            )
            self.logger.info("Message published to queue '%s'", queue_name)
            self._update_activity()  # Track activity
        except AMQPError as e:
            self.logger.error("Failed to publish message to queue '%s': %s", queue_name, e)
            raise e

        finally:
            try:
                # Ensure the channel or connection is properly closed if necessary
                if self.channel:
                    pass #NOSONAR
                    # await self.channel.close() #NOSONAR
                    # await self.close_connection() #NOSONAR
            except Exception as cleanup_error:
                self.logger.error("Failed to properly close the channel: %s", cleanup_error)

    async def consume_message(self, queue_name, callback) -> None:
        await self.ensure_connection()

        # Wrap callback to track activity
        async def activity_tracking_callback(message):
            self._update_activity()
            return await callback(message)

        try:
            queue = await self.declare_queue(queue_name)
            await queue.consume(activity_tracking_callback)
            self.logger.info("Started consuming message from queue '%s'", queue_name)

        except AMQPError as e:
            self.logger.error("Failed to consume from queue '%s': %s", queue_name, e)
            raise e

    async def close_connection(self) -> None:
        # Cancel health check task
        if self._health_check_task:
            self._health_check_task.cancel()
            self._health_check_task = None

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

    def get_connection_status(self) -> dict:
        """Get current connection status for health checks"""
        now = datetime.utcnow()

        status = {
            "connected": self.connection and not self.connection.is_closed,
            "connection_established_at": self.connection_established_at.isoformat() if self.connection_established_at else None,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "current_time": now.isoformat(),
            "health_check_interval": self.health_check_interval,
            "max_idle_time": self.max_idle_time,
            "max_connection_age": self.max_connection_age
        }

        if self.connection_established_at:
            status["connection_age_seconds"] = (now - self.connection_established_at).total_seconds()

        if self.last_activity:
            status["idle_seconds"] = (now - self.last_activity).total_seconds()

        return status


rabbitmq_util = RabbitMQUtil()