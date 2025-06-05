import json
import asyncio
from typing import Any
from datetime import datetime
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
        self._reconnecting = False
        self._reconnection_lock = asyncio.Lock()
        self._recovery_task = None  # Continuous recovery task
        self._recovery_interval = 30  # Start with 30 seconds
        self._max_recovery_interval = 300  # Max 5 minutes

    async def setup_connection(self, retries=5, delay=5) -> None:
        self.logger.info('Starting connection setup...')

        # Use lock to prevent concurrent connection attempts
        async with self._reconnection_lock:
            # Check if connection is already good
            if self.connection and not self.connection.is_closed:
                self.logger.debug("Connection already established and healthy")
                # Stop a recovery task if running
                await self._stop_recovery_task()
                return

            self._reconnecting = True

            try:
                for attempt in range(retries):
                    self.logger.info(f'Connection attempt {attempt + 1}/{retries}...')
                    try:
                        self.logger.debug('Attempting RabbitMQ connection...')

                        # Add timeout to prevent hanging
                        self.connection = await asyncio.wait_for(
                            connect_robust(
                                self.server,
                                heartbeat=60,
                                timeout=30,
                                client_properties={"connection_name": "bia project"}
                            ),
                            timeout=45  # 45 second timeouts
                        )

                        self.logger.debug('Connection established, creating channel...')
                        self.channel = await asyncio.wait_for(
                            self.connection.channel(),
                            timeout=30
                        )

                        self.logger.debug('Channel created, setting QoS...')
                        await asyncio.wait_for(
                            self.channel.set_qos(prefetch_count=1),
                            timeout=15
                        )

                        self.logger.info('RabbitMQ connection established successfully')

                        # Add health monitoring setup
                        self.connection_established_at = datetime.utcnow()
                        self.last_activity = datetime.utcnow()

                        # Start a health check task
                        await self._start_health_check_task()

                        # Stop a recovery task if running
                        await self._stop_recovery_task()

                        # Reset a recovery interval on successful connection
                        self._recovery_interval = 30

                        return

                    except asyncio.TimeoutError:
                        self.logger.error(f'Connection attempt {attempt + 1} timed out')
                        await self._cleanup_failed_connection()

                    except AMQPError as e:
                        self.logger.error(f'AMQP error on attempt {attempt + 1}: {e}')
                        await self._cleanup_failed_connection()

                    except Exception as e:
                        self.logger.error(f'Unexpected error on attempt {attempt + 1}: {e}')
                        await self._cleanup_failed_connection()

                    if attempt < retries - 1:
                        self.logger.info(f'Waiting {delay} seconds before retry...')
                        await asyncio.sleep(delay)

                # All attempts failed - start continuous recovery
                self.logger.error(f'All {retries} connection attempts failed, starting continuous recovery')
                await self._start_recovery_task()
                raise ConnectionError(f"Failed to establish RabbitMQ connection after {retries} attempts")

            finally:
                self._reconnecting = False

    async def _cleanup_failed_connection(self):
        """Clean up after a failed connection attempt"""
        try:
            if self.connection and not self.connection.is_closed:
                await asyncio.wait_for(self.connection.close(), timeout=10)
        except Exception as e:
            self.logger.warning(f"Error cleaning up failed connection: {e}")
        finally:
            self.connection = None
            self.channel = None

    async def _start_recovery_task(self):
        """Start a continuous recovery task"""
        if self._recovery_task and not self._recovery_task.done():
            self.logger.debug("Recovery task already running")
            return

        if self._recovery_task:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass

        self._recovery_task = asyncio.create_task(self._continuous_recovery())
        self.logger.info(f"Started continuous recovery task (interval: {self._recovery_interval}s)")

    async def _stop_recovery_task(self):
        """Stop a continuous recovery task"""
        if self._recovery_task and not self._recovery_task.done():
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
            self._recovery_task = None
            self.logger.debug("Recovery task stopped")

    async def _continuous_recovery(self):
        """Continuously attempt to recover connection"""
        while True:
            try:
                await asyncio.sleep(self._recovery_interval)

                # Check if we need to recover
                if not self.connection or self.connection.is_closed:
                    self.logger.info(f"Attempting connection recovery (interval: {self._recovery_interval}s)")
                    try:
                        await self.setup_connection(retries=1, delay=1)
                        # If successful, this task will be cancelled by setup_connection
                        break
                    except Exception as e:
                        self.logger.warning(f"Recovery attempt failed: {e}")
                        # Exponential backoff with max limit
                        self._recovery_interval = min(self._recovery_interval * 1.5, self._max_recovery_interval)
                        self.logger.debug(f"Increased recovery interval to {self._recovery_interval}s")
                else:
                    # Connection is healthy, stop recovery
                    self.logger.debug("Connection is healthy, stopping recovery task")
                    break

            except asyncio.CancelledError:
                self.logger.debug("Recovery task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Recovery task error: {e}")
                await asyncio.sleep(30)  # Wait before retrying

    async def _start_health_check_task(self):
        """Start a health check task if not already running"""
        if self._health_check_task and not self._health_check_task.done():
            self.logger.debug("Health check task already running")
            return

        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        self._health_check_task = asyncio.create_task(self._periodic_health_check())
        self.logger.debug("Health check task started")

    async def _periodic_health_check(self):
        """Periodically check connection health and reconnect if stale"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)

                if await self._should_reconnect():
                    self.logger.warning("Connection is stale, forcing reconnection...")
                    # Schedule reconnection as a separate task to avoid self-cancellation
                    asyncio.create_task(self._handle_stale_connection())
                    break  # Exit this health checks a task gracefully
                else:
                    # Test connection health with a lightweight operation
                    await self._test_connection_health()

            except asyncio.CancelledError:
                self.logger.debug("Health check task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Health check error: {e}")
                # Continue the loop after error, don't break

    async def _handle_stale_connection(self):
        """Handle stale connection in a separate task"""
        try:
            await self._force_reconnection()
        except Exception as e:
            self.logger.error(f"Failed to handle stale connection: {e}")

    async def _should_reconnect(self) -> bool:
        """Determine if connection should be refreshed"""
        # Don't reconnect if already reconnecting
        if self._reconnecting:
            return False

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
                # Simple channel operation to test health with timeout
                test_channel = await asyncio.wait_for(
                    self.connection.channel(),
                    timeout=10
                )
                await asyncio.wait_for(test_channel.close(), timeout=5)
                self.logger.debug("Connection health check passed")
            else:
                raise Exception("Connection is None or closed")
        except asyncio.TimeoutError:
            self.logger.warning("Connection health check timed out")
            if not self._reconnecting:
                asyncio.create_task(self._handle_stale_connection())
        except Exception as e:
            self.logger.warning(f"Connection health check failed: {e}")
            # Schedule reconnection as separate task
            if not self._reconnecting:
                asyncio.create_task(self._handle_stale_connection())

    async def _force_reconnection(self):
        """Force connection reconnection"""
        if self._reconnecting:
            self.logger.debug("Reconnection already in progress, skipping...")
            return

        self.logger.info("Forcing connection reconnection...")

        try:
            # Cancel health check task
            if self._health_check_task and not self._health_check_task.done():
                self._health_check_task.cancel()
                try:
                    await asyncio.wait_for(self._health_check_task, timeout=5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    self.logger.debug("Health check task cancelled for reconnection")
                self._health_check_task = None

            # Close existing connection with timeout
            if self.connection and not self.connection.is_closed:
                try:
                    await asyncio.wait_for(self.connection.close(), timeout=10)
                    self.logger.debug("Old connection closed")
                except asyncio.TimeoutError:
                    self.logger.warning("Timeout closing old connection, forcing cleanup")
                except Exception as e:
                    self.logger.warning(f"Error closing old connection: {e}")

            # Reset connection state
            self.connection = None
            self.channel = None

            # Establish new connection
            await self.setup_connection()
            self.logger.info("Connection reconnection completed successfully")

        except Exception as e:
            self.logger.error(f"Failed to reconnect: {e}")
            # Reset timestamps to avoid immediate retry
            self.connection_established_at = None
            self.last_activity = None
            # Start recovery task if not already running
            await self._start_recovery_task()

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
            await self.channel.default_exchange.publish(
                Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
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
                if self.channel:
                    pass
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
        # Stop all background tasks
        await self._stop_recovery_task()

        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
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

        # Check actual connection state
        is_connected = self.connection and not self.connection.is_closed

        status = {
            "connected": is_connected,
            "connection_established_at": self.connection_established_at.isoformat() if self.connection_established_at else None,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "current_time": now.isoformat(),
            "health_check_interval": self.health_check_interval,
            "max_idle_time": self.max_idle_time,
            "max_connection_age": self.max_connection_age,
            "health_check_running": self._health_check_task and not self._health_check_task.done(),
            "reconnecting": self._reconnecting,
            "recovery_running": self._recovery_task and not self._recovery_task.done(),
            "recovery_interval": self._recovery_interval
        }

        if self.connection_established_at and is_connected:
            status["connection_age_seconds"] = (now - self.connection_established_at).total_seconds()

        if self.last_activity and is_connected:
            status["idle_seconds"] = (now - self.last_activity).total_seconds()

        return status


rabbitmq_util = RabbitMQUtil()
