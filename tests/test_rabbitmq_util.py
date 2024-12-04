import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from aio_pika.exceptions import AMQPError
from app.utils.rabbitmq_util import RabbitMQUtil  # Ensure this path matches your module location

@pytest.fixture
def rabbitmq_util():
    """Fixture to initialize RabbitMQUtil instance."""
    return RabbitMQUtil()


@pytest.mark.asyncio
async def test_setup_connection_success(rabbitmq_util):
    with patch("app.utils.rabbitmq_util.connect_robust", new_callable=AsyncMock) as mock_connect, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_connect.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        await rabbitmq_util.setup_connection()

        assert mock_connect.called, "connect_robust was not called. Check the path for the patch."
        assert rabbitmq_util.connection == mock_connection, "Connection not set correctly."
        assert rabbitmq_util.channel == mock_channel, "Channel not set correctly."
        # mock_logger.info.assert_called_with('RabbitMQ connection established')


@pytest.mark.asyncio
async def test_setup_connection_retries_on_failure(rabbitmq_util):
    with patch("app.utils.rabbitmq_util.connect_robust", new_callable=AsyncMock, side_effect=AMQPError("Connection failed")), \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        with pytest.raises(AMQPError):
            await rabbitmq_util.setup_connection(retries=3, delay=0.1)

        assert mock_logger.error.call_count == 3, "Expected three retries but got a different count."


@pytest.mark.asyncio
async def test_declare_queue_success(rabbitmq_util):
    with patch.object(rabbitmq_util, "ensure_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "channel", new=AsyncMock()) as mock_channel, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        queue_name = "test_queue"
        mock_queue = AsyncMock()
        mock_channel.declare_queue.return_value = mock_queue

        queue = await rabbitmq_util.declare_queue(queue_name)

        mock_channel.declare_queue.assert_called_once_with(queue_name, durable=True, auto_delete=False)
        assert queue == mock_queue


@pytest.mark.asyncio
async def test_declare_queue_failure(rabbitmq_util):
    with patch.object(rabbitmq_util, "ensure_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "channel", new=AsyncMock()) as mock_channel, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        queue_name = "test_queue"
        mock_channel.declare_queue.side_effect = AMQPError("Failed to declare queue")

        with pytest.raises(AMQPError):
            await rabbitmq_util.declare_queue(queue_name)


@pytest.mark.asyncio
async def test_publish_message_success(rabbitmq_util):
    with patch.object(rabbitmq_util, "ensure_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "channel", new=AsyncMock()) as mock_channel, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        queue_name = "test_queue"
        message = {"key": "value"}
        await rabbitmq_util.publish_message(queue_name, message)

        mock_channel.default_exchange.publish.assert_called_once()
        # mock_logger.info.assert_called_with("Message published to queue '%s'", queue_name)


@pytest.mark.asyncio
async def test_publish_message_failure(rabbitmq_util):
    with patch.object(rabbitmq_util, "ensure_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "channel", new=AsyncMock()) as mock_channel, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        queue_name = "test_queue"
        message = {"key": "value"}
        mock_channel.default_exchange.publish.side_effect = AMQPError("Publish failed")

        with pytest.raises(AMQPError):
            await rabbitmq_util.publish_message(queue_name, message)


@pytest.mark.asyncio
async def test_consume_message_success(rabbitmq_util):
    with patch.object(rabbitmq_util, "ensure_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "declare_queue", new_callable=AsyncMock) as mock_declare_queue, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        queue_name = "test_queue"
        callback = AsyncMock()
        mock_queue = AsyncMock()
        mock_declare_queue.return_value = mock_queue

        await rabbitmq_util.consume_message(queue_name, callback)

        mock_declare_queue.assert_called_once_with(queue_name)
        mock_queue.consume.assert_called_once_with(callback)


@pytest.mark.asyncio
async def test_consume_message_failure(rabbitmq_util):
    with patch.object(rabbitmq_util, "ensure_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "declare_queue", new_callable=AsyncMock) as mock_declare_queue, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        queue_name = "test_queue"
        callback = AsyncMock()
        mock_declare_queue.side_effect = AMQPError("Consume failed")

        with pytest.raises(AMQPError):
            await rabbitmq_util.consume_message(queue_name, callback)


@pytest.mark.asyncio
async def test_close_connection_success(rabbitmq_util):
    with patch.object(rabbitmq_util, "connection", new=AsyncMock()) as mock_connection, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        mock_connection.is_closed = False

        await rabbitmq_util.close_connection()

        mock_connection.close.assert_called_once()
        mock_logger.info.assert_called_with('RabbitMQ connection closed')

@pytest.mark.asyncio
async def test_close_connection_already_closed(rabbitmq_util):
    with patch.object(rabbitmq_util, "connection", new=AsyncMock()) as mock_connection, \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        mock_connection.is_closed = True

        await rabbitmq_util.close_connection()

        mock_connection.close.assert_not_called()

@pytest.mark.asyncio
async def test_ensure_connection_reconnects_if_closed(rabbitmq_util):
    with patch.object(rabbitmq_util, "setup_connection", new_callable=AsyncMock), \
         patch.object(rabbitmq_util, "connection", new=MagicMock(is_closed=True)), \
         patch.object(rabbitmq_util, "logger") as mock_logger:

        await rabbitmq_util.ensure_connection()

        rabbitmq_util.setup_connection.assert_called_once()
        mock_logger.warning.assert_called_with('RabbitMQ connection lost, reconnecting...')
