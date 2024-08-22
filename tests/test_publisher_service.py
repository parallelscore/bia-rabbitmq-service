import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pika.exceptions import ChannelClosed, ConnectionClosed

from app.services.publisher_service import PublisherService


class TestPublisherHandler:
    @pytest.fixture
    def publish_handler(self, ):
        with patch('api.handlers.publish_handler.setup_logger') as mock_setup_logger:
            mock_setup_logger.return_value = MagicMock()
            return PublisherService()

    @pytest.mark.asyncio
    async def test_safe_publish_success(self, publish_handler):
        queue_name = 'test_queue'
        message = {'key': 'value'}

        publish_handler.rabbitmq_util.publish_message = AsyncMock()
        await publish_handler.safe_publish(queue_name, message)

        publish_handler.rabbitmq_util.publish_message.assert_awaited_once_with(queue_name, message)

    @pytest.mark.asyncio
    async def test_safe_publish_channel_closed(self, publish_handler):
        queue_name = 'test_queue'
        message = {'key': 'value'}

        publish_handler.rabbitmq_util.publish_message = AsyncMock(side_effect=[
            ChannelClosed(reply_code=200, reply_text="Channel closed"),
            None
        ])
        publish_handler.rabbitmq_util.reopen_channel = AsyncMock()

        await publish_handler.safe_publish(queue_name, message)

        publish_handler.rabbitmq_util.reopen_channel.assert_awaited_once()
        assert publish_handler.rabbitmq_util.publish_message.await_count == 2

    @pytest.mark.asyncio
    async def test_safe_publish_connection_closed(self, publish_handler):
        queue_name = 'test_queue'
        message = {'key': 'value'}

        publish_handler.rabbitmq_util.publish_message = AsyncMock(side_effect=[
            ConnectionClosed(reply_code=200, reply_text='Connection closed'),
            None
        ])
        publish_handler.rabbitmq_util.reopen_channel = AsyncMock()

        await publish_handler.safe_publish(queue_name, message)

        publish_handler.rabbitmq_util.reopen_channel.assert_awaited_once()
        assert publish_handler.rabbitmq_util.publish_message.await_count == 2

    @pytest.mark.asyncio
    async def test_safe_publish_general_exception(self, publish_handler):
        queue_name = 'test_queue'
        message = {'key': 'value'}

        publish_handler.rabbitmq_util.publish_message = AsyncMock(side_effect=Exception('Test exception'))

        await publish_handler.safe_publish(queue_name, message)

        publish_handler.rabbitmq_util.publish_message.assert_awaited_once_with(queue_name, message)
        publish_handler.logger.error.assert_called_with('Failed to publish message: Test exception')

    @pytest.mark.asyncio
    async def test_publish_ai_analysis_message_success(self, publish_handler):
        queue_name = 'test_queue'
        message = {'key': 'value'}

        publish_handler.rabbitmq_util.declare_queue = AsyncMock()
        publish_handler.safe_publish = AsyncMock()

        await publish_handler.publish_ai_analysis_message(queue_name, message)

        publish_handler.rabbitmq_util.declare_queue.assert_awaited_once_with(queue_name)
        publish_handler.safe_publish.assert_awaited_once_with(queue_name, message)

    @pytest.mark.asyncio
    async def test_publish_ai_analysis_message_exception(self, publish_handler):
        queue_name = 'test_queue'
        message = {'key': 'value'}

        publish_handler.rabbitmq_util.declare_queue = AsyncMock(side_effect=Exception('Test exception'))

        with pytest.raises(Exception, match='Test exception'):
            await publish_handler.publish_ai_analysis_message(queue_name, message)

        publish_handler.logger.error.assert_called_with('Error publishing message: Test exception')


if __name__ == '__main__':
    pytest.main()  # pragma: no cover
