# import pytest
# from aio_pika.exceptions import AMQPError
# from unittest.mock import MagicMock, patch

# from app.utils.rabbitmq_util import RabbitMQUtil


# class TestRabbitmqUtil:

#     @pytest.fixture
#     async def rabbitmq_util(self):
#         util = RabbitMQUtil()
#         await util.setup_connection()
#         yield util
#         await util.close_connection()

#     @pytest.mark.asyncio
#     async def test_declare_queue(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             queue_name = 'test_queue'
#             queue = await util.declare_queue(queue_name)
#             assert queue.name == queue_name

#     @pytest.mark.asyncio
#     async def test_publish_message(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             queue_name = 'test_queue'
#             message = {'key': 'value'}
#             await util.publish_message(queue_name, message)

#     @pytest.mark.asyncio
#     async def test_consume_message(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             queue_name = 'test_queue'

#             # Create an async mock function
#             async def async_callback(message):
#                 return MagicMock()

#             callback = MagicMock(side_effect=async_callback)

#             await util.consume_message(queue_name, callback)

#     # @pytest.mark.asyncio
#     # async def test_close_connection(self, rabbitmq_util):
#     #     async for util in rabbitmq_util:
#     #         await util.close_connection()

#     @pytest.mark.asyncio
#     async def test_ensure_connection(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             await util.ensure_connection()

#     @pytest.mark.asyncio
#     async def test_initialize(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             await util.initialize()

#     @pytest.mark.asyncio
#     async def test_publish_message_with_error(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             queue_name = 'test_queue'
#             message = {'key': 'value'}

#             # Mock the publish_message method to raise AMQPError for this test
#             with patch.object(util, 'publish_message', side_effect=AMQPError('Permission denied')):
#                 with pytest.raises(AMQPError) as exc_info:
#                     await util.publish_message(queue_name, message)

#                 assert 'Permission denied' in str(exc_info.value)


# if __name__ == '__main__':
#     pytest.main()  # pragma: no cover


# import pytest
# import asyncio
# from unittest import mock
# import aio_pika
# from aio_pika.exceptions import AMQPError
# from app.utils.rabbitmq_util import RabbitMQUtil, rabbitmq_util


# @pytest.mark.asyncio
# async def test_setup_connection_success(mocker):

#     mock_connection = mock.AsyncMock()
#     mocker.patch('aio_pika.connect_robust', return_value=mock_connection)

#     # mock_connect_robust = mock.Mock()
#     # aio_pika.connect_robust = mock_connect_robust
#     rabbitmq_util = RabbitMQUtil()
#     await rabbitmq_util.setup_connection()

#     aio_pika.connect_robust.assert_called_once()
    # mock_connection.channel.assert_called_once()
    
    # mock_connect_robust.assert_called_once()

    # function_to_test.send_email("test@example.com", "Hello")
    # mock_email_service.send.assert_called_with(email="test@example.com", message="Hello")
    # Create async mock for connection and channel
    # mock_connection = mock.MagicMock()
    # mock_channel = mock.MagicMock()

    # # Set async methods on the connection and channel
    # mock_connection.channel = mock.AsyncMock(return_value=mock_channel)
    # mock_channel.set_qos = mock.AsyncMock()

    # # Patch 'connect_robust' to return the mock connection
    # with mock.patch('aio_pika.connect_robust', new=mock.AsyncMock(return_value=mock_connection)):
    #     rabbitmq_util = RabbitMQUtil()

    #     # Call the method under test
    #     await rabbitmq_util.setup_connection()

    #     # Ensure the connection was established and a channel was created
    #     mock_connection.channel.assert_called_once()
    #     mock_channel.set_qos.assert_called_once_with(prefetch_count=1)

    #     # Check that the connection and channel are set properly in the instance
    #     assert rabbitmq_util.connection == mock_connection
    #     assert rabbitmq_util.channel == mock_channel



# Helper decorator for async tests
# @pytest.mark.asyncio
# async def test_setup_connection_success():
#     # Mock connection and channel
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel

#         await rabbitmq_util.setup_connection()

#         mock_connection.channel.assert_called_once()
#         mock_channel.set_qos.assert_called_once_with(prefetch_count=1)


# @pytest.mark.asyncio
# async def test_setup_connection_failure():
#     # Mock failure on connect
#     with mock.patch('aio_pika.connect_robust', side_effect=AMQPError('Connection error')):
#         rabbitmq_util = RabbitMQUtil()

#         with pytest.raises(AMQPError):
#             await rabbitmq_util.setup_connection(retries=3, delay=0.1)


# @pytest.mark.asyncio
# async def test_declare_queue_success():
#     # Mock queue declaration
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()
#     mock_queue = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel
#         mock_channel.declare_queue.return_value = mock_queue

#         await rabbitmq_util.setup_connection()
#         result = await rabbitmq_util.declare_queue('test_queue')

#         mock_channel.declare_queue.assert_called_once_with('test_queue', durable=True)
#         assert result == mock_queue


# @pytest.mark.asyncio
# async def test_declare_queue_failure():
#     # Mock failure in queue declaration
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel
#         mock_channel.declare_queue.side_effect = AMQPError('Declare error')

#         await rabbitmq_util.setup_connection()

#         with pytest.raises(AMQPError):
#             await rabbitmq_util.declare_queue('test_queue')


# @pytest.mark.asyncio
# async def test_publish_message_success():
#     # Mock successful message publishing
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel

#         await rabbitmq_util.setup_connection()
#         await rabbitmq_util.publish_message('test_queue', {'key': 'value'})

#         mock_channel.default_exchange.publish.assert_called_once()


# @pytest.mark.asyncio
# async def test_publish_message_failure():
#     # Mock failure in message publishing
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel
#         mock_channel.default_exchange.publish.side_effect = AMQPError('Publish error')

#         await rabbitmq_util.setup_connection()

#         with pytest.raises(AMQPError):
#             await rabbitmq_util.publish_message('test_queue', {'key': 'value'})


# @pytest.mark.asyncio
# async def test_consume_message_success():
#     # Mock successful message consumption
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()
#     mock_queue = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel
#         mock_channel.declare_queue.return_value = mock_queue

#         callback = mock.AsyncMock()

#         await rabbitmq_util.setup_connection()
#         await rabbitmq_util.consume_message('test_queue', callback)

#         mock_queue.consume.assert_called_once_with(callback)


# @pytest.mark.asyncio
# async def test_consume_message_failure():
#     # Mock failure in message consumption
#     mock_connection = mock.AsyncMock()
#     mock_channel = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         mock_connection.channel.return_value = mock_channel
#         mock_channel.declare_queue.side_effect = AMQPError('Consume error')

#         await rabbitmq_util.setup_connection()

#         with pytest.raises(AMQPError):
#             await rabbitmq_util.consume_message('test_queue', mock.AsyncMock())


# @pytest.mark.asyncio
# async def test_close_connection():
#     # Mock successful connection close
#     mock_connection = mock.AsyncMock()
#     mock_connection.is_closed = False

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         rabbitmq_util.connection = mock_connection

#         await rabbitmq_util.close_connection()

#         mock_connection.close.assert_called_once()


# @pytest.mark.asyncio
# async def test_ensure_connection_reconnect():
#     # Mock connection lost and reconnection
#     mock_connection = mock.AsyncMock()
#     mock_connection.is_closed = True  # Simulate closed connection
#     mock_channel = mock.AsyncMock()

#     with mock.patch('aio_pika.connect_robust', return_value=mock_connection):
#         rabbitmq_util = RabbitMQUtil()
#         rabbitmq_util.connection = mock_connection
#         mock_connection.channel.return_value = mock_channel

#         await rabbitmq_util.ensure_connection()

#         mock_connection.close.assert_called_once()
#         mock_connection.channel.assert_called_once()
