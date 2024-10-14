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

#     @pytest.mark.asyncio
#     async def test_close_connection(self, rabbitmq_util):
#         async for util in rabbitmq_util:
#             await util.close_connection()

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
