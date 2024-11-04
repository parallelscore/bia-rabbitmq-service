import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from aio_pika import IncomingMessage
from aio_pika.exceptions import ConnectionClosed
from app.listeners.queue_message_forwarder import QueueMessageForwarder
from app.core.config import settings


@pytest.fixture
def queue_message_forwarder():
    return QueueMessageForwarder()


@pytest.fixture
def incoming_message():
    message = MagicMock(spec=IncomingMessage)
    message.body = json.dumps({"data":{"key": "value"}}).encode('utf-8')
    return message


@pytest.fixture
def rabbitmq_queues():
    return {
        'listen_queues': {
            'test_queue': [
                {'method': 'queue', 'address': 'POST http://example.com'},
                {'method': 'endpoint', 'address': 'POST http://example.com'}
            ]
        }
    }


# Test consume_and_forward_callback when forwarding to a queue
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.rabbitmq_util')
@patch('app.listeners.queue_message_forwarder.publisher.publish_message')
async def test_consume_and_forward_callback_forward_to_queue(mock_publish_message, mock_rabbitmq, queue_message_forwarder, incoming_message, rabbitmq_queues):
    queue_message_forwarder.queue_name = 'test_queue'
    
    with patch('app.listeners.queue_message_forwarder.RABBITMQ_QUEUES', rabbitmq_queues):
        await queue_message_forwarder.consume_and_forward_callback(incoming_message)
    
    incoming_message.ack.assert_called_once()


# Test consume_and_forward_callback when forwarding to an endpoint
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.aiohttp.ClientSession')
@patch('app.listeners.queue_message_forwarder.rabbitmq_util')
async def test_consume_and_forward_callback_forward_to_endpoint(mock_rabbitmq, mock_aiohttp, queue_message_forwarder, incoming_message, rabbitmq_queues):
    queue_message_forwarder.queue_name = 'test_queue'

    mock_session = mock_aiohttp.return_value.__aenter__.return_value
    mock_session.post.return_value.__aenter__.return_value.status = 200
    
    with patch('app.listeners.queue_message_forwarder.RABBITMQ_QUEUES', rabbitmq_queues):
        await queue_message_forwarder.consume_and_forward_callback(incoming_message)

    incoming_message.ack.assert_called_once()
    mock_session.post.assert_called_once()


# Test consume_and_forward_callback failure and forwarding to error queue
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.rabbitmq_util')
@patch('app.listeners.queue_message_forwarder.publisher.publish_message')
async def test_consume_and_forward_callback_forward_to_error_queue(mock_publish_message, mock_rabbitmq, queue_message_forwarder, incoming_message, rabbitmq_queues):
    queue_message_forwarder.queue_name = 'test_queue'

    # Simulate an error during message forwarding
    with patch('app.listeners.queue_message_forwarder.RABBITMQ_QUEUES', rabbitmq_queues), \
         patch.object(queue_message_forwarder, 'forward_to_endpoint', side_effect=Exception("Test error")):

        await queue_message_forwarder.consume_and_forward_callback(incoming_message)

    # Ensure message is nacked and published to error queue
    incoming_message.nack.assert_called_once_with(requeue=False)
    mock_publish_message.assert_called_once()


@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.ensure_connection', new_callable=AsyncMock)
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.consume_message', new_callable=AsyncMock)
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.setup_connection', new_callable=AsyncMock)
async def test_consume_and_forward_success(mock_setup_connection, mock_consume_message, mock_ensure_connection, queue_message_forwarder):
    # Successful case: Message consumption runs without any exceptions
    queue_name = 'test_queue'

    await queue_message_forwarder.consume_and_forward(queue_name)

    # Ensure the connection was established and message consumption started
    mock_ensure_connection.assert_called_once()
    mock_consume_message.assert_called_once_with(queue_name=queue_name, callback=queue_message_forwarder.consume_and_forward_callback)
    mock_setup_connection.assert_not_called()  # No reconnection should happen in the success case


@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.ensure_connection', new_callable=AsyncMock)
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.consume_message', new_callable=AsyncMock)
async def test_consume_and_forward_keyboard_interrupt(mock_consume_message, mock_ensure_connection, queue_message_forwarder):
    # Simulate KeyboardInterrupt during message consumption
    queue_name = 'test_queue'

    mock_consume_message.side_effect = KeyboardInterrupt

    await queue_message_forwarder.consume_and_forward(queue_name)

    # Ensure the connection was established
    mock_ensure_connection.assert_called_once()
    # Ensure the consume_message was called once but interrupted
    mock_consume_message.assert_called_once_with(queue_name=queue_name, callback=queue_message_forwarder.consume_and_forward_callback)


@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.ensure_connection', new_callable=AsyncMock)
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.consume_message', new_callable=AsyncMock)
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.setup_connection', new_callable=AsyncMock)
async def test_consume_and_forward_connection_error(mock_setup_connection, mock_consume_message, mock_ensure_connection, queue_message_forwarder):
    # Simulate a RabbitMQ connection error (ConnectionClosed or ChannelClosed)
    queue_name = 'test_queue'

    mock_consume_message.side_effect = ConnectionClosed(0, "Connection was closed")  # Simulate connection closed exception

    await queue_message_forwarder.consume_and_forward(queue_name)

    # Ensure the connection was established, and then it tries to reconnect
    mock_ensure_connection.assert_called_once()
    mock_consume_message.assert_called_once_with(queue_name=queue_name, callback=queue_message_forwarder.consume_and_forward_callback)
    mock_setup_connection.assert_called_once()  # Reconnection should happen after connection error


@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.ensure_connection', new_callable=AsyncMock)
@patch('app.listeners.queue_message_forwarder.rabbitmq_util.consume_message', new_callable=AsyncMock)
async def test_consume_and_forward_generic_exception(mock_consume_message, mock_ensure_connection, queue_message_forwarder):
    # Simulate an unexpected exception during message consumption
    queue_name = 'test_queue'

    mock_consume_message.side_effect = Exception('Unexpected Error')

    await queue_message_forwarder.consume_and_forward(queue_name)

    # Ensure the connection was established
    mock_ensure_connection.assert_called_once()
    # Ensure the consume_message was called but failed with an unexpected exception
    mock_consume_message.assert_called_once_with(queue_name=queue_name, callback=queue_message_forwarder.consume_and_forward_callback)


# Test forward_to_queue
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.publisher.publish_message')
async def test_forward_to_queue(mock_publish_message, queue_message_forwarder):
    await queue_message_forwarder.forward_to_queue('test_queue', {"key": "value"})

    mock_publish_message.assert_called_once_with('test_queue', {"key": "value"})


# Test forward_to_endpoint success
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.aiohttp.ClientSession')
async def test_forward_to_endpoint_success(mock_aiohttp, queue_message_forwarder):
    # Create the mock session object
    mock_session = mock_aiohttp.return_value.__aenter__.return_value
    
    # Mock the response object and set the status
    mock_response = AsyncMock()
    mock_response.status = 200

    # Make the post method return the mock response
    mock_session.post.return_value = mock_response

    endpoint_info = {'method': 'endpoint', 'address': 'POST http://example.com'}
    await queue_message_forwarder.forward_to_endpoint(endpoint_info, {"key": "value"})

    mock_session.post.assert_called_once()


# Test forward_to_endpoint failure
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.aiohttp.ClientSession')
async def test_forward_to_endpoint_failure(mock_aiohttp, queue_message_forwarder):
    mock_session = mock_aiohttp.return_value.__aenter__.return_value
    
    # Mock the response object and set the status
    mock_response = AsyncMock()
    mock_response.status = 400

    # Make the post method return the mock response
    mock_session.post.return_value = mock_response

    endpoint_info = {'method': 'POST', 'address': 'POST http://example.com'}

    with pytest.raises(Exception, match='Got response 400 from endpoint'):
        await queue_message_forwarder.forward_to_endpoint(endpoint_info, {"key": "value"})

    mock_session.post.assert_called_once()


# Test extract_url for string address
@pytest.mark.asyncio
async def test_extract_url_string(queue_message_forwarder):
    method, url = await queue_message_forwarder.extract_url('POST http://example.com', {"key": "value"})
    assert method == 'POST'
    assert url == 'http://example.com'


# Test extract_url for conditional address
@pytest.mark.asyncio
async def test_extract_url_conditional(queue_message_forwarder):
    address = [{'condition': 'key == value', 'address': 'POST http://example.com'}]
    method, url = await queue_message_forwarder.extract_url(address, {"key": "value"})
    assert method == 'POST'
    assert url == 'http://example.com'


# Test forward_to_error_queue_and_nack
@pytest.mark.asyncio
@patch('app.listeners.queue_message_forwarder.publisher.publish_message')
async def test_forward_to_error_queue_and_nack(mock_publish_message, queue_message_forwarder, incoming_message):
    err = Exception("Test error")
    
    await queue_message_forwarder.forward_to_error_queue_and_nack(incoming_message, err)

    incoming_message.nack.assert_called_once_with(requeue=False)
    
    # decoded_msg = json.loads(incoming_message.body)
    # mock_publish_message.assert_called_once_with(settings.ERROR_QUEUE, {
    #     'payload': decoded_msg,
    #     'error': 'Test error'
    # })
