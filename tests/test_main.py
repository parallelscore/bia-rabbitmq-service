import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import FastAPI
from app.main import create_app, start_consumers


@pytest.fixture
def mock_settings():
    """Fixture for mocking the settings object."""
    with patch("app.main.settings") as mock_settings:
        mock_settings.PROJECT_NAME = "Test Project"
        mock_settings.DESCRIPTION = "Test Description"
        mock_settings.VERSION = "1.0.0"
        mock_settings.API_V1_STR = "/api/v1"
        yield mock_settings


@pytest.fixture
def mock_register_middlewares():
    """Fixture for mocking the register_middlewares function."""
    with patch("app.main.register_middlewares") as mock_register:
        yield mock_register


@pytest.fixture
def mock_publisher_router():
    """Fixture for mocking PublisherRouter."""
    with patch("app.main.PublisherRouter") as mock_publisher_router:
        mock_router_instance = MagicMock()
        mock_router_instance.router = MagicMock()
        mock_publisher_router.return_value = mock_router_instance
        yield mock_publisher_router


@pytest.fixture
def mock_server_metrics():
    """Fixture for mocking ServerMetrics."""
    with patch("app.main.ServerMetrics") as mock_server_metrics:
        mock_router_instance = MagicMock()
        mock_router_instance.router = MagicMock()
        mock_server_metrics.return_value = mock_router_instance
        yield mock_server_metrics


@pytest.fixture
def mock_rabbitmq_queues():
    """Fixture for mocking RABBITMQ_QUEUES constant."""
    with patch("app.main.RABBITMQ_QUEUES", {'listen_queues': ['test_queue']}):
        yield


@pytest.fixture
def mock_queue_message_forwarder():
    """Fixture for mocking queue_message_forwarder."""
    with patch("app.main.queue_message_forwarder") as mock_forwarder:
        mock_forwarder.consume_and_forward = AsyncMock()
        yield mock_forwarder


@pytest.fixture
def mock_time():
    """Fixture for mocking time.time to control app state start time."""
    with patch("app.main.time.time", return_value=100000):
        yield


@pytest.fixture
def mock_uvicorn_run():
    """Fixture for mocking uvicorn.run to prevent server start."""
    with patch("app.main.uvicorn.run") as mock_run:
        yield mock_run


@pytest.fixture
def mock_asyncio_loop():
    """Fixture for mocking asyncio event loop."""
    with patch("app.main.asyncio.get_event_loop") as mock_get_loop:
        mock_loop = AsyncMock()
        mock_get_loop.return_value = mock_loop
        yield mock_loop


def test_create_app(
    mock_settings,
    mock_register_middlewares,
    mock_publisher_router,
    mock_server_metrics,
    mock_time,
):
    """Test the creation of the FastAPI app and verify components are correctly initialized."""
    
    # Call create_app
    app = create_app()
    
    assert isinstance(app, FastAPI)
    assert app.title == "Test Project"
    assert app.description == "Test Description"
    assert app.version == "1.0.0"
    assert app.state.start_time == 100000
    assert app.state.requests_processed == 0
    
    # Check middleware registration
    mock_register_middlewares.assert_called_once_with(app)
    
    # Check router registration
    mock_publisher_router.assert_called_once()
    mock_server_metrics.assert_called_once_with(app)
    assert len(app.routes) > 0  # Verifying routes are added
