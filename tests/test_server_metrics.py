import time
from unittest.mock import patch, MagicMock
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api.routes.server_metrics import ServerMetrics  # adjust this import as needed


@pytest.fixture
def app():
    """Fixture for creating a FastAPI app instance with mock state attributes."""
    app = FastAPI()
    app.state.start_time = 100000  # Mock start time for uptime calculation
    app.state.requests_processed = 42  # Mock requests processed count
    return app


@pytest.fixture
def client(app):
    """Fixture for creating a TestClient instance for the FastAPI app."""
    metrics = ServerMetrics(app)
    app.include_router(metrics.router)
    return TestClient(app)


@patch("app.api.routes.server_metrics.time")
@patch("app.api.routes.server_metrics.psutil")
def test_server_metrics_endpoint(mock_psutil, mock_time, client):
    """Test the server metrics endpoint by mocking all dependencies and values."""
    
    # Mock time functions
    mock_time.strftime.return_value = "2023-01-01 12:00:00"
    mock_time.time.return_value = 200000  # Current time for uptime calculation

    # Mock memory info
    mock_virtual_memory = MagicMock()
    mock_virtual_memory.total = 16 * 1024**3  # 16 GB
    mock_virtual_memory.available = 4 * 1024**3  # 12 GB available
    mock_virtual_memory.free = 4 * 1024**3  # 12 GB free
    mock_virtual_memory.percent = 75.0
    mock_psutil.virtual_memory.return_value = mock_virtual_memory

    # Mock swap memory info
    mock_swap_memory = MagicMock()
    mock_swap_memory.total = 4 * 1024**3  # 4 GB
    mock_swap_memory.used = 1 * 1024**3  # 1 GB used
    mock_swap_memory.free = 3 * 1024**3  # 3 GB free
    mock_swap_memory.percent = 25.0
    mock_psutil.swap_memory.return_value = mock_swap_memory

    # Mock CPU usage
    mock_psutil.cpu_percent.return_value = 30.0
    mock_psutil.cpu_percent.side_effect = [30.0, [30.0, 40.0, 20.0, 10.0]]

    # Mock disk usage
    mock_disk_usage = MagicMock()
    mock_disk_usage.total = 500 * 1024**3  # 500 GB
    mock_disk_usage.used = 200 * 1024**3  # 200 GB used
    mock_disk_usage.free = 300 * 1024**3  # 300 GB free
    mock_disk_usage.percent = 40.0
    mock_psutil.disk_usage.return_value = mock_disk_usage

    # Mock disk I/O counters
    mock_disk_io_counters = MagicMock()
    mock_disk_io_counters.read_bytes = 1024**3  # 1 GB read
    mock_disk_io_counters.write_bytes = 2 * 1024**3  # 2 GB written
    mock_psutil.disk_io_counters.return_value = mock_disk_io_counters

    # Mock network I/O counters
    mock_net_io_counters = MagicMock()
    mock_net_io_counters.bytes_sent = 512 * 1024**2  # 512 MB sent
    mock_net_io_counters.bytes_recv = 1024 * 1024**2  # 1 GB received
    mock_psutil.net_io_counters.return_value = mock_net_io_counters

    # Mock load average
    mock_psutil.getloadavg.return_value = (0.5, 0.7, 0.9)

    # Mock number of active processes
    mock_psutil.pids.return_value = list(range(100))  # 100 active processes

    # Send request to the server metrics endpoint
    response = client.get("/")

    # Assertions
    assert response.status_code == 200
    content = response.content.decode("utf-8")

    # Check time and uptime
    assert "Current Server Time:</strong> <span>2023-01-01 12:00:00" in content
    assert "Server Uptime:</strong> <span>27:46:40" in content  # Mock uptime calculation from start_time and current time

    # Check requests processed
    assert "Requests Processed:</strong> <span>42" in content

    # Check memory information
    assert "Total: <span>16384.00 MB" in content
    assert "Available: <span>4096.00 MB" in content
    assert 'Percent Used: <span style="color:red">75.00%</span>' in content
    assert "Used: <span>12288.00 MB" in content
    assert "Free: <span>4096.00 MB" in content

    # Check swap memory information
    assert "Total: <span>4096.00 MB" in content
    assert "Used: <span>1024.00 MB" in content
    assert "Free: <span>3072.00 MB" in content
    assert 'Percent Used: <span style="color:green">25.00%</span>' in content

    # Check CPU usage
    assert 'CPU Usage:</strong> <span style="color:green">30.00%</span>' in content
    assert 'Core 1: <span style="color:green">30.00%</span>' in content
    assert 'Core 2: <span style="color:green">40.00%</span>' in content
    assert 'Core 3: <span style="color:green">20.00%</span>' in content
    assert 'Core 4: <span style="color:green">10.00%</span>' in content

    # Check disk usage
    assert "Total: <span>500.00 GB" in content
    assert "Used: <span>200.00 GB" in content
    assert "Free: <span>300.00 GB" in content
    assert 'Percent Used: <span style="color:green">40.00%</span>' in content

    # Check disk I/O
    assert "Read: <span>1024.00 MB" in content
    assert "Write: <span>2048.00 MB" in content

    # Check network I/O
    assert "Bytes Sent: <span>512.00 MB" in content
    assert "Bytes Received: <span>1024.00 MB" in content

    # Check load average
    assert "1 min: <span>0.50" in content
    assert "5 min: <span>0.70" in content
    assert "15 min: <span>0.90" in content

    # Check number of active processes
    assert "Number of Active Processes:</strong> <span>100" in content
