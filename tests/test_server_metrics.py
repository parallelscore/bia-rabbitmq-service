import time
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.api.routes.server_metrics import ServerMetrics  # Adjust based on your file structure


# Set up a mock FastAPI app and ServerMetrics instance for testing
app = FastAPI()
app.state.start_time = time.time() - 10000  # Mock server uptime of 10,000 seconds
app.state.requests_processed = 120  # Mock requests processed count
metrics_controller = ServerMetrics(app)
app.include_router(metrics_controller.router)

client = TestClient(app)


def test_server_metrics_response_code():
    response = client.get('/')
    assert response.status_code == 200


# @patch('app.api.routes.server_metrics.psutil.virtual_memory')
# @patch('app.api.routes.server_metrics.psutil.swap_memory')
# @patch('app.api.routes.server_metrics.psutil.cpu_percent')
# @patch('app.api.routes.server_metrics.psutil.disk_usage')
# @patch('app.api.routes.server_metrics.psutil.disk_io_counters')
# @patch('app.api.routes.server_metrics.psutil.net_io_counters')
# @patch('app.api.routes.server_metrics.psutil.getloadavg')
# @patch('app.api.routes.server_metrics.psutil.pids')
# def test_server_metrics_html_content(
#     mock_pids, mock_loadavg, mock_net_io, mock_disk_io, mock_disk_usage,
#     mock_cpu_percent, mock_swap_memory, mock_virtual_memory
# ):
#     # Setup mock return values
#     mock_virtual_memory.return_value = type('virtual_memory', (object,), {
#         'total': 16 * 1024 ** 3, 'available': 12 * 1024 ** 3, 'percent': 25,
#         'free': 8 * 1024 ** 3
#     })()
#     mock_swap_memory.return_value = type('swap_memory', (), {
#         'total': 2 * 1024 ** 3, 'used': 0.5 * 1024 ** 3, 'free': 1.5 * 1024 ** 3,
#         'percent': 25
#     })
#     mock_cpu_percent.return_value = 30
#     # mock_cpu_percent.return_value = [30, 45, 60, 75, 85]
#     mock_disk_usage.return_value = type('disk_usage', (), {
#         'total': 512 * 1024 ** 3, 'used': 128 * 1024 ** 3, 'free': 384 * 1024 ** 3,
#         'percent': 25
#     })
#     mock_disk_io.return_value = type('disk_io', (), {
#         'read_bytes': 1024 ** 2 * 100, 'write_bytes': 1024 ** 2 * 50
#     })
#     mock_net_io.return_value = type('net_io', (), {
#         'bytes_sent': 1024 ** 2 * 200, 'bytes_recv': 1024 ** 2 * 300
#     })
#     mock_loadavg.return_value = (1.5, 1.0, 0.5)
#     mock_pids.return_value = list(range(100))  # Simulating 100 active processes

#     response = client.get('/')
#     assert response.status_code == 200
#     html_content = response.text

#     # Check that all key metrics appear in the response
#     assert "Server Uptime:" in html_content
#     assert "Requests Processed:" in html_content
#     assert "Memory Info:" in html_content
#     assert "Swap Memory Info:" in html_content
#     assert "CPU Usage:" in html_content
#     assert "Disk Usage:" in html_content
#     assert "Disk I/O:" in html_content
#     assert "Network I/O:" in html_content
#     assert "Load Average:" in html_content
#     assert "Number of Active Processes:" in html_content


# @patch('app.api.routes.server_metrics.psutil.virtual_memory')
# @patch('app.api.routes.server_metrics.psutil.swap_memory')
# @patch('app.api.routes.server_metrics.psutil.cpu_percent')
# @patch('app.api.routes.server_metrics.psutil.disk_usage')
# def test_memory_cpu_swap_color_thresholds(
#     mock_disk_usage, mock_cpu_percent, mock_swap_memory, mock_virtual_memory
# ):
#     # Test cases for memory usage color
#     mock_virtual_memory.return_value = type('virtual_memory', (), {
#         'percent': 45
#     })
#     response = client.get('/')
#     assert 'style="color:green"' in response.text

#     mock_virtual_memory.return_value.percent = 65
#     response = client.get('/')
#     assert 'style="color:orange"' in response.text

#     mock_virtual_memory.return_value.percent = 85
#     response = client.get('/')
#     assert 'style="color:red"' in response.text

#     # Test cases for swap usage color
#     mock_swap_memory.return_value = type('swap_memory', (), {
#         'percent': 45
#     })
#     response = client.get('/')
#     assert 'style="color:green"' in response.text

#     mock_swap_memory.return_value.percent = 65
#     response = client.get('/')
#     assert 'style="color:orange"' in response.text

#     mock_swap_memory.return_value.percent = 85
#     response = client.get('/')
#     assert 'style="color:red"' in response.text

#     # Test cases for CPU usage color
#     mock_cpu_percent.return_value = 45
#     response = client.get('/')
#     assert 'style="color:green"' in response.text

#     mock_cpu_percent.return_value = 65
#     response = client.get('/')
#     assert 'style="color:orange"' in response.text

#     mock_cpu_percent.return_value = 85
#     response = client.get('/')
#     assert 'style="color:red"' in response.text

#     # Test cases for disk usage color
#     mock_disk_usage.return_value = type('disk_usage', (), {
#         'percent': 45
#     })
#     response = client.get('/')
#     assert 'style="color:green"' in response.text

#     mock_disk_usage.return_value.percent = 65
#     response = client.get('/')
#     assert 'style="color:orange"' in response.text

#     mock_disk_usage.return_value.percent = 85
#     response = client.get('/')
#     assert 'style="color:red"' in response.text


# import time
# from unittest.mock import patch, MagicMock
# from fastapi import FastAPI
# from fastapi.testclient import TestClient
# from app.api.routes.server_metrics import ServerMetrics

# class TestServerMetrics:

#     @classmethod
#     def setup_class(cls):
#         cls.app = FastAPI()
#         cls.app.state.start_time = 0  # Mock start time
#         cls.app.state.requests_processed = 10  # Example mock request count
#         cls.server_metrics = ServerMetrics(cls.app)
#         cls.client = TestClient(cls.server_metrics.router)

#     @patch('time.gmtime')
#     @patch('time.time', return_value=1000)
#     def test_uptime_calculation(self, mock_time, mock_gmtime):
#         """Test the uptime calculation based on current and start time."""
#         mock_gmtime.return_value = time.gmtime(1000)
#         response = self.client.get('/')
#         assert "00:16:40" in response.text  # Expecting uptime as 00:16:40

#     @patch('psutil.virtual_memory')
#     def test_memory_metrics(self, mock_virtual_memory):
#         """Test memory metrics to ensure correct display and calculation."""
#         mock_virtual_memory.return_value = MagicMock(
#             total=8 * 1024**3,  # 8 GB in bytes
#             available=4 * 1024**3,  # 4 GB in bytes
#             free=2 * 1024**3,  # 2 GB in bytes
#             percent=50  # 50% used
#         )
#         response = self.client.get('/')
#         assert "8192.00 MB" in response.text
#         assert "4096.00 MB" in response.text
#         assert "2048.00 MB" in response.text
#         assert '50.00%' in response.text
#         assert 'orange' in response.text  # Since 50% is in the orange range

#     @patch('psutil.swap_memory')
#     def test_swap_memory_metrics(self, mock_swap_memory):
#         """Test swap memory metrics for correctness."""
#         mock_swap_memory.return_value = MagicMock(
#             total=2 * 1024**3,  # 2 GB in bytes
#             used=1 * 1024**3,  # 1 GB in bytes
#             free=1 * 1024**3,  # 1 GB in bytes
#             percent=50
#         )
#         response = self.client.get('/')
#         assert "2048.00 MB" in response.text
#         assert "1024.00 MB" in response.text
#         assert '50.00%' in response.text
#         assert 'orange' in response.text

#     @patch('psutil.cpu_percent', return_value=75)
#     def test_cpu_usage_metrics(self, mock_cpu_percent):
#         """Test CPU usage metrics and color coding."""
#         response = self.client.get('/')
#         assert '75.00%' in response.text
#         assert 'orange' in response.text  # Since 75% is in the orange range

#     @patch('psutil.disk_usage')
#     def test_disk_usage_metrics(self, mock_disk_usage):
#         """Test disk usage metrics and correct formatting."""
#         mock_disk_usage.return_value = MagicMock(
#             total=100 * 1024**3,  # 100 GB in bytes
#             used=50 * 1024**3,  # 50 GB in bytes
#             free=50 * 1024**3,  # 50 GB in bytes
#             percent=50
#         )
#         response = self.client.get('/')
#         assert "100.00 GB" in response.text
#         assert "50.00 GB" in response.text
#         assert '50.00%' in response.text
#         assert 'orange' in response.text  # Since 50% is in the orange range

#     @patch('psutil.disk_io_counters')
#     def test_disk_io_metrics(self, mock_disk_io_counters):
#         """Test disk I/O metrics for expected values."""
#         mock_disk_io_counters.return_value = MagicMock(
#             read_bytes=500 * 1024**2,  # 500 MB in bytes
#             write_bytes=300 * 1024**2  # 300 MB in bytes
#         )
#         response = self.client.get('/')
#         assert '500.00 MB' in response.text
#         assert '300.00 MB' in response.text

#     @patch('psutil.net_io_counters')
#     def test_network_io_metrics(self, mock_net_io_counters):
#         """Test network I/O metrics for proper format and values."""
#         mock_net_io_counters.return_value = MagicMock(
#             bytes_sent=200 * 1024**2,  # 200 MB in bytes
#             bytes_recv=100 * 1024**2  # 100 MB in bytes
#         )
#         response = self.client.get('/')
#         assert '200.00 MB' in response.text
#         assert '100.00 MB' in response.text

#     @patch('psutil.getloadavg', return_value=(1.5, 0.75, 0.5))
#     def test_load_average_metrics(self, mock_getloadavg):
#         """Test load average metrics for expected values."""
#         response = self.client.get('/')
#         assert '1.50' in response.text
#         assert '0.75' in response.text
#         assert '0.50' in response.text

#     @patch('psutil.pids', return_value=[1, 2, 3, 4])
#     def test_num_processes_metric(self, mock_pids):
#         """Test number of active processes display."""
#         response = self.client.get('/')
#         assert '4' in response.text  # 4 active processes

#     def test_html_structure(self):
#         """Ensure the HTML structure is correct and contains essential tags."""
#         response = self.client.get('/')
#         assert response.status_code == 200
#         assert "<html>" in response.text
#         assert "<head>" in response.text
#         assert "<body>" in response.text
#         assert "Server Metrics" in response.text  # Check title presence
