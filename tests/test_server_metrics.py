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


