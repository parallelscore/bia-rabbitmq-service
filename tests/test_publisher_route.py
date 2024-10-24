import pytest
import asyncio
from fastapi import FastAPI, status
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

from app.api.routes.publisher import PublisherRouter
from app.schemas.publisher_schema import PublisherSchema

app = FastAPI()
publish_router = PublisherRouter()
app.include_router(publish_router.router)

client = TestClient(app)


class TestPublishRouter:
    @pytest.fixture
    def publish_data(self):
        return PublisherSchema(queue_name='test_queue', message={'key': 'value'})

    @pytest.mark.asyncio
    @patch('app.services.publisher_service.publisher.publish_message', new_callable=AsyncMock)
    async def test_publish_success(self, mock_publish, publish_data):
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: client.post('/publisher', json=publish_data.model_dump()))
        assert response.status_code == status.HTTP_201_CREATED
        mock_publish.assert_awaited_once_with(publish_data.queue_name, publish_data.message)

    @pytest.mark.asyncio
    @patch('app.services.publisher_service.publisher.publish_message', new_callable=AsyncMock)
    async def test_publish_failure(self, mock_publish, publish_data):
        mock_publish.side_effect = Exception('Publish failed')
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, lambda: client.post('/publisher', json=publish_data.model_dump()))
        assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert response.json() == {
            'detail': f'Error while trying to publish message to queue {publish_data.queue_name}: Publish failed'
        }


if __name__ == '__main__':
    pytest.main()  # pragma: no cover
