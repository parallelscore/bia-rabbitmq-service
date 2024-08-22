from fastapi import APIRouter, status
from fastapi.exceptions import HTTPException

from app.utils.logging_util import setup_logger
from app.services.publisher_service import publisher
from app.schemas.publisher_schema import PublisherSchema


class PublisherRouter:

    def __init__(self):

        self.logger = setup_logger(__name__)
        self.router = APIRouter()
        self.router.add_api_route('/publisher', self.publisher, methods=['POST'], tags=['publishers'],
                                  status_code=status.HTTP_201_CREATED)

    async def publisher(self, data: PublisherSchema):

        try:
            await publisher.publish_ai_analysis_message(data.queue_name, data.message)

        except Exception as e:
            self.logger.error(f'Error while trying to publish message to queue {data.queue_name}: {e}')
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f'Error while trying to publish message to queue {data.queue_name}: {e}')
