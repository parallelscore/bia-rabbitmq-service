from typing import Any, Dict
from fastapi import APIRouter, status
from fastapi.exceptions import HTTPException

from app.utils.logging_util import setup_logger
from app.utils.constants import RABBITMQ_QUEUES, AI_ANALYSIS_QUEUE
from app.services.publisher_service import publisher
from app.schemas.publisher_schema import PublisherSchema


class PublisherRouter:

    def __init__(self):
        
        self.logger = setup_logger(__name__)
        self.router = APIRouter()
        self.router.add_api_route('/publisher', self.publisher, methods=['POST'], tags=['publishers'],
                                  status_code=status.HTTP_201_CREATED)
        self.router.add_api_route('/publisher_ai_analysis', self.publisher_ai_analysis, methods=['POST'], tags=['publishers'],
                                  status_code=status.HTTP_201_CREATED)
    

    async def publisher(self, data: PublisherSchema):

        # if data.queue_name not in RABBITMQ_QUEUES:
        #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, 
        #                         detail=f'The queue {data.queue_name} does not have any registered subscribers, thus nobody will receive any message sent to it')
        
        try:
            await publisher.publish_message(data.queue_name, data.message)

        except Exception as e:
            self.logger.error(f'Error while trying to publish message to queue {data.queue_name}: {e}')
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f'Error while trying to publish message to queue {data.queue_name}: {e}')


    async def publisher_ai_analysis(self, message: Dict[str, Any]):

        try:
            await publisher.publish_message(AI_ANALYSIS_QUEUE, message)

        except Exception as e:
            self.logger.error(f'Error while trying to publish message to queue {AI_ANALYSIS_QUEUE}: {e}')
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f'Error while trying to publish message to queue {AI_ANALYSIS_QUEUE}: {e}')
        