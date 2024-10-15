import time
from sys import prefix

import uvicorn
import asyncio
from fastapi import FastAPI

from app.core.config import settings
from app.core.middleware import register_middlewares
from app.api.routes.publisher import PublisherRouter
from app.api.routes.server_metrics import ServerMetrics
from app.listeners.queue_message_forwader import queue_message_forwader
from app.listeners.queue_message_forwader_ai_service import queue_message_forwader_ai_service
from app.utils.constants import RABBITMQ_QUEUES



def create_app() -> FastAPI:

    app = FastAPI(
        title=settings.PROJECT_NAME,
        description=settings.DESCRIPTION,
        version=settings.VERSION,
    )
    app.state.start_time = time.time()
    app.state.requests_processed = 0

    # Register middleware
    register_middlewares(app)

    publisher_router = PublisherRouter().router
    server_metrics_router = ServerMetrics(app).router

    # Register the routers
    app.include_router(server_metrics_router)
    app.include_router(publisher_router, prefix=settings.API_V1_STR)

    # Start consumers on application startup
    app.add_event_handler('startup', start_consumers)

    return app


async def start_consumers():
    """
    Start the RabbitMQ consumers.
    """

    loop = asyncio.get_event_loop()
    try:
        # await loop.create_task(queue_message_forwader.consume_and_forward('bia_publish_queue'))
        await loop.create_task(queue_message_forwader.consume_and_forward('insomnia'))
        # await loop.create_task(queue_message_forwader_ai_service.consume_and_forward_to_ai_service('bia_publish_queue'))
        
        # for queue in RABBITMQ_QUEUES:
        #     await loop.create_task(queue_message_forwader.consume_and_forward(queue))

    except Exception as e:
        print(f"Error starting consumers: {e}")


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)  # pragma: no cover
