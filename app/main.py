import time
import uvicorn
import asyncio
from fastapi import FastAPI

from app.core.config import settings
from app.api.routes.health import HealthRouter
from app.utils.constants import RABBITMQ_QUEUES
from app.core.middleware import register_middlewares
from app.api.routes.publisher import PublisherRouter
from app.api.routes.server_metrics import ServerMetrics
from app.listeners.queue_message_forwarder import queue_message_forwarder


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

    # Create routers
    publisher_router = PublisherRouter().router
    server_metrics_router = ServerMetrics(app).router
    health_router = HealthRouter().router

    # Register the routers
    app.include_router(server_metrics_router)
    app.include_router(health_router, prefix=settings.API_V1_STR)
    app.include_router(publisher_router, prefix=settings.API_V1_STR)

    # Start consumers on application startup
    app.add_event_handler('startup', start_consumers)

    # Add shutdown event handler to clean up connections
    app.add_event_handler('shutdown', shutdown_consumers)

    return app


async def start_consumers():
    """
    Start the RabbitMQ consumers.
    """

    loop = asyncio.get_event_loop()
    try:
        consumer_tasks = []
        for queue in RABBITMQ_QUEUES['listen_queues']:
            task = loop.create_task(queue_message_forwarder.consume_and_forward(queue))
            consumer_tasks.append(task)

            # Add task completion monitoring
            def task_done_callback(task, queue_name=queue):
                if not task.cancelled():
                    exception = task.exception()
                    if exception:
                        print(f"Consumer task for queue {queue_name} failed with exception: {exception}")
                    else:
                        print(f"Consumer task for queue {queue_name} completed unexpectedly")

            task.add_done_callback(task_done_callback)

        # Store tasks for potential cleanup
        app.state.consumer_tasks = consumer_tasks

    except Exception as e:
        print(f"Error starting consumers: {e}")


async def shutdown_consumers():
    """
    Cleanup consumers and connections on shutdown.
    """
    try:
        # Cancel consumer tasks if they exist
        if hasattr(app.state, 'consumer_tasks'):
            for task in app.state.consumer_tasks:
                if not task.done():
                    task.cancel()

        # Close RabbitMQ connection
        from app.utils.rabbitmq_util import rabbitmq_util
        await rabbitmq_util.close_connection()

        print("Consumers and connections cleaned up successfully")

    except Exception as e:
        print(f"Error during shutdown: {e}")


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)  # pragma: no cover
