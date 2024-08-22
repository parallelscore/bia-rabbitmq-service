import time
import uvicorn
from fastapi import FastAPI

from app.core.config import settings
from app.core.middleware import register_middlewares
from app.api.routes.publisher import PublisherRouter
from app.api.routes.server_metrics import ServerMetrics


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

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, reload=True)  # pragma: no cover
