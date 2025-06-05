from datetime import datetime
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from app.listeners.queue_message_forwarder import queue_message_forwarder
from app.utils.rabbitmq_util import rabbitmq_util
from app.utils.constants import RABBITMQ_QUEUES
from app.utils.logging_util import setup_logger


class HealthRouter:
    def __init__(self):
        self.router = APIRouter()
        self.logger = setup_logger(__name__)

        # Add health check routes
        self.router.add_api_route('/health/rabbitmq', self.rabbitmq_health,
                                  methods=['GET'], tags=['health'],
                                  status_code=status.HTTP_200_OK)
        self.router.add_api_route('/health/consumers', self.consumer_health,
                                  methods=['GET'], tags=['health'],
                                  status_code=status.HTTP_200_OK)
        self.router.add_api_route('/health/full', self.full_health_check,
                                  methods=['GET'], tags=['health'],
                                  status_code=status.HTTP_200_OK)

    async def rabbitmq_health(self) -> JSONResponse:
        """Check RabbitMQ connection health"""
        try:
            connection_status = rabbitmq_util.get_connection_status()

            # Determine overall health
            is_healthy = connection_status.get("connected", False)

            if is_healthy:
                # Check if connection is getting stale
                idle_seconds = connection_status.get("idle_seconds", 0)
                connection_age = connection_status.get("connection_age_seconds", 0)

                max_idle = rabbitmq_util.max_idle_time
                max_age = rabbitmq_util.max_connection_age

                if idle_seconds > max_idle * 0.8 or connection_age > max_age * 0.8:
                    connection_status["warning"] = "Connection approaching staleness limits"

            return JSONResponse(
                content=connection_status,
                status_code=200 if is_healthy else 503
            )

        except Exception as e:
            self.logger.error(f"Error checking RabbitMQ health: {e}")
            return JSONResponse(
                content={"error": str(e), "healthy": False},
                status_code=503
            )

    async def consumer_health(self) -> JSONResponse:
        """Check consumer health status"""
        try:
            consumer_statuses = queue_message_forwarder.get_all_consumer_statuses()

            # Determine overall consumer health
            healthy_consumers = 0
            total_consumers = len(consumer_statuses)

            for consumer in consumer_statuses:
                if consumer["status"] == "active":
                    healthy_consumers += 1

            overall_health = {
                "consumers": consumer_statuses,
                "total_consumers": total_consumers,
                "healthy_consumers": healthy_consumers,
                "health_percentage": (healthy_consumers / total_consumers * 100) if total_consumers > 0 else 0,
                "overall_status": "healthy" if healthy_consumers == total_consumers else "degraded"
            }

            status_code = 200 if healthy_consumers == total_consumers else 503

            return JSONResponse(content=overall_health, status_code=status_code)

        except Exception as e:
            self.logger.error(f"Error checking consumer health: {e}")
            return JSONResponse(
                content={"error": str(e), "healthy": False},
                status_code=503
            )

    async def full_health_check(self) -> JSONResponse:
        """Complete health check including RabbitMQ and consumers"""
        try:
            # Get RabbitMQ health
            rabbitmq_status = rabbitmq_util.get_connection_status()

            # Get consumer health
            consumer_statuses = queue_message_forwarder.get_all_consumer_statuses()

            # Calculate overall health
            rabbitmq_healthy = rabbitmq_status.get("connected", False)

            healthy_consumers = sum(1 for c in consumer_statuses if c["status"] == "active")
            total_consumers = len(consumer_statuses)
            consumers_healthy = healthy_consumers == total_consumers

            overall_healthy = rabbitmq_healthy and consumers_healthy

            health_report = {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_status": "healthy" if overall_healthy else "unhealthy",
                "rabbitmq": {
                    "status": "healthy" if rabbitmq_healthy else "unhealthy",
                    "details": rabbitmq_status
                },
                "consumers": {
                    "status": "healthy" if consumers_healthy else "unhealthy",
                    "healthy_count": healthy_consumers,
                    "total_count": total_consumers,
                    "details": consumer_statuses
                }
            }

            return JSONResponse(
                content=health_report,
                status_code=200 if overall_healthy else 503
            )

        except Exception as e:
            self.logger.error(f"Error performing full health check: {e}")
            return JSONResponse(
                content={
                    "timestamp": datetime.utcnow().isoformat(),
                    "overall_status": "error",
                    "error": str(e)
                },
                status_code=503
            )