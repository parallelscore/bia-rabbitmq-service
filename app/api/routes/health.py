# In app/api/routes/health.py (new file)

from datetime import datetime
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from app.listeners.queue_message_forwarder import queue_message_forwarder
from app.utils.rabbitmq_util import rabbitmq_util
from app.utils.constants import RABBITMQ_QUEUES

class HealthRouter:
    def __init__(self):
        self.router = APIRouter()
        self.router.add_api_route('/health/rabbitmq', self.rabbitmq_health,
                                  methods=['GET'], tags=['health'])
        self.router.add_api_route('/health/consumers', self.consumer_health,
                                  methods=['GET'], tags=['health'])

    async def rabbitmq_health(self) -> JSONResponse:
        """Check RabbitMQ connection health"""
        now = datetime.utcnow()

        connection_status = {
            "connected": rabbitmq_util.connection and not rabbitmq_util.connection.is_closed,
            "connection_established_at": rabbitmq_util.connection_established_at.isoformat()
            if rabbitmq_util.connection_established_at else None,
            "last_activity": rabbitmq_util.last_activity.isoformat()
            if rabbitmq_util.last_activity else None,
            "current_time": now.isoformat()
        }

        if rabbitmq_util.connection_established_at and rabbitmq_util.last_activity:
            connection_status["connection_age_seconds"] = (
                    now - rabbitmq_util.connection_established_at
            ).total_seconds()
            connection_status["idle_seconds"] = (
                    now - rabbitmq_util.last_activity
            ).total_seconds()

        return JSONResponse(content=connection_status)

    async def consumer_health(self) -> JSONResponse:
        """Check consumer health status"""
        consumer_statuses = []

        for queue_name in RABBITMQ_QUEUES['listen_queues']:
            status = queue_message_forwarder.get_consumer_status(queue_name)
            consumer_statuses.append(status)

        return JSONResponse(content={
            "consumers": consumer_statuses,
            "total_consumers": len(consumer_statuses)
        })