from datetime import datetime
from fastapi import APIRouter, status, Query
from fastapi.responses import JSONResponse
from app.listeners.queue_message_forwarder import queue_message_forwarder
from app.utils.rabbitmq_util import rabbitmq_util
from app.utils.constants import RABBITMQ_QUEUES
from app.utils.logging_util import setup_logger
from app.services.alerting_service import alert_manager, Alert, AlertType, AlertSeverity


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

        # Enhanced alert management routes
        self.router.add_api_route('/alerts/active', self.get_active_alerts,
                                  methods=['GET'], tags=['alerts'],
                                  status_code=status.HTTP_200_OK)
        self.router.add_api_route('/alerts/history', self.get_alert_history,
                                  methods=['GET'], tags=['alerts'],
                                  status_code=status.HTTP_200_OK)
        self.router.add_api_route('/alerts/summary', self.get_alert_summary,
                                  methods=['GET'], tags=['alerts'],
                                  status_code=status.HTTP_200_OK)
        self.router.add_api_route('/alerts/test', self.send_test_alert,
                                  methods=['POST'], tags=['alerts'],
                                  status_code=status.HTTP_201_CREATED)
        self.router.add_api_route('/alerts/resolve/{alert_type}', self.resolve_alert,
                                  methods=['POST'], tags=['alerts'],
                                  status_code=status.HTTP_200_OK)

        # Recovery-specific endpoints
        self.router.add_api_route('/recovery/status', self.get_recovery_status,
                                  methods=['GET'], tags=['recovery'],
                                  status_code=status.HTTP_200_OK)

    async def rabbitmq_health(self) -> JSONResponse:
        """Check RabbitMQ connection health with recovery information"""
        try:
            connection_status = rabbitmq_util.get_connection_status()

            # Determine overall health
            is_healthy = connection_status.get("connected", False)
            recovery_running = connection_status.get("recovery_running", False)

            # Add debug information
            debug_info = {
                "connection_object_exists": rabbitmq_util.connection is not None,
                "connection_closed": rabbitmq_util.connection.is_closed if rabbitmq_util.connection else None,
                "reconnecting_flag": connection_status.get("reconnecting", False),
                "health_check_running": connection_status.get("health_check_running", False),
                "recovery_running": recovery_running,
                "recovery_interval": connection_status.get("recovery_interval", None)
            }
            connection_status["debug"] = debug_info

            # Enhanced status determination
            if recovery_running:
                connection_status["status"] = "recovering"
                connection_status["message"] = f"Connection recovery in progress (interval: {connection_status.get('recovery_interval', 'unknown')}s)"
            elif is_healthy:
                connection_status["status"] = "healthy"
                # Check if connection is getting stale
                idle_seconds = connection_status.get("idle_seconds", 0)
                connection_age = connection_status.get("connection_age_seconds", 0)

                max_idle = rabbitmq_util.max_idle_time
                max_age = rabbitmq_util.max_connection_age

                if idle_seconds > max_idle * 0.8 or connection_age > max_age * 0.8:
                    connection_status["warning"] = "Connection approaching staleness limits"
                    connection_status["status"] = "warning"
            else:
                connection_status["status"] = "unhealthy"
                connection_status["message"] = "Connection is down and no recovery process is running"

            # Log the status for debugging
            self.logger.info(f"RabbitMQ health check - Status: {connection_status['status']}, Connected: {is_healthy}, Recovery: {recovery_running}")

            # Return 200 if healthy or recovering, 503 if down without recovery
            status_code = 200 if (is_healthy or recovery_running) else 503

            return JSONResponse(
                content=connection_status,
                status_code=status_code
            )

        except Exception as e:
            self.logger.error(f"Error checking RabbitMQ health: {e}")
            return JSONResponse(
                content={"error": str(e), "healthy": False, "status": "error"},
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
        """Complete health check including RabbitMQ, consumers, and alerts"""
        try:
            # Get RabbitMQ health
            rabbitmq_status = rabbitmq_util.get_connection_status()

            # Get consumer health
            consumer_statuses = queue_message_forwarder.get_all_consumer_statuses()

            # Get active alerts and recovery status
            active_alerts = alert_manager.get_active_alerts()
            alert_summary = alert_manager.get_alert_summary()

            # Calculate overall health
            rabbitmq_healthy = rabbitmq_status.get("connected", False)
            recovery_running = rabbitmq_status.get("recovery_running", False)

            healthy_consumers = sum(1 for c in consumer_statuses if c["status"] == "active")
            total_consumers = len(consumer_statuses)
            consumers_healthy = healthy_consumers == total_consumers

            # Enhanced health determination
            critical_alerts = [a for a in active_alerts if a["severity"] == "critical"]
            high_alerts = [a for a in active_alerts if a["severity"] == "high"]

            if critical_alerts:
                overall_status = "critical"
            elif high_alerts or not rabbitmq_healthy:
                if recovery_running:
                    overall_status = "recovering"
                else:
                    overall_status = "unhealthy"
            elif not consumers_healthy:
                overall_status = "degraded"
            else:
                overall_status = "healthy"

            health_report = {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_status": overall_status,
                "rabbitmq": {
                    "status": "healthy" if rabbitmq_healthy else ("recovering" if recovery_running else "unhealthy"),
                    "connected": rabbitmq_healthy,
                    "recovery_running": recovery_running,
                    "details": rabbitmq_status
                },
                "consumers": {
                    "status": "healthy" if consumers_healthy else "unhealthy",
                    "healthy_count": healthy_consumers,
                    "total_count": total_consumers,
                    "details": consumer_statuses
                },
                "alerts": {
                    "active_count": len(active_alerts),
                    "critical_count": len(critical_alerts),
                    "high_count": len(high_alerts),
                    "active_alerts": active_alerts,
                    "summary": alert_summary
                }
            }

            # Status code based on overall health
            status_code_map = {
                "healthy": 200,
                "degraded": 200,
                "recovering": 200,  # 200 because recovery is expected behavior
                "unhealthy": 503,
                "critical": 503
            }

            return JSONResponse(
                content=health_report,
                status_code=status_code_map.get(overall_status, 503)
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

    async def get_active_alerts(self) -> JSONResponse:
        """Get all currently active alerts"""
        try:
            active_alerts = alert_manager.get_active_alerts()
            return JSONResponse(
                content={
                    "active_alerts": active_alerts,
                    "count": len(active_alerts),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            self.logger.error(f"Error getting active alerts: {e}")
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )

    async def get_alert_history(self, hours: int = Query(24, description="Hours of history to retrieve")) -> JSONResponse:
        """Get alert history for specified number of hours"""
        try:
            alert_history = alert_manager.get_alert_history(hours=hours)
            return JSONResponse(
                content={
                    "alert_history": alert_history,
                    "count": len(alert_history),
                    "hours": hours,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            self.logger.error(f"Error getting alert history: {e}")
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )

    async def get_alert_summary(self) -> JSONResponse:
        """Get comprehensive alert summary with recovery context"""
        try:
            summary = alert_manager.get_alert_summary()
            return JSONResponse(content=summary)
        except Exception as e:
            self.logger.error(f"Error getting alert summary: {e}")
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )

    async def get_recovery_status(self) -> JSONResponse:
        """Get detailed recovery status information"""
        try:
            connection_status = rabbitmq_util.get_connection_status()
            recovery_info = {
                "recovery_running": connection_status.get("recovery_running", False),
                "recovery_interval": connection_status.get("recovery_interval", None),
                "recovery_start_time": alert_manager.recovery_start_time.isoformat() if alert_manager.recovery_start_time else None,
                "recovery_duration_seconds": (datetime.utcnow() - alert_manager.recovery_start_time).total_seconds() if alert_manager.recovery_start_time else None,
                "connection_status": {
                    "connected": connection_status.get("connected", False),
                    "reconnecting": connection_status.get("reconnecting", False),
                    "last_activity": connection_status.get("last_activity"),
                    "connection_established_at": connection_status.get("connection_established_at")
                },
                "recovery_related_alerts": [
                    alert for alert in alert_manager.get_active_alerts()
                    if alert["alert_type"] in ["connection_recovery_started", "connection_recovery_failed", "recovery_interval_increased"]
                ]
            }

            return JSONResponse(content=recovery_info)
        except Exception as e:
            self.logger.error(f"Error getting recovery status: {e}")
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )

    async def send_test_alert(self) -> JSONResponse:
        """Send a test alert to verify alerting system"""
        try:
            test_alert = Alert(
                alert_type=AlertType.HEALTH_CHECK_FAILED,
                severity=AlertSeverity.LOW,
                title="Test Alert - Enhanced System",
                message="This is a test alert to verify the enhanced alerting system with recovery awareness is working correctly.",
                metadata={
                    "test": True,
                    "triggered_by": "manual_test",
                    "service": "rabbitmq_health_service",
                    "features": ["recovery_tracking", "enhanced_notifications", "smart_resolution"]
                }
            )

            success = await alert_manager.send_alert(test_alert, force=True)

            return JSONResponse(
                content={
                    "message": "Test alert sent" if success else "Test alert failed to send",
                    "alert_id": test_alert.alert_id,
                    "success": success,
                    "timestamp": datetime.utcnow().isoformat()
                },
                status_code=201 if success else 500
            )

        except Exception as e:
            self.logger.error(f"Error sending test alert: {e}")
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )

    async def resolve_alert(self, alert_type: str) -> JSONResponse:
        """Manually resolve an active alert"""
        try:
            # Convert string to AlertType enum
            try:
                alert_type_enum = AlertType(alert_type)
            except ValueError:
                return JSONResponse(
                    content={"error": f"Invalid alert type: {alert_type}"},
                    status_code=400
                )

            await alert_manager.resolve_alert(
                alert_type_enum,
                resolution_message="Manually resolved via API"
            )

            return JSONResponse(
                content={
                    "message": f"Alert {alert_type} resolved successfully",
                    "alert_type": alert_type,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

        except Exception as e:
            self.logger.error(f"Error resolving alert: {e}")
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )