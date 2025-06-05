import asyncio
from datetime import datetime
from app.utils.logging_util import setup_logger
from app.utils.rabbitmq_util import rabbitmq_util
from app.listeners.queue_message_forwarder import queue_message_forwarder
from app.services.alerting_service import alert_manager, rabbitmq_alert_monitor, AlertType
from app.core.config import settings


class AlertBackgroundTask:
    """Background task to monitor health and send alerts with recovery awareness"""

    def __init__(self):
        self.logger = setup_logger(__name__)
        self.monitoring_interval = 120  # 2 minutes (more frequent for better recovery tracking)
        self.task = None
        self.running = False
        self.last_alert_summary_time = None

    async def start_monitoring(self):
        """Start the alert monitoring background task"""
        if self.running:
            self.logger.warning("Alert monitoring is already running")
            return

        self.running = True
        self.task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("Enhanced alert monitoring started with recovery tracking")

    async def stop_monitoring(self):
        """Stop the alert monitoring background task"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.logger.info("Alert monitoring stopped")

    async def _monitoring_loop(self):
        """Main monitoring loop with enhanced recovery tracking"""
        while self.running:
            try:
                await self._check_and_send_alerts()

                # Send periodic summary for long-running issues
                await self._send_periodic_summary()

                await asyncio.sleep(self.monitoring_interval)

            except asyncio.CancelledError:
                self.logger.info("Alert monitoring task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in alert monitoring loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    async def _check_and_send_alerts(self):
        """Check system health and send alerts if needed"""
        try:
            # Check RabbitMQ connection health with recovery awareness
            connection_status = rabbitmq_util.get_connection_status()
            connection_alerts = await rabbitmq_alert_monitor.check_connection_health(connection_status)

            # Check consumer health
            consumer_statuses = queue_message_forwarder.get_all_consumer_statuses()
            consumer_alerts = await rabbitmq_alert_monitor.check_consumer_health(consumer_statuses)

            # Send all alerts
            all_alerts = connection_alerts + consumer_alerts

            for alert in all_alerts:
                await alert_manager.send_alert(alert)

            # Resolve alerts if systems are healthy
            await self._check_alert_resolutions(connection_status, consumer_statuses)

            if all_alerts:
                self.logger.info(f"Processed {len(all_alerts)} alerts")
            else:
                self.logger.debug("Health check completed - no alerts triggered")

        except Exception as e:
            self.logger.error(f"Error during health check: {e}")

    async def _send_periodic_summary(self):
        """Send periodic summary alerts for long-running issues"""
        now = datetime.utcnow()

        # Send summary every 30 minutes if there are active alerts
        if (not self.last_alert_summary_time or
                (now - self.last_alert_summary_time).total_seconds() > 1800):  # 30 minutes

            active_alerts = alert_manager.get_active_alerts()

            if active_alerts:
                summary = alert_manager.get_alert_summary()

                # Create summary alert for multiple active issues
                if len(active_alerts) > 1:
                    from app.services.alerting_service import Alert, AlertType, AlertSeverity

                    summary_alert = Alert(
                        alert_type=AlertType.HEALTH_CHECK_FAILED,
                        severity=AlertSeverity.MEDIUM,
                        title=f"System Health Summary - {len(active_alerts)} Active Issues",
                        message=f"Multiple issues detected: {', '.join([alert['alert_type'] for alert in active_alerts])}. Recovery in progress: {summary['recovery_in_progress']}",
                        metadata=summary
                    )

                    await alert_manager.send_alert(summary_alert)

                self.last_alert_summary_time = now
                self.logger.info(f"Sent periodic summary - {len(active_alerts)} active alerts")

    async def _check_alert_resolutions(self, connection_status: dict, consumer_statuses: list):
        """Check if any alerts should be resolved with recovery awareness"""

        # Resolve connection alerts if connection is healthy
        if connection_status.get("connected", False):
            await alert_manager.resolve_alert(AlertType.CONNECTION_DOWN, "Connection restored successfully")
            await alert_manager.resolve_alert(AlertType.CONNECTION_RECOVERY_FAILED, "Connection recovery completed")

            # Check if idle/age issues are resolved
            idle_seconds = connection_status.get("idle_seconds", 0)
            if idle_seconds < alert_manager.connection_idle_threshold * 0.5:  # Resolved if under half threshold
                await alert_manager.resolve_alert(AlertType.CONNECTION_IDLE, "Connection activity resumed")

            connection_age = connection_status.get("connection_age_seconds", 0)
            if connection_age < alert_manager.connection_age_threshold * 0.5:  # Fresh connection
                await alert_manager.resolve_alert(AlertType.CONNECTION_AGE, "Connection refreshed")

        # Resolve recovery alerts if not in recovery mode
        if not connection_status.get("recovery_running", False):
            await alert_manager.resolve_alert(AlertType.CONNECTION_RECOVERY_STARTED, "Recovery process completed")
            await alert_manager.resolve_alert(AlertType.RECOVERY_INTERVAL_INCREASED, "Recovery process completed")

        # Resolve consumer alerts if consumers are healthy
        active_consumers = [c for c in consumer_statuses if c.get("status") == "active"]
        if len(active_consumers) == len(consumer_statuses) and len(consumer_statuses) > 0:
            await alert_manager.resolve_alert(AlertType.CONSUMER_DOWN, "All consumers are active")

            # Check if message processing issues are resolved
            for consumer in consumer_statuses:
                idle_seconds = consumer.get("idle_seconds", 0)
                if idle_seconds < alert_manager.consumer_idle_threshold * 0.3:  # Active processing
                    await alert_manager.resolve_alert(AlertType.MESSAGE_PROCESSING_SLOW, f"Consumer {consumer.get('queue_name', 'unknown')} resumed processing")


# Global instance
alert_background_task = AlertBackgroundTask()