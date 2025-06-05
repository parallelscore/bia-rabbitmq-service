import json
import asyncio
import aiohttp
import smtplib
from enum import Enum
from typing import Dict, List
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart

from app.core.config import settings
from app.utils.logging_util import setup_logger



class AlertSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    CONNECTION_IDLE = "connection_idle"
    CONNECTION_AGE = "connection_age"
    CONNECTION_DOWN = "connection_down"
    CONNECTION_RECOVERY_STARTED = "connection_recovery_started"
    CONNECTION_RECOVERY_FAILED = "connection_recovery_failed"
    CONNECTION_RECOVERED = "connection_recovered"
    CONSUMER_DOWN = "consumer_down"
    CONSUMER_ERROR = "consumer_error"
    CONSUMER_RECOVERED = "consumer_recovered"
    HEALTH_CHECK_FAILED = "health_check_failed"
    MESSAGE_PROCESSING_SLOW = "message_processing_slow"
    RECOVERY_INTERVAL_INCREASED = "recovery_interval_increased"


class Alert:
    def __init__(self, alert_type: AlertType, severity: AlertSeverity,
                 title: str, message: str, metadata: Dict = None):
        self.alert_type = alert_type
        self.severity = severity
        self.title = title
        self.message = message
        self.metadata = metadata or {}
        self.timestamp = datetime.utcnow()
        self.alert_id = f"{alert_type.value}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}"

    def to_dict(self) -> Dict:
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type.value,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }


class AlertManager:
    def __init__(self):
        self.logger = setup_logger(__name__)
        self.alert_history: List[Alert] = []
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_cooldowns: Dict[str, datetime] = {}

        # Configuration
        self.email_enabled = getattr(settings, 'ALERT_EMAIL_ENABLED', False)
        self.webhook_enabled = getattr(settings, 'ALERT_WEBHOOK_ENABLED', False)
        self.slack_enabled = getattr(settings, 'ALERT_SLACK_ENABLED', False)

        # Email settings
        self.smtp_server = getattr(settings, 'ALERT_SMTP_SERVER', 'localhost')
        self.smtp_port = getattr(settings, 'ALERT_SMTP_PORT', 587)
        self.smtp_username = getattr(settings, 'ALERT_SMTP_USERNAME', '')
        self.smtp_password = getattr(settings, 'ALERT_SMTP_PASSWORD', '')
        self.smtp_from = getattr(settings, 'ALERT_EMAIL_FROM', 'alerts@example.com')
        self.smtp_to = getattr(settings, 'ALERT_EMAIL_TO', '').split(',')

        # Webhook settings
        self.webhook_url = getattr(settings, 'ALERT_WEBHOOK_URL', '')

        # Slack settings
        self.slack_webhook_url = getattr(settings, 'ALERT_SLACK_WEBHOOK_URL', '')

        # Alert thresholds
        self.connection_idle_threshold = getattr(settings, 'ALERT_CONNECTION_IDLE_THRESHOLD', 18000)  # 5 hours
        self.connection_age_threshold = getattr(settings, 'ALERT_CONNECTION_AGE_THRESHOLD', 82800)    # 23 hours
        self.consumer_idle_threshold = getattr(settings, 'ALERT_CONSUMER_IDLE_THRESHOLD', 86400)     # 24 hours
        self.recovery_failure_threshold = getattr(settings, 'ALERT_RECOVERY_FAILURE_THRESHOLD', 300)  # 5 minutes

        # Cooldown periods (prevent spam)
        self.alert_cooldown_seconds = {
            AlertType.CONNECTION_IDLE: 3600,                    # 1 hour
            AlertType.CONNECTION_AGE: 3600,                     # 1 hour
            AlertType.CONNECTION_DOWN: 300,                     # 5 minutes
            AlertType.CONNECTION_RECOVERY_STARTED: 1800,       # 30 minutes
            AlertType.CONNECTION_RECOVERY_FAILED: 600,         # 10 minutes
            AlertType.CONSUMER_DOWN: 600,                      # 10 minutes
            AlertType.CONSUMER_ERROR: 300,                     # 5 minutes
            AlertType.HEALTH_CHECK_FAILED: 600,                # 10 minutes
            AlertType.RECOVERY_INTERVAL_INCREASED: 1800,       # 30 minutes
        }

        # Track recovery state for smarter alerting
        self.recovery_start_time = None
        self.last_recovery_interval = None

    async def send_alert(self, alert: Alert, force: bool = False) -> bool:
        """Send an alert through configured channels"""

        # Check cooldown unless forced
        if not force and self._is_in_cooldown(alert):
            self.logger.debug(f"Alert {alert.alert_id} is in cooldown, skipping")
            return False

        self.logger.warning(f"Sending alert: {alert.title}")

        # Store alert
        self.alert_history.append(alert)
        self.active_alerts[alert.alert_type.value] = alert
        self.alert_cooldowns[alert.alert_type.value] = datetime.utcnow()

        # Send through configured channels
        success = False

        if self.email_enabled:
            try:
                await self._send_email_alert(alert)
                success = True
            except Exception as e:
                self.logger.error(f"Failed to send email alert: {e}")

        if self.webhook_enabled:
            try:
                await self._send_webhook_alert(alert)
                success = True
            except Exception as e:
                self.logger.error(f"Failed to send webhook alert: {e}")

        if self.slack_enabled:
            try:
                await self._send_slack_alert(alert)
                success = True
            except Exception as e:
                self.logger.error(f"Failed to send Slack alert: {e}")

        return success

    def _is_in_cooldown(self, alert: Alert) -> bool:
        """Check if alert type is in cooldown period"""
        last_sent = self.alert_cooldowns.get(alert.alert_type.value)
        if not last_sent:
            return False

        cooldown_seconds = self.alert_cooldown_seconds.get(alert.alert_type, 300)
        return (datetime.utcnow() - last_sent).total_seconds() < cooldown_seconds

    async def _send_email_alert(self, alert: Alert):
        """Send alert via email"""
        if not self.smtp_to:
            return

        def send_email():
            msg = MIMEMultipart()
            msg['From'] = self.smtp_from
            msg['To'] = ', '.join(self.smtp_to)
            msg['Subject'] = f"[{alert.severity.value.upper()}] RabbitMQ Alert: {alert.title}"

            # Create email body with recovery context
            recovery_context = ""
            if alert.alert_type in [AlertType.CONNECTION_RECOVERY_STARTED, AlertType.CONNECTION_RECOVERY_FAILED, AlertType.CONNECTION_RECOVERED]:
                recovery_context = f"""
Recovery Context:
- Recovery started: {self.recovery_start_time.isoformat() if self.recovery_start_time else 'N/A'}
- Recovery duration: {(datetime.utcnow() - self.recovery_start_time).total_seconds() if self.recovery_start_time else 'N/A'} seconds
- Current recovery interval: {alert.metadata.get('recovery_interval', 'N/A')} seconds
"""

            body = f"""
RabbitMQ Service Alert

Severity: {alert.severity.value.upper()}
Alert Type: {alert.alert_type.value}
Time: {alert.timestamp.isoformat()}

Message:
{alert.message}

{recovery_context}

Metadata:
{json.dumps(alert.metadata, indent=2)}

Alert ID: {alert.alert_id}
Service: {settings.PROJECT_NAME}

Health Check: GET {self._get_base_url()}/api/v1/health/full
"""

            msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            if self.smtp_username and self.smtp_password:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)

            server.send_message(msg)
            server.quit()

        # Run email sending in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, send_email)

    async def _send_webhook_alert(self, alert: Alert):
        """Send alert via webhook"""
        if not self.webhook_url:
            return

        payload = {
            "service": settings.PROJECT_NAME,
            "alert": alert.to_dict(),
            "recovery_context": {
                "recovery_start_time": self.recovery_start_time.isoformat() if self.recovery_start_time else None,
                "recovery_duration_seconds": (datetime.utcnow() - self.recovery_start_time).total_seconds() if self.recovery_start_time else None,
                "health_url": f"{self._get_base_url()}/api/v1/health/full"
            }
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.webhook_url, json=payload, timeout=30) as response:
                if response.status not in {200, 201, 202}:
                    raise Exception(f"Webhook returned status {response.status}")

    async def _send_slack_alert(self, alert: Alert):
        """Send alert via Slack webhook with recovery context"""
        if not self.slack_webhook_url:
            return

        # Color coding for Slack
        color_map = {
            AlertSeverity.LOW: "good",
            AlertSeverity.MEDIUM: "warning",
            AlertSeverity.HIGH: "danger",
            AlertSeverity.CRITICAL: "danger"
        }

        # Add recovery context for connection alerts
        fields = [
            {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
            {"title": "Type", "value": alert.alert_type.value, "short": True},
            {"title": "Time", "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"), "short": True},
            {"title": "Service", "value": settings.PROJECT_NAME, "short": True},
        ]

        # Add recovery information for connection-related alerts
        if alert.alert_type in [AlertType.CONNECTION_RECOVERY_STARTED, AlertType.CONNECTION_RECOVERY_FAILED, AlertType.CONNECTION_RECOVERED]:
            if self.recovery_start_time:
                recovery_duration = (datetime.utcnow() - self.recovery_start_time).total_seconds()
                fields.append({"title": "Recovery Duration", "value": f"{recovery_duration:.0f}s", "short": True})

            recovery_interval = alert.metadata.get('recovery_interval')
            if recovery_interval:
                fields.append({"title": "Recovery Interval", "value": f"{recovery_interval}s", "short": True})

        fields.append({"title": "Message", "value": alert.message, "short": False})

        # Add action buttons for critical alerts
        actions = []
        if alert.severity == AlertSeverity.CRITICAL:
            actions = [
                {
                    "type": "button",
                    "text": "View Health Status",
                    "url": f"{self._get_base_url()}/api/v1/health/full"
                }
            ]

        payload = {
            "text": f"RabbitMQ Alert: {alert.title}",
            "attachments": [{
                "color": color_map.get(alert.severity, "warning"),
                "fields": fields,
                "actions": actions
            }]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.slack_webhook_url, json=payload, timeout=30) as response:
                if response.status not in {200, 201, 202}:
                    raise Exception(f"Slack webhook returned status {response.status}")

    def _get_base_url(self) -> str:
        """Get base URL for health check links"""
        # This could be configured via environment variable
        return getattr(settings, 'ALERT_BASE_URL', 'http://localhost:8000')

    async def resolve_alert(self, alert_type: AlertType, resolution_message: str = ""):
        """Mark an alert as resolved"""
        if alert_type.value in self.active_alerts:
            resolved_alert = self.active_alerts[alert_type.value]
            del self.active_alerts[alert_type.value]

            self.logger.info(f"Resolved alert: {resolved_alert.title}")

            # Send resolution notification for critical alerts
            if resolved_alert.severity in [AlertSeverity.CRITICAL, AlertSeverity.HIGH]:
                resolution_alert = Alert(
                    alert_type=self._get_resolution_alert_type(alert_type),
                    severity=AlertSeverity.LOW,
                    title=f"RESOLVED: {resolved_alert.title}",
                    message=f"Previous alert has been resolved. {resolution_message}",
                    metadata={
                        "resolved_alert_id": resolved_alert.alert_id,
                        "resolution_time": datetime.utcnow().isoformat(),
                        "resolution_duration_seconds": (datetime.utcnow() - resolved_alert.timestamp).total_seconds()
                    }
                )
                await self.send_alert(resolution_alert, force=True)

    def _get_resolution_alert_type(self, original_type: AlertType) -> AlertType:
        """Get the appropriate resolution alert type"""
        resolution_map = {
            AlertType.CONNECTION_DOWN: AlertType.CONNECTION_RECOVERED,
            AlertType.CONSUMER_DOWN: AlertType.CONSUMER_RECOVERED,
            AlertType.CONNECTION_RECOVERY_FAILED: AlertType.CONNECTION_RECOVERED
        }
        return resolution_map.get(original_type, original_type)

    def track_recovery_started(self):
        """Track when recovery starts"""
        self.recovery_start_time = datetime.utcnow()

    def track_recovery_completed(self):
        """Track when recovery completes"""
        self.recovery_start_time = None
        self.last_recovery_interval = None

    def get_active_alerts(self) -> List[Dict]:
        """Get all active alerts"""
        return [alert.to_dict() for alert in self.active_alerts.values()]

    def get_alert_history(self, hours: int = 24) -> List[Dict]:
        """Get alert history for specified hours"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [
            alert.to_dict() for alert in self.alert_history
            if alert.timestamp > cutoff
        ]

    def get_alert_summary(self) -> Dict:
        """Get alert summary with recovery context"""
        now = datetime.utcnow()
        recent_alerts = self.get_alert_history(hours=24)

        summary = {
            "timestamp": now.isoformat(),
            "active_alerts_count": len(self.active_alerts),
            "alerts_last_24h": len(recent_alerts),
            "recovery_in_progress": self.recovery_start_time is not None,
            "recovery_duration_seconds": (now - self.recovery_start_time).total_seconds() if self.recovery_start_time else None,
            "alert_breakdown": {}
        }

        # Count alerts by type and severity
        for alert in recent_alerts:
            alert_type = alert["alert_type"]
            severity = alert["severity"]

            if alert_type not in summary["alert_breakdown"]:
                summary["alert_breakdown"][alert_type] = {"total": 0, "by_severity": {}}

            summary["alert_breakdown"][alert_type]["total"] += 1

            if severity not in summary["alert_breakdown"][alert_type]["by_severity"]:
                summary["alert_breakdown"][alert_type]["by_severity"][severity] = 0

            summary["alert_breakdown"][alert_type]["by_severity"][severity] += 1

        return summary


class RabbitMQAlertMonitor:
    """Monitor RabbitMQ health and trigger alerts with recovery awareness"""

    def __init__(self, alert_manager: AlertManager):
        self.alert_manager = alert_manager
        self.logger = setup_logger(__name__)
        self.last_connection_state = None
        self.last_recovery_state = None

    async def check_connection_health(self, connection_status: Dict) -> List[Alert]:
        """Check connection health and generate alerts with recovery awareness"""
        alerts = []

        connected = connection_status.get("connected", False)
        recovery_running = connection_status.get("recovery_running", False)
        recovery_interval = connection_status.get("recovery_interval", 30)
        reconnecting = connection_status.get("reconnecting", False)

        # Track state changes
        current_state = {
            "connected": connected,
            "recovery_running": recovery_running,
            "reconnecting": reconnecting
        }

        # Connection down alert
        if not connected and not recovery_running:
            alerts.append(Alert(
                alert_type=AlertType.CONNECTION_DOWN,
                severity=AlertSeverity.CRITICAL,
                title="RabbitMQ Connection Down",
                message="RabbitMQ connection is not established and recovery is not running",
                metadata=connection_status
            ))

        # Recovery started alert (only once)
        if recovery_running and (not self.last_recovery_state or not self.last_recovery_state.get("recovery_running")):
            self.alert_manager.track_recovery_started()
            alerts.append(Alert(
                alert_type=AlertType.CONNECTION_RECOVERY_STARTED,
                severity=AlertSeverity.HIGH,
                title="RabbitMQ Connection Recovery Started",
                message=f"Connection recovery process has started with interval {recovery_interval}s",
                metadata={"recovery_interval": recovery_interval, "initial_failure_time": datetime.utcnow().isoformat()}
            ))

        # Recovery interval increased (indicates prolonged failure)
        if recovery_running and self.alert_manager.last_recovery_interval and recovery_interval > self.alert_manager.last_recovery_interval:
            alerts.append(Alert(
                alert_type=AlertType.RECOVERY_INTERVAL_INCREASED,
                severity=AlertSeverity.MEDIUM,
                title="Recovery Interval Increased",
                message=f"Recovery interval increased from {self.alert_manager.last_recovery_interval}s to {recovery_interval}s indicating prolonged connection failure",
                metadata={"old_interval": self.alert_manager.last_recovery_interval, "new_interval": recovery_interval}
            ))

        # Recovery failed (running for too long)
        if recovery_running and self.alert_manager.recovery_start_time:
            recovery_duration = (datetime.utcnow() - self.alert_manager.recovery_start_time).total_seconds()
            if recovery_duration > self.alert_manager.recovery_failure_threshold:
                alerts.append(Alert(
                    alert_type=AlertType.CONNECTION_RECOVERY_FAILED,
                    severity=AlertSeverity.HIGH,
                    title="Connection Recovery Taking Too Long",
                    message=f"Recovery has been running for {recovery_duration:.0f}s (threshold: {self.alert_manager.recovery_failure_threshold}s)",
                    metadata={"recovery_duration": recovery_duration, "recovery_interval": recovery_interval}
                ))

        # Connection recovered
        if connected and (self.last_connection_state and not self.last_connection_state.get("connected")):
            self.alert_manager.track_recovery_completed()
            recovery_duration = None
            if self.alert_manager.recovery_start_time:
                recovery_duration = (datetime.utcnow() - self.alert_manager.recovery_start_time).total_seconds()

            alerts.append(Alert(
                alert_type=AlertType.CONNECTION_RECOVERED,
                severity=AlertSeverity.LOW,
                title="RabbitMQ Connection Recovered",
                message=f"Connection has been successfully restored" + (f" after {recovery_duration:.0f}s" if recovery_duration else ""),
                metadata={"recovery_duration": recovery_duration}
            ))

        # Check idle and age for connected connections
        if connected:
            idle_seconds = connection_status.get("idle_seconds", 0)
            if idle_seconds > self.alert_manager.connection_idle_threshold:
                alerts.append(Alert(
                    alert_type=AlertType.CONNECTION_IDLE,
                    severity=AlertSeverity.MEDIUM,
                    title="RabbitMQ Connection Idle Too Long",
                    message=f"Connection has been idle for {idle_seconds}s (threshold: {self.alert_manager.connection_idle_threshold}s)",
                    metadata={"idle_seconds": idle_seconds, "threshold": self.alert_manager.connection_idle_threshold}
                ))

            connection_age = connection_status.get("connection_age_seconds", 0)
            if connection_age > self.alert_manager.connection_age_threshold:
                alerts.append(Alert(
                    alert_type=AlertType.CONNECTION_AGE,
                    severity=AlertSeverity.MEDIUM,
                    title="RabbitMQ Connection Too Old",
                    message=f"Connection age is {connection_age}s (threshold: {self.alert_manager.connection_age_threshold}s)",
                    metadata={"connection_age": connection_age, "threshold": self.alert_manager.connection_age_threshold}
                ))

        # Update state tracking
        self.last_connection_state = current_state
        self.last_recovery_state = {"recovery_running": recovery_running}
        self.alert_manager.last_recovery_interval = recovery_interval

        return alerts

    async def check_consumer_health(self, consumer_statuses: List[Dict]) -> List[Alert]:
        """Check consumer health and generate alerts"""
        alerts = []

        for consumer in consumer_statuses:
            queue_name = consumer.get("queue_name", "unknown")
            status = consumer.get("status", "unknown")

            # Check if consumer is down
            if status not in ["active", "starting"]:
                alerts.append(Alert(
                    alert_type=AlertType.CONSUMER_DOWN,
                    severity=AlertSeverity.HIGH,
                    title=f"Consumer Down: {queue_name}",
                    message=f"Consumer for queue {queue_name} is in status: {status}",
                    metadata=consumer
                ))
                continue

            # Check if consumer has been idle too long
            idle_seconds = consumer.get("idle_seconds")
            if idle_seconds and idle_seconds > self.alert_manager.consumer_idle_threshold:
                alerts.append(Alert(
                    alert_type=AlertType.MESSAGE_PROCESSING_SLOW,
                    severity=AlertSeverity.MEDIUM,
                    title=f"Consumer Idle: {queue_name}",
                    message=f"Consumer for queue {queue_name} has been idle for {idle_seconds}s",
                    metadata=consumer
                ))

        return alerts


# Global instances
alert_manager = AlertManager()
rabbitmq_alert_monitor = RabbitMQAlertMonitor(alert_manager)