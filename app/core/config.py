import os
from dotenv import load_dotenv
from typing import List, ClassVar
from pydantic import Field, ConfigDict
from pydantic_settings import BaseSettings


class BaseConfig(BaseSettings):
    PROJECT_NAME: str = Field('BIA RabbitMQ Service', json_schema_extra={'env': 'PROJECT_NAME'})
    DESCRIPTION: str = Field('This is the backend service for BIA rabbitmq engine',
                             json_schema_extra={'env': 'DESCRIPTION'})
    VERSION: str = Field('1.0.0', json_schema_extra={'env': 'VERSION'})
    CORS_ORIGINS: List[str] = Field(default=['*'], json_schema_extra={'env': 'CORS_ORIGINS'})
    API_V1_STR: str = Field('/api/v1', json_schema_extra={'env': 'API_V1_STR'})
    AI_SERVICE_BASE_URL: str = Field(..., json_schema_extra={'env': 'AI_SERVICE_BASE_URL'})
    AI_ANALYSIS_QUEUE: str = Field(..., json_schema_extra={'env': 'AI_ANALYSIS_QUEUE'})
    BACKEND_PUBLISH_QUEUE: str = Field(..., json_schema_extra={'env': 'BACKEND_PUBLISH_QUEUE'})
    RABBITMQ_URL: str = Field(..., json_schema_extra={'env': 'RABBITMQ_URL'})
    REDIS_HOST: str = Field(..., json_schema_extra={'env': 'REDIS_HOST'})
    REDIS_PORT: int = Field(..., json_schema_extra={'env': 'REDIS_PORT'})
    REDIS_DB: int = Field(..., json_schema_extra={'env': 'REDIS_DB'})
    REDIS_PASSWORD: str = Field(..., json_schema_extra={'env': 'REDIS_PASSWORD'})
    ERROR_QUEUE: str = Field(..., json_schema_extra={'env': 'ERROR_QUEUE'})

    # RabbitMQ health monitoring settings
    RABBITMQ_HEALTH_CHECK_INTERVAL: int = Field(3600, json_schema_extra={'env': 'RABBITMQ_HEALTH_CHECK_INTERVAL'})  # 1 hour
    RABBITMQ_MAX_IDLE_TIME: int = Field(21600, json_schema_extra={'env': 'RABBITMQ_MAX_IDLE_TIME'})  # 6 hours
    RABBITMQ_MAX_CONNECTION_AGE: int = Field(86400, json_schema_extra={'env': 'RABBITMQ_MAX_CONNECTION_AGE'})  # 24 hours

    # Alert system settings
    ALERT_EMAIL_ENABLED: bool = Field(False, json_schema_extra={'env': 'ALERT_EMAIL_ENABLED'})
    ALERT_WEBHOOK_ENABLED: bool = Field(False, json_schema_extra={'env': 'ALERT_WEBHOOK_ENABLED'})
    ALERT_SLACK_ENABLED: bool = Field(False, json_schema_extra={'env': 'ALERT_SLACK_ENABLED'})

    # Email alert settings
    ALERT_SMTP_SERVER: str = Field('localhost', json_schema_extra={'env': 'ALERT_SMTP_SERVER'})
    ALERT_SMTP_PORT: int = Field(587, json_schema_extra={'env': 'ALERT_SMTP_PORT'})
    ALERT_SMTP_USERNAME: str = Field('', json_schema_extra={'env': 'ALERT_SMTP_USERNAME'})
    ALERT_SMTP_PASSWORD: str = Field('', json_schema_extra={'env': 'ALERT_SMTP_PASSWORD'})
    ALERT_EMAIL_FROM: str = Field('alerts@example.com', json_schema_extra={'env': 'ALERT_EMAIL_FROM'})
    ALERT_EMAIL_TO: str = Field('', json_schema_extra={'env': 'ALERT_EMAIL_TO'})  # Comma-separated emails

    # Webhook alert settings
    ALERT_WEBHOOK_URL: str = Field('', json_schema_extra={'env': 'ALERT_WEBHOOK_URL'})

    # Slack alert settings
    ALERT_SLACK_WEBHOOK_URL: str = Field('', json_schema_extra={'env': 'ALERT_SLACK_WEBHOOK_URL'})

    # Base URL for health check links in alerts
    ALERT_BASE_URL: str = Field('http://localhost:8000', json_schema_extra={'env': 'ALERT_BASE_URL'})

    # Alert thresholds
    ALERT_CONNECTION_IDLE_THRESHOLD: int = Field(18000, json_schema_extra={'env': 'ALERT_CONNECTION_IDLE_THRESHOLD'})  # 5 hours
    ALERT_CONNECTION_AGE_THRESHOLD: int = Field(82800, json_schema_extra={'env': 'ALERT_CONNECTION_AGE_THRESHOLD'})    # 23 hours
    ALERT_CONSUMER_IDLE_THRESHOLD: int = Field(86400, json_schema_extra={'env': 'ALERT_CONSUMER_IDLE_THRESHOLD'})     # 24 hours

    # Recovery-specific alert settings
    ALERT_RECOVERY_FAILURE_THRESHOLD: int = Field(300, json_schema_extra={'env': 'ALERT_RECOVERY_FAILURE_THRESHOLD'})  # 5 minutes

    base_config: ClassVar = ConfigDict(
        arbitrary_types_allowed=True,
    )


class DevConfig(BaseConfig):
    DEBUG: bool = Field(True, json_schema_extra={'env': 'DEBUG'})


class DemoConfig(BaseConfig):
    DEBUG: bool = Field(True, json_schema_extra={'env': 'DEBUG'})


class ProdConfig(BaseConfig):
    DEBUG: bool = Field(False, json_schema_extra={'env': 'DEBUG'})


def get_settings():
    env = os.getenv('ENV', '').lower()

    env_mapping = {
        'prod': ('.env.production', ProdConfig),
        'demo': ('.env.testing', DemoConfig),
        'dev': ('.env.development', DevConfig),
    }

    # If ENV is not specified, default to the basic .env file
    if not env:
        load_dotenv('.env')
        return BaseConfig()

    # Load the environment-specific .env file if ENV is specified
    env_file, config_class = env_mapping.get(env, ('.env', BaseConfig))

    # Load the env file only if it exists
    if os.path.exists(env_file):
        print(f"Loading {env} configuration from {env_file}")
        load_dotenv(env_file)
    else:
        print(f"Environment file {env_file} does not exist")

    return config_class()


settings = get_settings()
