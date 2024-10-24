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
    RABBITMQ_URL: str = Field(..., json_schema_extra={'env': 'RABBITMQ_URL'})
    REDIS_HOST: str = Field(..., json_schema_extra={'env': 'REDIS_HOST'})
    REDIS_PORT: int = Field(..., json_schema_extra={'env': 'REDIS_PORT'})
    REDIS_DB: int = Field(..., json_schema_extra={'env': 'REDIS_DB'})
    REDIS_PASSWORD: str = Field(..., json_schema_extra={'env': 'REDIS_PASSWORD'})
    ERROR_QUEUE: str = Field(..., json_schema_extra={'env': 'ERROR_QUEUE'})

    base_config: ClassVar = ConfigDict(
        arbitrary_types_allowed=True,
    )


class DevelopmentConfig(BaseConfig):
    DEBUG: bool = Field(True, json_schema_extra={'env': 'DEBUG'})
    AI_SERVICE_BASE_URL: str = Field(..., json_schema_extra={'env': 'AI_SERVICE_BASE_URL'})
    RABBITMQ_URL: str = Field(..., json_schema_extra={'env': 'RABBITMQ_URL'})
    REDIS_HOST: str = Field(..., json_schema_extra={'env': 'REDIS_HOST'})
    REDIS_PORT: int = Field(..., json_schema_extra={'env': 'REDIS_PORT'})
    REDIS_DB: int = Field(..., json_schema_extra={'env': 'REDIS_DB'})
    REDIS_PASSWORD: str = Field(..., json_schema_extra={'env': 'REDIS_PASSWORD'})
    ERROR_QUEUE: str = Field(..., json_schema_extra={'env': 'ERROR_QUEUE'})


class TestingConfig(BaseConfig):
    # DEBUG: bool = Field(True, env='DEBUG')
    DEBUG: bool = Field(True, json_schema_extra={'env': 'DEBUG'})


class ProductionConfig(BaseConfig):
    DEBUG: bool = Field(False, json_schema_extra={'env': 'DEBUG'})
    AI_SERVICE_BASE_URL: str = Field(..., json_schema_extra={'env': 'AI_SERVICE_BASE_URL'})
    RABBITMQ_URL: str = Field(..., json_schema_extra={'env': 'RABBITMQ_URL'})
    REDIS_HOST: str = Field(..., json_schema_extra={'env': 'REDIS_HOST'})
    REDIS_PORT: int = Field(..., json_schema_extra={'env': 'REDIS_PORT'})
    REDIS_DB: int = Field(..., json_schema_extra={'env': 'REDIS_DB'})
    REDIS_PASSWORD: str = Field(..., json_schema_extra={'env': 'REDIS_PASSWORD'})
    ERROR_QUEUE: str = Field(..., json_schema_extra={'env': 'ERROR_QUEUE'})


def get_settings():
    env = os.getenv('ENV', '').lower()

    env_mapping = {
        'production': ('.env.production', ProductionConfig),
        'testing': ('.env.testing', TestingConfig),
        'development': ('.env.development', DevelopmentConfig),
    }

    # If ENV is not specified, default to the basic .env file
    if not env:
        load_dotenv('.env')
        return BaseConfig()

    # Load the environment-specific .env file if ENV is specified
    env_file, config_class = env_mapping.get(env, ('.env', BaseConfig))
    load_dotenv(env_file)
    return config_class()


settings = get_settings()
