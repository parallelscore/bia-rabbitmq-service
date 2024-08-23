import os
from typing import List
from pydantic import Field
from dotenv import load_dotenv
from pydantic_settings import BaseSettings


class BaseConfig(BaseSettings):
    PROJECT_NAME: str = Field('BIA RabbitMQ Service', env='PROJECT_NAME')
    DESCRIPTION: str = Field('This is the backend service for BIA rabbitmq engine', env='DESCRIPTION')
    VERSION: str = Field('1.0.0', env='VERSION')
    CORS_ORIGINS: List[str] = Field(default=['*'], env='CORS_ORIGINS')
    API_V1_STR: str = Field('/api/v1', env='API_V1_STR')
    RABBITMQ_URL: str = Field('amqp://guest:guest@localhost:5672', env='RABBITMQ_URL')
    REDIS_HOST: str = Field('', env='REDIS_HOST')
    REDIS_PORT: int = Field(6379, env='REDIS_PORT')
    REDIS_DB: int = Field(0, env='REDIS_DB')
    REDIS_PASSWORD: str = Field('', env='REDIS_PASSWORD')

    class Config:
        arbitrary_types_allowed = True


class DevelopmentConfig(BaseConfig):
    DEBUG: bool = Field(True, env='DEBUG')
    RABBITMQ_URL: str = Field('', env='RABBITMQ_URL')
    REDIS_HOST: str = Field('', env='REDIS_HOST')
    REDIS_PORT: int = Field(6379, env='REDIS_PORT')
    REDIS_DB: int = Field(0, env='REDIS_DB')
    REDIS_PASSWORD: str = Field('', env='REDIS_PASSWORD')


class TestingConfig(BaseConfig):
    DEBUG: bool = Field(True, env='DEBUG')


class ProductionConfig(BaseConfig):
    DEBUG: bool = Field(False, env='DEBUG')
    RABBITMQ_URL: str = Field('', env='RABBITMQ_URL')
    REDIS_HOST: str = Field('', env='REDIS_HOST')
    REDIS_PORT: int = Field(6379, env='REDIS_PORT')
    REDIS_DB: int = Field(0, env='REDIS_DB')
    REDIS_PASSWORD: str = Field('', env='REDIS_PASSWORD')


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
