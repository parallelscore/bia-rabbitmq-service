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
    RABBITMQ_URL: str = Field(..., json_schema_extra={'env': 'RABBITMQ_URL'})
    REDIS_HOST: str = Field(..., json_schema_extra={'env': 'REDIS_HOST'})
    REDIS_PORT: int = Field(..., json_schema_extra={'env': 'REDIS_PORT'})
    REDIS_DB: int = Field(..., json_schema_extra={'env': 'REDIS_DB'})
    REDIS_PASSWORD: str = Field(..., json_schema_extra={'env': 'REDIS_PASSWORD'})

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
