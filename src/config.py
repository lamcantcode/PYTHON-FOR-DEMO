"""
Reference: https://pydantic-docs.helpmanual.io/usage/settings/
"""

from pydantic import BaseSettings
from logger import logging

class Settings(BaseSettings):
    HOST: str = "localhost"
    NAME: str = "postgres"
    USER: str = "postgres"
    PASSWORD: str = "postgres"
    CONNECTION_TIMEOUT: int = 60

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_prefix = "POSTGRES_"
        env_file_encoding = "utf-8"

class uvicorn_Settings(BaseSettings):

    HOST: str  = "localhost"
    PORT: int = 0
    LOG_LEVEL: str = "uvicorn"
    ACCESS_LOG: str = "uvicorn"
    LOG_FILENAME: str= "uvicorn"
    LOG_FILEMODE: str = "uvicorn"
    LOG_CONFIG_LEVEL: int= logging.INFO
    LOG_FORMAT: str = "uvicorn"

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_prefix = "UVICORN_"
        env_file_encoding = "utf-8"






