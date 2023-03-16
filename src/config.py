from pydantic import BaseSettings, PostgresDsn
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    db_parameter_url: PostgresDsn
    db_transaction_url: PostgresDsn
    jwt_secret: str
    algorithm: str
    jwt_expire_time: int

    class Config:
        env_file = ".env"


@lru_cache
def get_settings():
    return Settings()  # type: ignore
