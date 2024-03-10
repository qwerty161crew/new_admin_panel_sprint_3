from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    pg_user: str
    pg_password: str
    pg_port: int
    pg_name: str
    pg_host: str

    redis_port: int
    redis_host: str

    els_port: int
    els_host: str

    class Config:
        env_prefix = "PG_"
        env_file = ".env"
