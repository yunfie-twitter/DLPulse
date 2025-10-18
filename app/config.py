from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0
    celery_concurrency: int = 3
    
    class Config:
        env_file = ".env"

settings = Settings()
