from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    RIOT_API_KEY: str
    ENVIRONMENT: str = "production"
    CACHE_DURATION_HOURS: int = 24
    MAX_PLAYERS_ANALYZED: int = 50
    
    class Config:
        env_file = ".env"

settings = Settings()
