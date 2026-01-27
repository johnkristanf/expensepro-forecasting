from dotenv import load_dotenv
load_dotenv()

import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    APP_ENV: str
    DATABASE_URL: str

    class Config:
        env_file = ".env"

settings = None
if os.getenv("APP_ENV") == "development":
    settings = Settings()
