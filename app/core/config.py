from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_SERVICE_KEY: str
    OCR_URL: str
    MAPPER_URL:str

    model_config = SettingsConfigDict(
        env_file=".env"
    )


settings = Settings()

SUPABASE_URL = settings.SUPABASE_URL.strip()
SUPABASE_SERVICE_KEY = settings.SUPABASE_SERVICE_KEY.strip()
OCR_URL = settings.OCR_URL.strip()
MAPPER_URL = settings.MAPPER_URL.strip()

