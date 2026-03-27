from functools import lru_cache
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    DATABASE_URL: str = Field(default=...)
    SPARK_APP_NAME: str = Field(default=...)
    SPARK_TIMEZONE: str = Field(default=...)
    API_PORT: int = Field(default=...)
    GRAFANA_PORT: int = Field(default=3000)
    GF_SECURITY_ADMIN_USER: str = Field(default="admin")
    GF_SECURITY_ADMIN_PASSWORD: str = Field(default="admin")
    GRAFANA_POSTGRES_DB: str = Field(default="postgres")
    GRAFANA_POSTGRES_USER: str = Field(default="grafana_reader")
    GRAFANA_POSTGRES_PASSWORD: str = Field(default="grafana_reader")

    model_config = SettingsConfigDict(
        env_file=(".env",),
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (dotenv_settings,)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
