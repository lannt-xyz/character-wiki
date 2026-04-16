from pathlib import Path
from typing import Tuple, Type

from pydantic import field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource

_PROJECT_ROOT = Path(__file__).parent.parent
_YAML_FILE = str(_PROJECT_ROOT / "config" / "settings.yaml")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        yaml_file=_YAML_FILE,
        yaml_file_encoding="utf-8",
        env_prefix="PIPELINE_",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls),
        )

    # Story source — swap story = change these 3 fields only
    story_slug: str = "mao-son-troc-quy-nhan"
    base_url: str = "https://truyencv.io/truyen/{story_slug}/chuong-{n}/"
    total_chapters: int = 3534

    # API endpoints
    ollama_url: str = "http://localhost:11434"

    # LLM configuration
    llm_timeout: int = 120
    llm_max_retries: int = 3

    # Paths
    data_dir: str = "data"
    db_path: str = "db/pipeline.db"
    logs_dir: str = "logs"

    # Crawler
    crawler_rate_limit: float = 1.0
    crawler_max_retries: int = 5
    crawler_delay_sec: float = 1.0

    # Wiki extraction
    wiki_batch_size: int = 5
    wiki_extract_model: str = "gemma4-32k:latest"
    wiki_context_threshold: int = 50
    wiki_max_consecutive_fail: int = 5
    wiki_snapshot_min_change: int = 1

    @field_validator("total_chapters")
    @classmethod
    def total_chapters_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("total_chapters must be positive")
        return v

    def get_chapter_url(self, chapter_num: int) -> str:
        return (
            self.base_url
            .replace("{story_slug}", self.story_slug)
            .replace("{n}", str(chapter_num))
        )


settings = Settings()
