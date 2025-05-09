"""Общие настройки для приложения."""

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict


class Settings(BaseSettings):
    """Конфиг с переменными окружения."""

    GOOGLE_FITNESS_ENDPOINT: str | None = (
        "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
    )

    SCOPES: List[str] | None = [
        "https://www.googleapis.com/auth/fitness.activity.read",
        "https://www.googleapis.com/auth/fitness.blood_glucose.read",
        "https://www.googleapis.com/auth/fitness.blood_pressure.read",
        "https://www.googleapis.com/auth/fitness.body.read",
        "https://www.googleapis.com/auth/fitness.body_temperature.read",
        "https://www.googleapis.com/auth/fitness.heart_rate.read",
        "https://www.googleapis.com/auth/fitness.location.read",
        "https://www.googleapis.com/auth/fitness.nutrition.read",
        "https://www.googleapis.com/auth/fitness.oxygen_saturation.read",
        "https://www.googleapis.com/auth/fitness.reproductive_health.read",
        "https://www.googleapis.com/auth/fitness.sleep.read",
    ]

    DATA_TYPES_BY_SCOPE: Dict[str, List[str]] | None = {
        "https://www.googleapis.com/auth/fitness.activity.read": [
            "com.google.step_count.delta"
        ],
        "https://www.googleapis.com/auth/fitness.blood_glucose.read": [
            "com.google.blood_glucose"
        ],
        "https://www.googleapis.com/auth/fitness.blood_pressure.read": [
            "com.google.blood_pressure"
        ],
        "https://www.googleapis.com/auth/fitness.body.read": [
            "com.google.weight",
            "com.google.body.fat.percentage",
            "com.google.height",
        ],
        "https://www.googleapis.com/auth/fitness.body_temperature.read": [
            "com.google.body.temperature"
        ],
        "https://www.googleapis.com/auth/fitness.heart_rate.read": [
            "com.google.heart_rate.bpm"
        ],
        "https://www.googleapis.com/auth/fitness.location.read": [
            "com.google.location.sample"
        ],
        "https://www.googleapis.com/auth/fitness.nutrition.read": [
            "com.google.nutrition"
        ],
        "https://www.googleapis.com/auth/fitness.oxygen_saturation.read": [
            "com.google.oxygen_saturation"
        ],
        "https://www.googleapis.com/auth/fitness.reproductive_health.read": [
            "com.google.menstrual_flow",
            "com.google.menstrual_cycle",
        ],
        "https://www.googleapis.com/auth/fitness.sleep.read": [
            "com.google.sleep.segment"
        ],
    }

    CHUNK_DURATION_MS: int | None = 30 * 24 * 60 * 60 * 1000 


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore",  # игнорировать лишние переменные в .env
    )
