"""Общие настройки для приложения."""

import time
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict


class Settings(BaseSettings):
    """Конфиг с переменными окружения."""

    GOOGLE_FITNESS_ENDPOINT: str | None = (
        "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
    )

    SCOPES: List[str] | None = [
        # "https://www.googleapis.com/auth/fitness.activity.read",
        # "https://www.googleapis.com/auth/fitness.blood_glucose.read",
        # "https://www.googleapis.com/auth/fitness.blood_pressure.read",
        # "https://www.googleapis.com/auth/fitness.body.read",
        # "https://www.googleapis.com/auth/fitness.body_temperature.read",
        # "https://www.googleapis.com/auth/fitness.heart_rate.read",
        # "https://www.googleapis.com/auth/fitness.nutrition.read",
        # "https://www.googleapis.com/auth/fitness.oxygen_saturation.read",
        # "https://www.googleapis.com/auth/fitness.reproductive_health.read",
        "https://www.googleapis.com/auth/fitness.sleep.read",
    ]

    # SCOPES: List[str] | None = [
    #     # "https://www.googleapis.com/auth/fitness.activity.read",
    #     # "https://www.googleapis.com/auth/fitness.blood_glucose.read",
    #     # "https://www.googleapis.com/auth/fitness.blood_pressure.read",
    #     # "https://www.googleapis.com/auth/fitness.body.read",
    #     # "https://www.googleapis.com/auth/fitness.body_temperature.read",
    #     # "https://www.googleapis.com/auth/fitness.heart_rate.read",
    #     # "https://www.googleapis.com/auth/fitness.nutrition.read",
    #     # "https://www.googleapis.com/auth/fitness.oxygen_saturation.read",
    #     # "https://www.googleapis.com/auth/fitness.reproductive_health.read",
    #     "https://www.googleapis.com/auth/fitness.sleep.read",
    # ]

    DATA_TYPES_BY_SCOPE: Dict[str, List[str]] | None = {
        "https://www.googleapis.com/auth/fitness.activity.read": [
            "com.google.activity.segment",
            "com.google.calories.bmr",
            "com.google.calories.expended",
            "com.google.cycling.pedaling.cadence",
            "com.google.cycling.pedaling.cumulative",
            "com.google.heart_minutes",
            "com.google.active_minutes",
            "com.google.power.sample",
            "com.google.step_count.cadence",
            "com.google.step_count.delta",
            "com.google.activity.exercise",
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
        "https://www.googleapis.com/auth/fitness.nutrition.read": [
            "com.google.hydration",
            "com.google.nutrition",
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

    REDIS_HOST: str | None = "redis"
    REDIS_PORT: str | None = "6379"
    REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE: str | None = "REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE-"
    
    START_MS: int | None = int((time.time() - 360 * 24 * 60 * 60) * 1000)
    # START_MS: int | None = 0
    END_MS: int | None = int(time.time() * 1000)

    DOMAIN_NAME: str | None = "http://hse-coursework-health.ru"
    AUTH_API_BASE_URL: str | None = f"{DOMAIN_NAME}:8081"
    DATA_COLLECTION_API_BASE_URL: str | None = f"{DOMAIN_NAME}:8082"

    NOTIFICATIONS_API_BASE_URL: str = "http://notifications-api:8083/notifications-api/api/v1/notifications"

    model_config = SettingsConfigDict(
        env_file=".env.prod",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore",  # игнорировать лишние переменные в .env
    )


settings = Settings()
