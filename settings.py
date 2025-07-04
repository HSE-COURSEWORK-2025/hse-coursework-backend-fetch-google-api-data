"""Общие настройки для приложения."""

import time
from abc import ABC, abstractmethod
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict
from urllib.parse import urlencode
import requests

from typing import Optional, Any



class URLGeneratorInterface(ABC):
    @abstractmethod
    def request(
        self,
        session: requests.Session,
        headers: Dict[str, str],
        body: Optional[Dict[str, Any]],
        url_params: Dict[str, Any],
    ) -> dict:
        """
        Выполняет HTTP-запрос для данного генератора.
        Если body не None, используется POST, иначе GET.
        Возвращает распарсенный JSON-ответ.
        """
        ...


class DefaultURLGenerator(URLGeneratorInterface):
    url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"

    def request(
        self,
        session: requests.Session,
        headers: Dict[str, str],
        body: Optional[Dict[str, Any]],
        url_params: Dict[str, Any],
    ) -> dict:
        resp = session.post(self.url, json=body, headers=headers, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("bucket", [])


class SleepTimeDataURLGenerator(URLGeneratorInterface):
    base_url = "https://www.googleapis.com/fitness/v1/users/me/sessions"

    def request(
        self,
        session: requests.Session,
        headers: Dict[str, str],
        body: Optional[Dict[str, Any]],
        url_params: Dict[str, Any],
    ) -> dict:
        start_time = url_params.get("start_time")
        end_time = url_params.get("end_time")
        activity_type = url_params.get("activity_type", 72)
        if not start_time:
            raise ValueError("Missing required parameter: start_time")
        if not end_time:
            raise ValueError("Missing required parameter: end_time")

        params = {
            "startTime": start_time,
            "endTime": end_time,
            "activityType": activity_type,
        }
        query_string = urlencode(params)
        url = f"{self.base_url}?{query_string}"

        resp = session.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        payload = resp.json()
        return payload.get("session", [])


class Settings(BaseSettings):
    """Конфиг с переменными окружения."""

    GOOGLE_FITNESS_ENDPOINT: str | None = (
        "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
    )

    SCOPES: List[str] | None = [
        "https://www.googleapis.com/auth/fitness.activity.read",
        "https://www.googleapis.com/auth/fitness.blood_pressure.read",
        "https://www.googleapis.com/auth/fitness.blood_glucose.read",
        "https://www.googleapis.com/auth/fitness.body.read",
        "https://www.googleapis.com/auth/fitness.body_temperature.read",
        "https://www.googleapis.com/auth/fitness.heart_rate.read",
        "https://www.googleapis.com/auth/fitness.nutrition.read",
        "https://www.googleapis.com/auth/fitness.oxygen_saturation.read",
        "https://www.googleapis.com/auth/fitness.reproductive_health.read",
        "https://www.googleapis.com/auth/fitness.sleep.read",
    ]

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

    API_URL_BY_DATA_TYPES: Dict[str, URLGeneratorInterface] | None = {
        "com.google.sleep.segment": SleepTimeDataURLGenerator()
    }

    CHUNK_DURATION_MS: int | None = 30 * 24 * 60 * 60 * 1000

    REDIS_HOST: str | None = "redis"
    REDIS_PORT: str | None = "6379"
    REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE: str | None = (
        "REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE-"
    )

    START_MS: int | None = int((time.time() - 360 * 24 * 60 * 60) * 1000)
    # START_MS: int | None = 0
    END_MS: int | None = int(time.time() * 1000)

    DOMAIN_NAME: str | None = "http://hse-coursework-health.ru"
    AUTH_API_BASE_URL: str | None = f"{DOMAIN_NAME}:8081"
    DATA_COLLECTION_API_BASE_URL: str | None = f"{DOMAIN_NAME}:8082"

    NOTIFICATIONS_API_BASE_URL: str = (
        "http://notifications-api:8083/notifications-api/api/v1/notifications"
    )

    model_config = SettingsConfigDict(
        env_file=".env.prod",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore",
    )


settings = Settings()
default_url_generator = DefaultURLGenerator()
