import asyncio
import argparse
import logging
import json
import math
import sys
import datetime
import certifi
import requests

from settings import Settings, default_url_generator
from redis import redis_client
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Dict, Optional, Any, List, Tuple

from notifications import notifications_api
from models import BucketModel
from abc import ABC


settings = Settings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


GOOGLE_TO_DATA_TYPE: Dict[str, str] = {
    "com.google.oxygen_saturation": "BloodOxygenData",
    "com.google.heart_rate.bpm": "HeartRateRecord",
    "com.google.height": "HeightRecord",
    "com.google.weight": "WeightRecord",
    "com.google.activity.segment": "ActivitySegmentRecord",
    "com.google.activity.exercise": "ExerciseSessionRecord",
    "com.google.calories.bmr": "BasalMetabolicRateRecord",
    "com.google.calories.expended": "TotalCaloriesBurnedRecord",
    "com.google.cycling.pedaling.cadence": "CadenceRecord",
    "com.google.cycling.pedaling.cumulative": "CumulativeCadenceRecord",
    "com.google.heart_minutes": "HeartMinutesRecord",
    "com.google.active_minutes": "ActiveMinutesRecord",
    "com.google.power.sample": "PowerRecord",
    "com.google.step_count.cadence": "StepCadenceRecord",
    "com.google.step_count.delta": "StepsRecord",
    "com.google.sleep.segment": "SleepSessionData",
    "com.google.sleep.segment.stages": "SleepSessionStagesData",
    "com.google.sleep.segment.time": "SleepSessionTimeData",
    "com.google.blood_pressure": "BloodPressureData",
    "com.google.blood_glucose": "BloodGlucoseData",
    "com.google.body.fat.percentage": "BodyFatPercentageData",
    "com.google.body.temperature": "BodyTemperatureData",
    "com.google.hydration": "HydrationData",
    "com.google.nutrition": "NutritionData",
}

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.verify = certifi.where()

all_data_types = []
for scope in settings.SCOPES or []:
    types = settings.DATA_TYPES_BY_SCOPE.get(scope, [])
    all_data_types.extend([dt for dt in types if dt.startswith("com.google")])

total_types = len(all_data_types)
weight = 100.0 / total_types if total_types > 0 else 0.0


class DataProcessorInterface(ABC):
    def process(self, bucket: Any) -> Tuple[str, List[dict]]:
        pass


class DefaultDataProcessor(DataProcessorInterface):
    def __init__(self, data_type: str):
        self.data_type = data_type

    def process(self, bucket: Any) -> Tuple[str, List[dict]]:
        results: list[dict] = []
        parsed_bucket = BucketModel.model_validate(bucket)

        for dataset in parsed_bucket.dataset:
            for point in dataset.point:
                ts_ns = point.startTimeNanos
                time_str = convert_nanoseconds_to_utc(int(ts_ns)) if ts_ns else ""
                values = point.value
                if not values:
                    continue
                val = values[0]
                if val.fpVal:
                    v = val.fpVal
                elif val.intVal:
                    v = val.intVal
                else:
                    return self.data_type, []
                if v is not None:
                    results.append({"timestamp": time_str, "value": v})
        return self.data_type, results


class SleepStagesDataProcessor(DataProcessorInterface):
    data_type = "com.google.sleep.segment.stages"

    def process(self, bucket: Any) -> Tuple[str, List[dict]]:
        results: list[dict] = []
        parsed_bucket = BucketModel.model_validate(bucket)
        start_time_str = convert_milliseconds_to_utc(int(parsed_bucket.startTimeMillis))
        end_time_str = convert_milliseconds_to_utc(int(parsed_bucket.endTimeMillis))

        for dataset in parsed_bucket.dataset:
            result_arr = []
            for point in dataset.point:
                start_time_ns = point.startTimeNanos
                end_time_ns = point.endTimeNanos
                start_time_str = (
                    convert_nanoseconds_to_utc(int(start_time_ns))
                    if start_time_ns
                    else ""
                )
                end_time_str = (
                    convert_nanoseconds_to_utc(int(end_time_ns)) if end_time_ns else ""
                )
                values = point.value
                if not values:
                    continue
                val = values[0]

                stage_val = None
                if val.fpVal:
                    stage_val = val.fpVal
                elif val.intVal:
                    stage_val = val.intVal
                if stage_val is not None:
                    result_arr.append(
                        {
                            "endTime": end_time_str,
                            "stage": stage_val,
                            "startTime": start_time_str,
                        }
                    )
            if result_arr:
                results.append(
                    {"timestamp": start_time_str, "value": json.dumps(result_arr)}
                )
        return self.data_type, results


class SleepTimeDataProcessor(DataProcessorInterface):
    data_type = "com.google.sleep.segment.time"

    def process(self, bucket: Any) -> Tuple[str, List[dict]]:
        results: list[dict] = []
        parsed_bucket = BucketModel.model_validate(bucket)

        start_time_ms = parsed_bucket.startTimeMillis
        end_time_ms = parsed_bucket.endTimeMillis

        if start_time_ms is None or end_time_ms is None:
            return self.data_type, results

        duration_ms = end_time_ms - start_time_ms
        duration_min = duration_ms / (1000 * 60)
        duration_min = int(math.ceil(duration_min))

        results.append(
            {
                "timestamp": convert_milliseconds_to_utc(start_time_ms),
                "value": duration_min,
            }
        )

        return self.data_type, results


data_processors_by_datatype = {
    dt: [DefaultDataProcessor(dt)] for dt in GOOGLE_TO_DATA_TYPE
}

data_processors_by_datatype["com.google.sleep.segment"] = [
    SleepStagesDataProcessor(),
    SleepTimeDataProcessor(),
]


def convert_milliseconds_to_utc(timestamp_ms: int) -> str:
    """
    Конвертирует миллисекунды в строку вида "DD Month YYYY HH:MM:SS.mmm UTC"
    """
    timestamp_sec = timestamp_ms / 1000.0
    dt = datetime.datetime.utcfromtimestamp(timestamp_sec)
    ms = int(timestamp_ms % 1000)
    return dt.strftime("%d %B %Y %H:%M:%S") + f".{ms:03d} UTC"


def convert_nanoseconds_to_utc(timestamp_ns: int) -> str:
    """
    Конвертирует наносекунды в строку вида "DD Month YYYY HH:MM:SS.micro UTC"
    """
    timestamp_sec = timestamp_ns / 1_000_000_000.0
    dt = datetime.datetime.utcfromtimestamp(timestamp_sec)
    nanosec = int(timestamp_ns % 1_000_000_000)
    micros = f"{nanosec:09d}"[:6]
    return dt.strftime("%d %B %Y %H:%M:%S") + f".{micros} UTC"


def load_user(user_json: str) -> dict:
    """
    Загружает JSON-объект пользователя либо из файла, либо из строки.
    """
    try:
        with open(user_json, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, OSError):
        return json.loads(user_json)


def fetch_google_fitness_api_token(data: dict) -> Optional[str]:
    """
    Делает GET-запрос на URL получения access_token для Google Fitness API.
    Возвращает токен или None в случае ошибки / 401.
    """
    try:
        resp = session.get(
            data["google_fitness_api_token_url"],
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 401:
            logger.warning(
                f"⚠️ Пропускаем {data['email']}: 401 Unauthorized при получении google_fitness_api_token"
            )
            return None
        resp.raise_for_status()
        return resp.json().get("access_token")
    except requests.RequestException as e:
        logger.error(
            f"Ошибка при получении Google Fitness API токена для {data['email']}: {e}"
        )
        return None


def fetch_access_token(data: dict) -> Optional[str]:
    """
    Делает GET-запрос на URL получения общего access_token (для Вашего backend).
    Возвращает токен или None.
    """
    try:
        resp = session.get(
            data["access_token_url"], headers={"Accept": "application/json"}, timeout=10
        )
        if resp.status_code == 401:
            logger.warning(
                f"⚠️ Пропускаем {data['email']}: 401 Unauthorized при получении access_token"
            )
            return None
        resp.raise_for_status()
        return resp.json().get("access_token")
    except requests.RequestException as e:
        logger.error(f"Ошибка при получении access_token для {data['email']}: {e}")
        return None


def fetch_fitness_data(token: str, data_type: str, start_ms: int, end_ms: int) -> dict:
    """
    Выполняет запрос к Google Fitness API для указанного data_type за период [start_ms, end_ms].
    URLGeneratorInterface теперь отвечает за выполнение запроса.
    """
    body = {
        "aggregateBy": [{"dataTypeName": data_type}],
        "startTimeMillis": start_ms,
        "endTimeMillis": end_ms,
    }
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

    start_iso = datetime.datetime.utcfromtimestamp(start_ms / 1000).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )
    end_iso = datetime.datetime.utcfromtimestamp(end_ms / 1000).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )

    url_generator = settings.API_URL_BY_DATA_TYPES.get(data_type, default_url_generator)
    return url_generator.request(
        session=session,
        headers=headers,
        body=body,
        url_params={"start_time": start_iso, "end_time": end_iso, "activity_type": 72},
    )


async def fetch_full_period(
    google_fitness_api_token: str,
    access_token: str,
    data_type: str,
    start_ms: int,
    end_ms: int,
    email: str,
) -> int:
    """
    Для одного data_type бежит по чанкам, считает прогресс, парсит данные и отправляет их на внешний сервис.
    Возвращает количество успешно отправленных записей для этого data_type (sum of processed lengths).
    """
    try:
        idx = all_data_types.index(data_type)
    except ValueError:
        idx = 0
    offset = idx * weight
    total_duration = end_ms - start_ms
    current_start = start_ms

    sent_records_count = 0

    while current_start < end_ms:
        current_end = min(current_start + settings.CHUNK_DURATION_MS, end_ms)

        try:
            payload = fetch_fitness_data(
                google_fitness_api_token, data_type, current_start, current_end
            )
        except Exception as e:
            logger.error(
                f"❌ Ошибка ({data_type}) [{current_start}-{current_end}]: {e}"
            )
            return sent_records_count

        last_ts = max((int(b.get("startTimeMillis", 0)) for b in payload), default=None)
        curr_t = convert_milliseconds_to_utc(current_start)
        if last_ts is not None and total_duration > 0:
            local_pct = (last_ts - start_ms) / total_duration * 100
            overall = min(int(offset + (local_pct * weight / 100.0)), 100)
            bar = json.dumps({"type": "google_fitness_api", "progress": overall})
            try:
                await redis_client.set(
                    f"{settings.REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE}{email}",
                    bar,
                )
                logger.info(
                    f"[{data_type}] Прогресс: local {int(local_pct)}% → overall {overall}%"
                )
            except Exception as e:
                logger.error(f"Не удалось обновить прогресс в Redis для {email}: {e}")

        for bucket in payload:
            data_processors = data_processors_by_datatype.get(data_type)
            if not data_processors:
                continue
            for data_processor in data_processors:
                curr_data_type, processed = data_processor.process(bucket)

                if not processed:
                    continue

                url_data_type = GOOGLE_TO_DATA_TYPE.get(curr_data_type)

                if not url_data_type:
                    continue

                url = (
                    f"{settings.DATA_COLLECTION_API_BASE_URL}"
                    f"/data-collection-api/api/v1/post_data/raw_data_google_fitness_api/{url_data_type}"
                )
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {access_token}",
                }

                try:
                    resp = session.post(
                        url, json=processed, headers=headers, timeout=10
                    )
                    resp.raise_for_status()
                    sent_records_count += len(processed)
                    logger.info(
                        f"→ Отправлено {len(processed)} записей '{data_type}' на {url}"
                    )
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки {data_type} на {url}: {e}")

        current_start = current_end

    final_overall = min(int(offset + weight), 100)
    try:
        await redis_client.set(
            f"{settings.REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE}{email}",
            json.dumps({"type": "google_fitness_api", "progress": final_overall}),
        )
        logger.info(f"[{data_type}] Завершён: overall {final_overall}%")
    except Exception as e:
        logger.error(f"Не удалось записать финальный прогресс в Redis для {email}: {e}")

    return sent_records_count


async def main():
    parser = argparse.ArgumentParser(
        description="Сбор всех Google Fitness данных для одного пользователя"
    )
    parser.add_argument(
        "--user-json",
        required=True,
        help="Путь к JSON-файлу или JSON-строка с данными пользователя",
    )
    args = parser.parse_args()

    user = load_user(args.user_json)
    user_email = user.get("email")
    if not user_email:
        logger.error("Не указан email пользователя в JSON")
        sys.exit(1)

    logger.info(f"Выполняется для пользователя: {user_email}")

    google_fitness_api_token = fetch_google_fitness_api_token(user)
    if not google_fitness_api_token:
        logger.error("Не удалось получить Google Fitness API токен, выходим")
        sys.exit(0)

    access_token = fetch_access_token(user)
    if not access_token:
        logger.error("Не удалось получить общий access token, выходим")
        sys.exit(0)

    try:
        await redis_client.connect()
    except Exception as e:
        logger.error(f"Не удалось подключиться к Redis: {e}")
        sys.exit(1)

    start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    subject_start = "[GoogleFitness] Начало выгрузки данных"
    body_start = f"""
    <html>
      <body>
        <h2>🚀 Выгрузка Google Fitness API — Старт</h2>
        <p><strong>Пользователь:</strong> {user_email}</p>
        <p><strong>Время запуска:</strong> {start_time}</p>
        <p>Начинается сбор всех типов данных ({total_types} типов).</p>
      </body>
    </html>
    """
    try:
        await notifications_api.send_email(user_email, subject_start, body_start)
        logger.info("Отправлено email-уведомление о старте выгрузки")
    except Exception as e:
        logger.error(f"Не удалось отправить email-уведомление о старте: {e}")

    total_sent = 0
    for idx, dt in enumerate(all_data_types, start=1):
        logger.info(f"Обработка типа данных [{idx}/{total_types}]: {dt}")
        sent_for_type = await fetch_full_period(
            google_fitness_api_token,
            access_token,
            dt,
            settings.START_MS,
            settings.END_MS,
            user_email,
        )
        total_sent += sent_for_type
        logger.info(f"Закончили {dt}. Отправлено записей: {sent_for_type}")

    finish_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    subject_end = "[GoogleFitness] Завершение выгрузки данных"
    body_end = f"""
    <html>
      <body>
        <h2>✅ Выгрузка Google Fitness API — Завершена</h2>
        <p><strong>Пользователь:</strong> {user_email}</p>
        <p><strong>Время старта:</strong> {start_time}</p>
        <p><strong>Время окончания:</strong> {finish_time}</p>
        <p><strong>Всего типов обработано:</strong> {total_types}</p>
        <p><strong>Всего записей отправлено:</strong> {total_sent}</p>
      </body>
    </html>
    """
    try:
        await notifications_api.send_email(user_email, subject_end, body_end)
        logger.info("Отправлено email-уведомление о завершении выгрузки")
    except Exception as e:
        logger.error(f"Не удалось отправить email-уведомление о завершении: {e}")

    try:
        await redis_client.disconnect()
    except Exception as e:
        logger.warning(f"Ошибка при отключении от Redis: {e}")

    logger.info("Скрипт успешно завершил работу")


if __name__ == "__main__":
    asyncio.run(main())
