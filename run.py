import asyncio
import argparse
import logging
import json
import sys
import datetime
import certifi

from settings import Settings
from redis import redis_client
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Dict, Optional

from notifications import notifications_api
import requests

# Настройки
settings = Settings()

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

FLOAT_TYPES = [
    "com.google.oxygen_saturation",
    "com.google.heart_rate.bpm",
    "com.google.height",
    "com.google.weight",
]
INT_TYPES = [
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
    "com.google.sleep.segment",
]

GOOGLE_TO_DATA_TYPE: Dict[str, str] = {
    # float-типы
    "com.google.oxygen_saturation":         "BloodOxygenData",
    "com.google.heart_rate.bpm":            "HeartRateRecord",
    "com.google.height":                    "HeightRecord",
    "com.google.weight":                    "WeightRecord",

    # int-типы
    "com.google.activity.segment":          "ActivitySegmentRecord",
    "com.google.activity.exercise":         "ExerciseSessionRecord",
    "com.google.calories.bmr":              "BasalMetabolicRateRecord",
    "com.google.calories.expended":         "TotalCaloriesBurnedRecord",
    "com.google.cycling.pedaling.cadence":  "CadenceRecord",
    "com.google.cycling.pedaling.cumulative": "CumulativeCadenceRecord",
    "com.google.heart_minutes":             "HeartMinutesRecord",
    "com.google.active_minutes":            "ActiveMinutesRecord",
    "com.google.power.sample":              "PowerRecord",
    "com.google.step_count.cadence":        "StepCadenceRecord",
    "com.google.step_count.delta":          "StepsRecord",
    "com.google.sleep.segment":             "SleepSessionData",
}

# === Настраиваем сессию с retry и актуальным CA ===
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

# === Собираем все data_types из настроек в один список ===
all_data_types = []
for scope in settings.SCOPES or []:
    types = settings.DATA_TYPES_BY_SCOPE.get(scope, [])
    all_data_types.extend([dt for dt in types if dt.startswith("com.google")])

total_types = len(all_data_types)
# Вес одной «части» прогресса
weight = 100.0 / total_types if total_types > 0 else 0.0


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
            timeout=10
        )
        if resp.status_code == 401:
            logger.warning(f"⚠️ Пропускаем {data['email']}: 401 Unauthorized при получении google_fitness_api_token")
            return None
        resp.raise_for_status()
        return resp.json().get("access_token")
    except requests.RequestException as e:
        logger.error(f"Ошибка при получении Google Fitness API токена для {data['email']}: {e}")
        return None


def fetch_access_token(data: dict) -> Optional[str]:
    """
    Делает GET-запрос на URL получения общего access_token (для Вашего backend).
    Возвращает токен или None.
    """
    try:
        resp = session.get(
            data["access_token_url"],
            headers={"Accept": "application/json"},
            timeout=10
        )
        if resp.status_code == 401:
            logger.warning(f"⚠️ Пропускаем {data['email']}: 401 Unauthorized при получении access_token")
            return None
        resp.raise_for_status()
        return resp.json().get("access_token")
    except requests.RequestException as e:
        logger.error(f"Ошибка при получении access_token для {data['email']}: {e}")
        return None


def fetch_fitness_data(
    token: str,
    data_type: str,
    start_ms: int,
    end_ms: int
) -> dict:
    """
    Делает POST-запрос в Google Fitness API, чтобы взять данные за период [start_ms, end_ms]
    """
    body = {
        "aggregateBy": [{"dataTypeName": data_type}],
        "startTimeMillis": start_ms,
        "endTimeMillis": end_ms,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    resp = session.post(
        settings.GOOGLE_FITNESS_ENDPOINT,
        json=body,
        headers=headers,
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()


def process_bucket_data(bucket: dict, data_type: str) -> list[dict]:
    """
    Разбирает один bucket: возвращает список словарей с полями 'timestamp' и 'value'.
    Если встретится неизвестный тип, сохраняет сырые данные в файл <data_type>.json.
    """
    results: list[dict] = []
    for dataset in bucket.get("dataset", []):
        for point in dataset.get("point", []):
            ts_ns = point.get("startTimeNanos")
            time_str = convert_nanoseconds_to_utc(int(ts_ns)) if ts_ns else ""
            values = point.get("value", [])
            if not values:
                continue
            val = values[0]
            if data_type in FLOAT_TYPES:
                v = val.get("fpVal")
            elif data_type in INT_TYPES:
                v = val.get("intVal")
            else:
                # Сохраняем весь payload для неизвестных типов
                fname = f"{data_type.replace('.', '_')}.json"
                try:
                    with open(fname, "w", encoding="utf-8") as f:
                        json.dump(bucket, f, ensure_ascii=False, indent=2)
                    logger.warning(f"Сырые данные для неизвестного типа '{data_type}' сохранены в {fname}")
                except Exception as e:
                    logger.error(f"Не удалось сохранить сырые данные в {fname}: {e}")
                return []
            if v is not None:
                results.append({"timestamp": time_str, "value": v})
    return results


async def fetch_full_period(
    google_fitness_api_token: str,
    access_token: str,
    data_type: str,
    start_ms: int,
    end_ms: int,
    email: str
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

        # 1) Запрос в Google Fitness API
        try:
            payload = fetch_fitness_data(google_fitness_api_token, data_type, current_start, current_end)
        except Exception as e:
            logger.error(f"❌ Ошибка ({data_type}) [{current_start}-{current_end}]: {e}")
            # Прерываем цикл по этому data_type и возвращаем то, что уже отправили
            return sent_records_count

        # 2) Вычисляем прогресс
        buckets = payload.get("bucket", [])
        last_ts = max((int(b.get("startTimeMillis", 0)) for b in buckets), default=None)
        if last_ts is not None and total_duration > 0:
            local_pct = (last_ts - start_ms) / total_duration * 100
            overall = min(int(offset + (local_pct * weight / 100.0)), 100)
            bar = json.dumps({"type": "google_fitness_api", "progress": overall})
            try:
                await redis_client.set(
                    f"{settings.REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE}{email}",
                    bar
                )
                logger.info(f"[{data_type}] Прогресс: local {int(local_pct)}% → overall {overall}%")
            except Exception as e:
                logger.error(f"Не удалось обновить прогресс в Redis для {email}: {e}")

        # 3) Парсим каждый bucket и отправляем
        for bucket in buckets:
            processed = process_bucket_data(bucket, data_type)
            if not processed:
                continue

            # Пропускаем, если нет сопоставления в GOOGLE_TO_DATA_TYPE
            url_data_type = GOOGLE_TO_DATA_TYPE.get(data_type)
            if not url_data_type:
                continue

            url = (
                f"{settings.DATA_COLLECTION_API_BASE_URL}"
                f"/data-collection-api/api/v1/post_data/raw_data_google_fitness_api/{url_data_type}"
            )
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            try:
                resp = session.post(
                    url,
                    json=processed,
                    headers=headers,
                    timeout=10
                )
                resp.raise_for_status()
                sent_records_count += len(processed)
                logger.info(f"→ Отправлено {len(processed)} записей '{data_type}' на {url}")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки {data_type} на {url}: {e}")

        current_start = current_end

    # 4) Финальный прогресс для этого data_type
    final_overall = min(int(offset + weight), 100)
    try:
        await redis_client.set(
            f"{settings.REDIS_DATA_COLLECTION_GOOGLE_FITNESS_API_PROGRESS_BAR_NAMESPACE}{email}",
            json.dumps({"type": "google_fitness_api", "progress": final_overall})
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

    # 1) Загружаем данные пользователя
    user = load_user(args.user_json)
    user_email = user.get("email")
    if not user_email:
        logger.error("Не указан email пользователя в JSON")
        sys.exit(1)

    # 2) Получаем токены
    google_fitness_api_token = fetch_google_fitness_api_token(user)
    if not google_fitness_api_token:
        logger.error("Не удалось получить Google Fitness API токен, выходим")
        sys.exit(1)

    access_token = fetch_access_token(user)
    if not access_token:
        logger.error("Не удалось получить общий access token, выходим")
        sys.exit(1)

    # 3) Подключаем Redis
    try:
        await redis_client.connect()
    except Exception as e:
        logger.error(f"Не удалось подключиться к Redis: {e}")
        sys.exit(1)

    # 4) Уведомление о старте
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

    # 5) Обрабатываем все data_types по порядку
    total_sent = 0
    for idx, dt in enumerate(all_data_types, start=1):
        logger.info(f"Обработка типа данных [{idx}/{total_types}]: {dt}")
        sent_for_type = await fetch_full_period(
            google_fitness_api_token,
            access_token,
            dt,
            settings.START_MS,
            settings.END_MS,
            user_email
        )
        total_sent += sent_for_type
        logger.info(f"Закончили {dt}. Отправлено записей: {sent_for_type}")

    # 6) Уведомление о завершении
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

    # 7) Отключаем Redis и выходим
    try:
        await redis_client.disconnect()
    except Exception as e:
        logger.warning(f"Ошибка при отключении от Redis: {e}")

    logger.info("Скрипт успешно завершил работу")


if __name__ == "__main__":
    asyncio.run(main())
