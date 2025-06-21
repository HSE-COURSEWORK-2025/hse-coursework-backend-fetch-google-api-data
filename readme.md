# HSE Coursework: Backend Fetch Google API Data

Этот репозиторий содержит процесс для сбора и агрегации данных пользователя из Google Fitness API, их обработки и отправки на внутренние сервисы.

## Основные возможности
- Получение данных Google Fitness API по различным типам (сон, активность, пульс и др.)
- Отправка уведомлений о ходе выгрузки
- Использование Redis для хранения прогресса выгрузки


## Быстрый старт

### 1. Клонирование репозитория
```bash
git clone https://github.com/your-username/hse-coursework-backend-fetch-google-api-data.git
cd hse-coursework-backend-fetch-google-api-data
```

### 2. Сборка Docker-образа
```bash
docker build -f dockerfile.dag -t fetch-users-app:latest .
```

### 3. Запуск контейнера
```bash
docker run --env-file .env.prod fetch-users-app:latest --user-json <user.json>
```
Где `<user.json>` — путь к файлу или строка с данными пользователя.

### 4. Переменные окружения
Используйте `.env.dev` для разработки и `.env.prod` для продакшена. Примеры переменных:
```
DATA_COLLECTION_API_BASE_URL=http://localhost:8082
AUTH_API_BASE_URL=http://localhost:8081
REDIS_HOST=localhost
```

### 5. Развёртывание в Kubernetes
Скрипт для развертывания:
```bash
./deploy.sh
```

## Структура проекта
- `run.py` — основной скрипт запуска
- `models.py` — Pydantic-модели для валидации данных
- `settings.py` — конфигурация и генераторы URL
- `notifications.py` — отправка email-уведомлений
- `redis.py` — клиент для Redis
- `requirements.txt` — зависимости Python
- `dockerfile.dag` — Dockerfile для сборки образа

## Пример запуска
```bash
docker run --env-file .env.prod fetch-users-app:latest --user-json user.json
```

## Пример входных данных

В качестве входных данных необходимо передавать JSON следующего формата (пример, все приватные данные затерты):

```json
{
  "google_sub": "<user_google_sub>",
  "email": "user@example.com",
  "name": "Имя Фамилия",
  "picture": "https://example.com/avatar.jpg",
  "google_fitness_api_token_url": "http://localhost:8081/auth-api/api/v1/internal/users/get_user_google_fitness_api_fresh_access_token?email=user%40example.com",
  "access_token_url": "http://localhost:8081/auth-api/api/v1/internal/users/get_user_auth_token?email=user%40example.com"
}
```
