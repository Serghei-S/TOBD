# 📊 Bitcoin Data Platform

**Полноценная Big Data платформа** для сбора, обработки и визуализации данных о криптовалюте Bitcoin. Проект демонстрирует современный ETL-пайплайн с использованием Apache Airflow, PySpark и интерактивного дашборда.

---

## 🎯 Что делает проект

Платформа автоматически:
1. **Собирает данные** о курсе Bitcoin с публичного API CoinGecko
2. **Сохраняет сырые данные** в JSON формате и PostgreSQL
3. **Обрабатывает данные** с помощью Apache Spark (агрегация по дням)
4. **Загружает результаты** в аналитическое хранилище PostgreSQL
5. **Визуализирует данные** через интерактивный Streamlit дашборд

---

## 📡 Источник данных

### CoinGecko API (бесплатный)

**Endpoint:** `https://api.coingecko.com/api/v3/coins/bitcoin/market_chart`

**Параметры запроса:**
| Параметр | Значение | Описание |
|----------|----------|----------|
| `vs_currency` | `usd` | Валюта для отображения цены |
| `days` | `30` | Количество дней истории |

**Получаемые данные:**
- `prices` — массив [timestamp, price] с ценами Bitcoin в USD
- `market_caps` — рыночная капитализация
- `total_volumes` — объёмы торгов

**Пример ответа API:**
```json
{
  "prices": [
    [1701388800000, 43521.23],
    [1701392400000, 43612.87],
    ...
  ]
}
```

**Ограничения бесплатного API:**
- 10-30 запросов в минуту
- Данные обновляются каждые 1-5 минут
- История до 365 дней

---

## 🏗️ Архитектура системы

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BITCOIN DATA PLATFORM                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   ┌──────────────┐                                                  │
│   │  CoinGecko   │                                                  │
│   │     API      │                                                  │
│   └──────┬───────┘                                                  │
│          │ HTTP GET (каждые 6 часов)                                │
│          ▼                                                          │
│   ┌──────────────────────────────────────────────────────────┐      │
│   │                    APACHE AIRFLOW                         │      │
│   │  ┌────────────┐   ┌────────────┐   ┌────────────┐        │      │
│   │  │  Extract   │──▶│ Transform  │──▶│    Load    │        │      │
│   │  │  (Python)  │   │  (PySpark) │   │  (Python)  │        │      │
│   │  └────────────┘   └────────────┘   └────────────┘        │      │
│   └──────────────────────────────────────────────────────────┘      │
│          │                   │                   │                  │
│          ▼                   ▼                   ▼                  │
│   ┌────────────┐      ┌────────────┐      ┌────────────┐           │
│   │  RAW JSON  │      │  Parquet   │      │ PostgreSQL │           │
│   │   Files    │      │   Files    │      │    DWH     │           │
│   └────────────┘      └────────────┘      └─────┬──────┘           │
│                                                  │                  │
│                                                  ▼                  │
│                                          ┌────────────┐            │
│                                          │  Streamlit │            │
│                                          │  Dashboard │            │
│                                          └────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 ETL Pipeline (DAG: bitcoin_etl)

### Task 1: Extract (`extract_bitcoin_prices`)
- Запрос к CoinGecko API
- Сохранение JSON в `data/raw/bitcoin_YYYY-MM-DD.json`
- Запись сырых данных в таблицу `raw_bitcoin_prices`

### Task 2: Transform (`transform_with_spark`)
- Чтение JSON файла через PySpark
- Парсинг временных меток (Unix → DateTime)
- Агрегация по дням:
  - `open_price_usd` — цена открытия дня
  - `close_price_usd` — цена закрытия дня
  - `avg_price_usd` — средняя цена за день
  - `max_price_usd` — максимальная цена
  - `min_price_usd` — минимальная цена
  - `samples_per_day` — количество точек данных
- Сохранение в Parquet: `data/processed/bitcoin_YYYY-MM-DD/`

### Task 3: Load (`load_to_postgres`)
- Чтение Parquet файла
- Загрузка в таблицу `bitcoin_daily_metrics` (UPSERT)

**Расписание:** каждые 6 часов (`timedelta(hours=6)`)

---

## 🛠️ Технологический стек

| Компонент | Технология | Версия | Назначение |
|-----------|------------|--------|------------|
| Оркестрация | Apache Airflow | 2.9.2 | Планирование и мониторинг ETL |
| Обработка данных | PySpark | 3.5.1 | Трансформация и агрегация |
| База данных | PostgreSQL | 15 | Хранение RAW и DWH |
| Визуализация | Streamlit | 1.37.0 | Интерактивный дашборд |
| Контейнеризация | Docker | - | Изоляция сервисов |
| Графики | Plotly | 5.22.0 | Интерактивные визуализации |

---

## 📁 Структура проекта

```
bigdata_project/
├── airflow/
│   ├── dags/
│   │   ├── __init__.py
│   │   └── bitcoin_etl_dag.py    # Основной DAG
│   ├── logs/                      # Логи выполнения задач
│   ├── plugins/                   # Кастомные плагины Airflow
│   ├── Dockerfile                 # Образ Airflow + PySpark
│   └── requirements.txt           # Python зависимости
│
├── spark_jobs/
│   ├── __init__.py
│   └── bitcoin_transform.py       # Spark трансформации
│
├── streamlit_app/
│   ├── app.py                     # Дашборд
│   ├── Dockerfile
│   └── requirements.txt
│
├── data/
│   ├── raw/                       # Сырые JSON файлы
│   └── processed/                 # Parquet файлы после Spark
│
├── docs/
│   └── architecture.md            # Документация архитектуры
│
├── docker-compose.yml             # Конфигурация всех сервисов
├── airflow.env                    # Переменные окружения
└── README.md                      # Этот файл
```

---

## 🚀 Быстрый старт

### Требования
- Docker Desktop (4+ ГБ RAM)
- Git

### Запуск

```bash
# 1. Клонирование репозитория
git clone <repository-url>
cd bigdata_project

# 2. Запуск всех сервисов
docker compose up -d

# 3. Дождитесь инициализации (~2-3 минуты)
docker compose logs -f airflow-init
```

### Доступ к сервисам

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| Airflow UI | http://localhost:8080 | admin | admin |
| Streamlit | http://localhost:8501 | - | - |
| PostgreSQL DWH | localhost:5433 | analytics | analytics |

---

## ⚙️ Конфигурация

### Переменные окружения (airflow.env)

```env
# API
BITCOIN_API_URL=https://api.coingecko.com/api/v3/coins/bitcoin/market_chart
BITCOIN_LOOKBACK_DAYS=30
COINGECKO_API_KEY=CG-DemoAPIKey

# PostgreSQL DWH
BITCOIN_DB_HOST=postgres-dwh
BITCOIN_DB_PORT=5432
BITCOIN_DB_NAME=bitcoin
BITCOIN_DB_USER=analytics
BITCOIN_DB_PASSWORD=analytics
```

---

## 📊 Схема базы данных

### Таблица: `raw_bitcoin_prices`
```sql
CREATE TABLE raw_bitcoin_prices (
    id SERIAL PRIMARY KEY,
    logical_date DATE NOT NULL,
    payload JSONB NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Таблица: `bitcoin_daily_metrics`
```sql
CREATE TABLE bitcoin_daily_metrics (
    event_date DATE PRIMARY KEY,
    open_price_usd NUMERIC,
    close_price_usd NUMERIC,
    avg_price_usd NUMERIC,
    max_price_usd NUMERIC,
    min_price_usd NUMERIC,
    samples_per_day INTEGER,
    processed_at TIMESTAMPTZ
);
```

---

## 🔧 Отладка

```bash
# Статус контейнеров
docker compose ps

# Логи Airflow Scheduler
docker compose logs -f airflow-scheduler

# Логи Streamlit
docker compose logs -f streamlit

# Подключение к PostgreSQL
docker compose exec postgres-dwh psql -U analytics -d bitcoin

# Проверка данных
SELECT * FROM bitcoin_daily_metrics ORDER BY event_date DESC LIMIT 10;
```

---

## 🛑 Остановка

```bash
# Остановить все сервисы
docker compose down

# Остановить и удалить volumes (ОСТОРОЖНО: удалит данные)
docker compose down -v
```

---

## 📈 Roadmap

- [ ] Добавить больше криптовалют (ETH, BNB, SOL)
- [ ] Интеграция с Apache Kafka для real-time данных
- [ ] Хранение сырых данных в MinIO/S3
- [ ] Мониторинг через Grafana + Prometheus
- [ ] Алерты в Telegram при аномалиях цены
- [ ] ML модель для прогнозирования цены

---

## 👤 Автор

Учебный проект по курсу Big Data

---

## 📝 Лицензия

MIT License
