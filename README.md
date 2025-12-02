# 🚀 Crypto Analytics Platform

**Полноценная Big Data платформа** для сбора, обработки, машинного обучения и визуализации данных о криптовалютах. Проект демонстрирует современный ETL-пайплайн с Apache Airflow, PySpark, ML-прогнозами на XGBoost и интерактивными дашбордами.

---

## 🎯 Что делает проект

Платформа автоматически:
1. **Собирает данные** о курсах 5 криптовалют с CoinGecko API (365 дней истории)
2. **Загружает Fear & Greed Index** — индекс настроений рынка
3. **Обрабатывает данные** с помощью Apache Spark (агрегация, объёмы, market cap)
4. **Загружает результаты** в аналитическое хранилище PostgreSQL
5. **Прогнозирует тренд** с помощью XGBoost (UP/DOWN/FLAT)
6. **Визуализирует данные** через Streamlit и Grafana дашборды

---

## 🪙 Поддерживаемые криптовалюты

| Монета | Символ | ID CoinGecko |
|--------|--------|--------------|
| Bitcoin | BTC | bitcoin |
| Ethereum | ETH | ethereum |
| BNB | BNB | binancecoin |
| Solana | SOL | solana |
| Cardano | ADA | cardano |

---

## 📡 Источники данных

### 1. CoinGecko API (бесплатный)

**Endpoint:** `https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart`

**Получаемые данные:**
- `prices` — история цен в USD
- `market_caps` — рыночная капитализация
- `total_volumes` — объёмы торгов

**Параметры:**
| Параметр | Значение | Описание |
|----------|----------|----------|
| `vs_currency` | `usd` | Валюта |
| `days` | `365` | Период истории |

### 2. Alternative.me Fear & Greed Index

**Endpoint:** `https://api.alternative.me/fng/?limit=30`

**Данные:**
- `value` — индекс от 0 (страх) до 100 (жадность)
- `value_classification` — категория (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)

---

## 🏗️ Архитектура системы

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      CRYPTO ANALYTICS PLATFORM                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────┐     ┌──────────────┐                                │
│   │  CoinGecko   │     │ Fear & Greed │                                │
│   │     API      │     │     API      │                                │
│   └──────┬───────┘     └──────┬───────┘                                │
│          │                    │                                         │
│          └────────┬───────────┘                                         │
│                   ▼                                                     │
│   ┌─────────────────────────────────────────────────────────────┐      │
│   │                    APACHE AIRFLOW                            │      │
│   │  ┌────────────┐   ┌────────────┐   ┌────────────┐           │      │
│   │  │  Extract   │──▶│ Transform  │──▶│    Load    │           │      │
│   │  │  5 coins   │   │  (PySpark) │   │ PostgreSQL │           │      │
│   │  │ + F&G Index│   │            │   │            │           │      │
│   │  └────────────┘   └────────────┘   └────────────┘           │      │
│   └─────────────────────────────────────────────────────────────┘      │
│                              │                                          │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────┐      │
│   │                    POSTGRESQL DWH                            │      │
│   │  • crypto_daily_metrics (цены, объёмы, market cap)          │      │
│   │  • fear_greed_index (настроения рынка)                      │      │
│   │  • raw_crypto_prices (сырые данные)                         │      │
│   └───────────────────────┬─────────────────────────────────────┘      │
│                           │                                             │
│           ┌───────────────┼───────────────┐                            │
│           ▼               ▼               ▼                            │
│   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                   │
│   │  Streamlit   │ │   Grafana    │ │   XGBoost    │                   │
│   │  Dashboard   │ │  Dashboards  │ │  ML Models   │                   │
│   │   :8501      │ │    :3000     │ │  Trend Pred  │                   │
│   └──────────────┘ └──────────────┘ └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🤖 Machine Learning

### Прогнозирование тренда (XGBoost Classifier)

Модель классифицирует направление движения цены:
- **UP** — рост более 2%
- **DOWN** — падение более 2%
- **FLAT** — боковое движение (±2%)

**Features (признаки):**
- Лаговые цены (1, 3, 7, 14, 30 дней)
- Скользящие средние (MA7, MA14, MA30)
- RSI (Relative Strength Index)
- Волатильность
- Изменения цены за периоды

**Горизонты прогноза:** 7, 14, 30 дней

### Прогноз цены (XGBoost Regressor)

Предсказание абсолютной цены на:
- Завтра (1 день)
- Неделю (7 дней)
- Месяц (30 дней)

---

## 🔄 ETL Pipeline (DAG: crypto_etl)

### Task 1: Extract (`extract_crypto_prices`)
- Загрузка данных 5 криптовалют с CoinGecko
- Загрузка Fear & Greed Index
- Сохранение в JSON и PostgreSQL

### Task 2: Transform (`transform_with_spark`)
- PySpark агрегация по дням:
  - Цены (open, close, avg, max, min)
  - Объёмы торгов (volume_usd)
  - Рыночная капитализация (market_cap_usd)
- Сохранение в Parquet

### Task 3: Load (`load_to_postgres`)
- Загрузка в `crypto_daily_metrics` (UPSERT)

**Расписание:** каждые 6 часов

---

## 🛠️ Технологический стек

| Компонент | Технология | Версия | Назначение |
|-----------|------------|--------|------------|
| Оркестрация | Apache Airflow | 2.9.2 | ETL pipeline |
| Обработка данных | PySpark | 3.5.1 | Трансформация |
| База данных | PostgreSQL | 15 | DWH |
| ML | XGBoost | 2.0+ | Прогнозирование |
| Визуализация | Streamlit | 1.37+ | Интерактивный дашборд |
| Мониторинг | Grafana | 10.2 | Дашборды и алерты |
| Графики | Plotly | 5.22+ | Визуализации |
| Контейнеризация | Docker | - | Изоляция сервисов |

---

## 📁 Структура проекта

```
bigdata_project/
├── airflow/
│   ├── dags/
│   │   └── bitcoin_etl_dag.py    # DAG для 5 криптовалют
│   ├── logs/
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark_jobs/
│   └── bitcoin_transform.py       # Spark трансформации
│
├── streamlit_app/
│   ├── app.py                     # Основной дашборд
│   ├── ml_model.py                # XGBoost модели
│   ├── Dockerfile
│   └── requirements.txt
│
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/           # PostgreSQL datasource
│   │   └── dashboards/            # Dashboard config
│   └── dashboards/
│       └── crypto_analytics.json  # Готовый дашборд
│
├── data/
│   ├── raw/                       # JSON файлы
│   └── processed/                 # Parquet файлы
│
├── docker-compose.yml
├── airflow.env
├── .gitignore
└── README.md
```

---

## 🚀 Быстрый старт

### Требования
- Docker Desktop (4+ ГБ RAM)
- Git

### Запуск

```bash
# 1. Клонирование
git clone https://github.com/your-repo/bigdata_project.git
cd bigdata_project

# 2. Запуск всех сервисов
docker compose up -d

# 3. Дождитесь инициализации (~2-3 минуты)
docker compose logs -f airflow-scheduler
```

### Доступ к сервисам

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| **Streamlit** | http://localhost:8501 | - | - |
| **Grafana** | http://localhost:3000 | admin | admin |
| **Airflow UI** | http://localhost:8080 | admin | admin |
| PostgreSQL DWH | localhost:5433 | analytics | analytics |

---

## 📊 Функции дашборда Streamlit

### 📈 Аналитика
- Выбор криптовалюты (BTC, ETH, BNB, SOL, ADA)
- Графики цен (линейный, candlestick, область)
- KPI метрики (цена, волатильность, объёмы)
- Fear & Greed Index

### 🔮 ML Прогнозы
- Предсказание цены на 1/7/30 дней
- Классификация тренда (UP/DOWN/FLAT)
- Вероятности и метрики модели

### 🔗 Корреляция
- Матрица корреляции 5 криптовалют
- Нормализованное сравнение динамики

### 📋 Данные
- Таблица с историей
- Экспорт в CSV

---

## 📊 Grafana дашборды

Готовый дашборд включает:
- 📈 График цен криптовалют
- 😨 Fear & Greed Gauge
- 💰 Текущие цены (Stat)
- 📊 Объёмы торгов
- 🎢 Волатильность
- 📈 Сравнение всех криптовалют

---

## 📊 Схема базы данных

### `crypto_daily_metrics`
```sql
CREATE TABLE crypto_daily_metrics (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    coin_symbol VARCHAR(10) NOT NULL,
    coin_name VARCHAR(50) NOT NULL,
    event_date DATE NOT NULL,
    open_price_usd NUMERIC,
    close_price_usd NUMERIC,
    avg_price_usd NUMERIC,
    max_price_usd NUMERIC,
    min_price_usd NUMERIC,
    volume_usd NUMERIC,
    market_cap_usd NUMERIC,
    samples_per_day INTEGER,
    processed_at TIMESTAMPTZ,
    UNIQUE(coin_id, event_date)
);
```

### `fear_greed_index`
```sql
CREATE TABLE fear_greed_index (
    id SERIAL PRIMARY KEY,
    event_date DATE UNIQUE NOT NULL,
    value INTEGER NOT NULL,
    value_classification VARCHAR(50)
);
```

---

## 🔧 Полезные команды

```bash
# Статус контейнеров
docker compose ps

# Логи сервисов
docker compose logs -f airflow-scheduler
docker compose logs -f streamlit
docker compose logs -f grafana

# Подключение к PostgreSQL
docker compose exec postgres-dwh psql -U analytics -d bitcoin

# Проверка данных
SELECT coin_id, COUNT(*), MIN(event_date), MAX(event_date) 
FROM crypto_daily_metrics GROUP BY coin_id;

# Перезапуск ETL
docker compose restart airflow-scheduler
```

---

## 🛑 Остановка

```bash
# Остановить сервисы
docker compose down

# Удалить с данными
docker compose down -v
```

---

## ✅ Реализованные фичи

- [x] Поддержка 5 криптовалют
- [x] 365 дней истории
- [x] Объёмы торгов и Market Cap
- [x] Fear & Greed Index
- [x] Корреляция криптовалют
- [x] ML прогнозирование (XGBoost)
- [x] Классификация тренда (UP/DOWN/FLAT)
- [x] Интерактивные Grafana дашборды
- [x] Streamlit визуализация

---

## 📈 Roadmap

- [ ] Real-time данные через WebSocket
- [ ] Интеграция Apache Kafka
- [ ] Алерты в Telegram
- [ ] Хранение в MinIO/S3
- [ ] LSTM для временных рядов

---

## 👤 Автор

Учебный проект по курсу Big Data / TOBD

---

## 📝 Лицензия

MIT License
