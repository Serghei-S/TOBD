FROM apache/airflow:2.9.2-python3.11

USER root

# Установка системных зависимостей одним слоем
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Копируем requirements.txt отдельно для лучшего кэширования
COPY requirements.txt /requirements.txt

# Устанавливаем зависимости с кэшированием pip
ARG AIRFLOW_VERSION="2.9.2"
ARG PYTHON_MAJOR_MINOR_VERSION="3.11"
ARG AIRFLOW_CONSTRAINTS_LOCATION="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR_VERSION}.txt"

# Используем pip cache для ускорения повторных сборок
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    pip install --constraint "${AIRFLOW_CONSTRAINTS_LOCATION}" -r /requirements.txt
