import os
from datetime import timedelta
from typing import Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Конфигурация страницы
st.set_page_config(
    page_title="Bitcoin Analytics Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Кастомные стили
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(90deg, #f7931a, #ffb347);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        border-radius: 10px;
        padding: 1rem;
        border-left: 4px solid #f7931a;
    }
    .stMetric {
        background-color: #1e1e2e;
        padding: 1rem;
        border-radius: 10px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
    .info-box {
        background: #1e3a5f;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def _db_settings() -> Tuple[str, str, str, str, str]:
    """Получение настроек подключения к БД"""
    return (
        os.getenv("STREAMLIT_DB_HOST", os.getenv("BITCOIN_DB_HOST", "localhost")),
        os.getenv("STREAMLIT_DB_PORT", os.getenv("BITCOIN_DB_PORT", "5432")),
        os.getenv("STREAMLIT_DB_NAME", os.getenv("BITCOIN_DB_NAME", "bitcoin")),
        os.getenv("STREAMLIT_DB_USER", os.getenv("BITCOIN_DB_USER", "analytics")),
        os.getenv("STREAMLIT_DB_PASSWORD", os.getenv("BITCOIN_DB_PASSWORD", "analytics")),
    )


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Загрузка данных из PostgreSQL с кэшированием на 5 минут"""
    host, port, dbname, user, password = _db_settings()
    with psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    ) as conn:
        query = """
            SELECT
                event_date,
                open_price_usd,
                close_price_usd,
                avg_price_usd,
                max_price_usd,
                min_price_usd,
                samples_per_day,
                processed_at
            FROM bitcoin_daily_metrics
            ORDER BY event_date;
        """
        df = pd.read_sql(query, conn, parse_dates=["event_date", "processed_at"])
    return df


def calculate_metrics(df: pd.DataFrame) -> dict:
    """Расчёт дополнительных метрик"""
    if df.empty:
        return {}
    
    df = df.copy()
    
    # Дневное изменение
    df['daily_change'] = df['close_price_usd'] - df['open_price_usd']
    df['daily_change_pct'] = (df['daily_change'] / df['open_price_usd']) * 100
    
    # Волатильность (разница между max и min)
    df['volatility'] = df['max_price_usd'] - df['min_price_usd']
    df['volatility_pct'] = (df['volatility'] / df['avg_price_usd']) * 100
    
    # Скользящее среднее
    if len(df) >= 7:
        df['ma_7'] = df['avg_price_usd'].rolling(window=7).mean()
    
    latest = df.iloc[-1]
    previous = df.iloc[-2] if len(df) > 1 else latest
    
    return {
        'df_enriched': df,
        'latest': latest,
        'previous': previous,
        'total_days': len(df),
        'avg_volatility': df['volatility_pct'].mean(),
        'max_price_ever': df['max_price_usd'].max(),
        'min_price_ever': df['min_price_usd'].min(),
        'avg_price_period': df['avg_price_usd'].mean(),
        'total_change_pct': ((df.iloc[-1]['close_price_usd'] - df.iloc[0]['open_price_usd']) / df.iloc[0]['open_price_usd']) * 100 if len(df) > 0 else 0
    }


def render_sidebar(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Боковая панель с настройками"""
    st.sidebar.markdown("## ⚙️ Настройки")
    
    # Фильтр по датам
    st.sidebar.markdown("### 📅 Период данных")
    min_date = df["event_date"].min().date()
    max_date = df["event_date"].max().date()
    
    date_range = st.sidebar.date_input(
        "Выберите диапазон",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date
    
    mask = (df["event_date"] >= pd.Timestamp(start_date)) & (
        df["event_date"] <= pd.Timestamp(end_date)
    )
    filtered_df = df.loc[mask].copy()
    
    # Выбор типа графика
    st.sidebar.markdown("### 📊 Тип графика")
    chart_type = st.sidebar.selectbox(
        "Выберите визуализацию",
        ["Линейный график", "Candlestick (свечи)", "Область"],
        index=0
    )
    
    # Информация о данных
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Статистика")
    st.sidebar.metric("Всего дней", len(filtered_df))
    st.sidebar.metric("Точек данных", filtered_df['samples_per_day'].sum() if not filtered_df.empty else 0)
    
    # О проекте
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ О проекте")
    st.sidebar.info(
        "**Bitcoin Data Platform**\n\n"
        "Данные загружаются из CoinGecko API, "
        "обрабатываются Apache Spark и "
        "хранятся в PostgreSQL.\n\n"
        "Обновление: каждые 6 часов"
    )
    
    return filtered_df, chart_type


def render_header():
    """Заголовок дашборда"""
    st.markdown('<h1 class="main-header">₿ Bitcoin Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown(
        "<p style='text-align: center; color: #888;'>"
        "Аналитика курса Bitcoin на основе данных CoinGecko API | "
        "Обработка: Apache Airflow + PySpark | Хранение: PostgreSQL"
        "</p>",
        unsafe_allow_html=True
    )


def render_kpi_metrics(metrics: dict):
    """Отображение ключевых метрик"""
    latest = metrics['latest']
    previous = metrics['previous']
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Текущая цена
    price_delta = latest['close_price_usd'] - previous['close_price_usd']
    price_delta_pct = (price_delta / previous['close_price_usd']) * 100 if previous['close_price_usd'] > 0 else 0
    
    col1.metric(
        "💰 Текущая цена",
        f"${latest['close_price_usd']:,.2f}",
        f"{price_delta_pct:+.2f}%",
        delta_color="normal"
    )
    
    # Дневной максимум
    col2.metric(
        "📈 Максимум дня",
        f"${latest['max_price_usd']:,.2f}",
        f"от avg: +${latest['max_price_usd'] - latest['avg_price_usd']:,.0f}"
    )
    
    # Дневной минимум
    col3.metric(
        "📉 Минимум дня",
        f"${latest['min_price_usd']:,.2f}",
        f"от avg: -${latest['avg_price_usd'] - latest['min_price_usd']:,.0f}",
        delta_color="inverse"
    )
    
    # Волатильность
    volatility = latest['max_price_usd'] - latest['min_price_usd']
    volatility_pct = (volatility / latest['avg_price_usd']) * 100
    col4.metric(
        "🎢 Волатильность",
        f"${volatility:,.0f}",
        f"{volatility_pct:.1f}% от цены"
    )


def render_summary_stats(metrics: dict):
    """Сводная статистика за период"""
    st.markdown("### 📊 Сводка за период")
    
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric(
        "🔝 Максимум периода",
        f"${metrics['max_price_ever']:,.2f}"
    )
    col2.metric(
        "🔻 Минимум периода",
        f"${metrics['min_price_ever']:,.2f}"
    )
    col3.metric(
        "📊 Средняя цена",
        f"${metrics['avg_price_period']:,.2f}"
    )
    col4.metric(
        "📈 Изменение",
        f"{metrics['total_change_pct']:+.2f}%",
        delta_color="normal" if metrics['total_change_pct'] >= 0 else "inverse"
    )


def render_price_chart(df: pd.DataFrame, chart_type: str):
    """Отображение графика цены"""
    st.markdown("### 📈 Динамика курса Bitcoin")
    
    if chart_type == "Candlestick (свечи)":
        fig = go.Figure(data=[go.Candlestick(
            x=df['event_date'],
            open=df['open_price_usd'],
            high=df['max_price_usd'],
            low=df['min_price_usd'],
            close=df['close_price_usd'],
            name='BTC/USD',
            increasing_line_color='#00ff88',
            decreasing_line_color='#ff4444'
        )])
        fig.update_layout(
            title="Bitcoin Candlestick Chart",
            yaxis_title="Цена (USD)",
            xaxis_title="Дата",
            template="plotly_dark",
            height=500
        )
    elif chart_type == "Область":
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['max_price_usd'],
            fill=None,
            mode='lines',
            line_color='rgba(0,255,136,0.3)',
            name='Максимум'
        ))
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['min_price_usd'],
            fill='tonexty',
            mode='lines',
            line_color='rgba(255,68,68,0.3)',
            fillcolor='rgba(247,147,26,0.2)',
            name='Минимум'
        ))
        fig.add_trace(go.Scatter(
            x=df['event_date'],
            y=df['avg_price_usd'],
            mode='lines',
            line=dict(color='#f7931a', width=2),
            name='Средняя цена'
        ))
        fig.update_layout(
            title="Bitcoin Price Range",
            yaxis_title="Цена (USD)",
            xaxis_title="Дата",
            template="plotly_dark",
            height=500
        )
    else:  # Линейный график
        fig = px.line(
            df,
            x="event_date",
            y=["open_price_usd", "close_price_usd", "avg_price_usd"],
            labels={"value": "Цена (USD)", "event_date": "Дата", "variable": "Метрика"},
            title="Динамика цен Bitcoin (USD)",
            template="plotly_dark",
            color_discrete_map={
                "open_price_usd": "#00ff88",
                "close_price_usd": "#ff6b6b",
                "avg_price_usd": "#f7931a"
            }
        )
        fig.update_layout(height=500)
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)


def render_volatility_chart(df: pd.DataFrame):
    """График волатильности"""
    st.markdown("### 🎢 Анализ волатильности")
    
    df_vol = df.copy()
    df_vol['volatility'] = df_vol['max_price_usd'] - df_vol['min_price_usd']
    df_vol['volatility_pct'] = (df_vol['volatility'] / df_vol['avg_price_usd']) * 100
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_vol['event_date'],
        y=df_vol['volatility'],
        name='Волатильность ($)',
        marker_color=np.where(df_vol['volatility_pct'] > df_vol['volatility_pct'].mean(), '#ff6b6b', '#00ff88')
    ))
    
    fig.add_trace(go.Scatter(
        x=df_vol['event_date'],
        y=[df_vol['volatility'].mean()] * len(df_vol),
        mode='lines',
        name=f'Средняя: ${df_vol["volatility"].mean():,.0f}',
        line=dict(color='#f7931a', dash='dash')
    ))
    
    fig.update_layout(
        title="Дневная волатильность Bitcoin",
        yaxis_title="Волатильность (USD)",
        xaxis_title="Дата",
        template="plotly_dark",
        height=400,
        showlegend=True
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_data_table(df: pd.DataFrame):
    """Таблица с данными"""
    st.markdown("### 📋 Детальные данные")
    
    # Форматирование для отображения
    display_df = df.copy()
    display_df['event_date'] = display_df['event_date'].dt.strftime('%Y-%m-%d')
    display_df = display_df.rename(columns={
        'event_date': 'Дата',
        'open_price_usd': 'Открытие ($)',
        'close_price_usd': 'Закрытие ($)',
        'avg_price_usd': 'Средняя ($)',
        'max_price_usd': 'Максимум ($)',
        'min_price_usd': 'Минимум ($)',
        'samples_per_day': 'Точек данных'
    })
    
    # Выбор столбцов
    columns_to_show = ['Дата', 'Открытие ($)', 'Закрытие ($)', 'Средняя ($)', 'Максимум ($)', 'Минимум ($)', 'Точек данных']
    display_df = display_df[columns_to_show]
    
    st.dataframe(
        display_df.sort_values('Дата', ascending=False),
        use_container_width=True,
        hide_index=True
    )
    
    # Кнопка скачивания
    col1, col2, col3 = st.columns([1, 1, 2])
    csv = df.to_csv(index=False).encode("utf-8")
    col1.download_button(
        "📥 Скачать CSV",
        data=csv,
        file_name="bitcoin_daily_metrics.csv",
        mime="text/csv"
    )


def render_footer(df: pd.DataFrame):
    """Футер с информацией об обновлении"""
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'processed_at' in df.columns and not df.empty:
            last_update = df['processed_at'].max()
            st.caption(f"🕐 Последнее обновление: {last_update.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    with col2:
        st.caption(f"📊 Данных в выборке: {len(df)} дней")
    
    with col3:
        st.caption("🔗 Источник: CoinGecko API")


def main():
    """Главная функция приложения"""
    render_header()
    
    # Загрузка данных
    try:
        df = load_data()
    except psycopg2.Error as exc:
        st.error(f"❌ Не удалось подключиться к PostgreSQL: {exc}")
        st.info("Убедитесь, что сервис postgres-dwh запущен и DAG bitcoin_etl выполнен.")
        return
    except Exception as e:
        st.error(f"❌ Ошибка: {e}")
        return
    
    if df.empty:
        st.warning("⚠️ Таблица bitcoin_daily_metrics пока пуста.")
        st.info(
            "Для загрузки данных:\n"
            "1. Откройте Airflow UI: http://localhost:8080\n"
            "2. Активируйте DAG 'bitcoin_etl'\n"
            "3. Запустите DAG вручную (кнопка ▶️)"
        )
        return
    
    # Боковая панель
    filtered_df, chart_type = render_sidebar(df)
    
    if filtered_df.empty:
        st.warning("⚠️ Нет данных в выбранном диапазоне дат.")
        return
    
    # Расчёт метрик
    metrics = calculate_metrics(filtered_df)
    
    # Основные KPI
    render_kpi_metrics(metrics)
    
    st.markdown("---")
    
    # Сводка за период
    render_summary_stats(metrics)
    
    st.markdown("---")
    
    # График цены
    render_price_chart(filtered_df, chart_type)
    
    # График волатильности
    render_volatility_chart(filtered_df)
    
    # Таблица данных
    render_data_table(filtered_df)
    
    # Футер
    render_footer(filtered_df)


if __name__ == "__main__":
    main()
