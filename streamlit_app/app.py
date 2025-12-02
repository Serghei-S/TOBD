import os
from datetime import timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import streamlit as st
from dotenv import load_dotenv

from ml_model import CryptoPricePredictor, TrendClassifier, predict_trend

load_dotenv()

st.set_page_config(
    page_title="Crypto Analytics & Predictions",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

CRYPTO_OPTIONS = {
    "Bitcoin (BTC)": "bitcoin",
    "Ethereum (ETH)": "ethereum", 
    "BNB": "binancecoin",
    "Solana (SOL)": "solana",
    "Cardano (ADA)": "cardano",
}

CRYPTO_COLORS = {
    "bitcoin": "#f7931a",
    "ethereum": "#627eea",
    "binancecoin": "#f3ba2f",
    "solana": "#00ffa3",
    "cardano": "#0033ad",
}

# Стили
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #f7931a, #627eea, #00ffa3);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .trend-up { color: #00ff88; font-weight: bold; font-size: 1.5rem; }
    .trend-down { color: #ff4444; font-weight: bold; font-size: 1.5rem; }
    .trend-flat { color: #ffaa00; font-weight: bold; font-size: 1.5rem; }
    div[data-testid="stMetricValue"] { font-size: 1.5rem; }
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-radius: 12px;
        padding: 15px;
        border: 1px solid #dee2e6;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 10px 20px;
        color: white !important;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #f7931a 0%, #ff6b6b 100%) !important;
    }
</style>
""", unsafe_allow_html=True)


def _db_settings() -> Tuple[str, str, str, str, str]:
    return (
        os.getenv("STREAMLIT_DB_HOST", "localhost"),
        os.getenv("STREAMLIT_DB_PORT", "5432"),
        os.getenv("STREAMLIT_DB_NAME", "bitcoin"),
        os.getenv("STREAMLIT_DB_USER", "analytics"),
        os.getenv("STREAMLIT_DB_PASSWORD", "analytics"),
    )


@st.cache_data(ttl=300)
def load_data(coin_id: str = "bitcoin") -> pd.DataFrame:
    host, port, dbname, user, password = _db_settings()
    queries = [
        f"""SELECT coin_id, coin_symbol, coin_name, event_date,
               open_price_usd, close_price_usd, avg_price_usd,
               max_price_usd, min_price_usd, volume_usd, market_cap_usd,
               samples_per_day, processed_at
        FROM crypto_daily_metrics WHERE coin_id = '{coin_id}' ORDER BY event_date;""",
        """SELECT 'bitcoin' as coin_id, 'BTC' as coin_symbol, 'Bitcoin' as coin_name,
               event_date, open_price_usd, close_price_usd, avg_price_usd,
               max_price_usd, min_price_usd, 0 as volume_usd, 0 as market_cap_usd,
               samples_per_day, processed_at
        FROM bitcoin_daily_metrics ORDER BY event_date;"""
    ]
    
    with psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
        for query in queries:
            try:
                df = pd.read_sql(query, conn, parse_dates=["event_date", "processed_at"])
                if not df.empty:
                    return df
            except:
                continue
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_all_crypto() -> pd.DataFrame:
    """Загрузка данных всех криптовалют для корреляции"""
    host, port, dbname, user, password = _db_settings()
    query = """SELECT coin_id, coin_symbol, event_date, close_price_usd 
               FROM crypto_daily_metrics ORDER BY event_date;"""
    try:
        with psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
            return pd.read_sql(query, conn, parse_dates=["event_date"])
    except:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_fear_greed() -> pd.DataFrame:
    """Загрузка Fear & Greed Index"""
    host, port, dbname, user, password = _db_settings()
    query = "SELECT event_date, value, value_classification FROM fear_greed_index ORDER BY event_date DESC LIMIT 30;"
    try:
        with psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
            return pd.read_sql(query, conn, parse_dates=["event_date"])
    except:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def get_predictions(df: pd.DataFrame) -> Dict:
    if len(df) < 60:
        return None
    try:
        predictor = CryptoPricePredictor(lookback_days=30)
        return predictor.predict_multiple_horizons(df)
    except Exception as e:
        st.warning(f"Ошибка прогноза: {e}")
        return None


@st.cache_data(ttl=600)
def get_trend_prediction(df: pd.DataFrame, horizon: int = 7) -> Dict:
    if len(df) < 60:
        return None
    try:
        return predict_trend(df, horizon_days=horizon, threshold_pct=2.0)
    except Exception as e:
        st.warning(f"Ошибка классификации: {e}")
        return None


def render_sidebar() -> Tuple[str, str, pd.DataFrame]:
    st.sidebar.markdown("## ⚙️ Настройки")
    
    st.sidebar.markdown("### 🪙 Криптовалюта")
    selected_name = st.sidebar.selectbox("Выберите монету", list(CRYPTO_OPTIONS.keys()))
    coin_id = CRYPTO_OPTIONS[selected_name]
    
    df = load_data(coin_id)
    
    if df.empty:
        st.sidebar.warning(f"Нет данных для {selected_name}")
        return coin_id, selected_name, df
    
    st.sidebar.markdown("### 📅 Период")
    min_date, max_date = df["event_date"].min().date(), df["event_date"].max().date()
    date_range = st.sidebar.date_input("Диапазон", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        mask = (df["event_date"] >= pd.Timestamp(date_range[0])) & (df["event_date"] <= pd.Timestamp(date_range[1]))
        df = df.loc[mask].copy()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Данные")
    st.sidebar.metric("Дней", len(df))
    if not df.empty:
        st.sidebar.metric("Цена", f"${df['close_price_usd'].iloc[-1]:,.2f}")
    
    st.sidebar.markdown("---")
    st.sidebar.info("**Crypto Platform**\n\n🔹 CoinGecko API\n🔹 Airflow + Spark\n🔹 XGBoost ML\n🔹 Grafana: :3000")
    
    return coin_id, selected_name, df


def render_header(coin_name: str):
    st.markdown(f'<h1 class="main-header">🚀 {coin_name} Analytics</h1>', unsafe_allow_html=True)


def render_fear_greed():
    """Fear & Greed индекс"""
    fg_df = load_fear_greed()
    if fg_df.empty:
        return
    
    latest = fg_df.iloc[0]
    value = int(latest['value'])
    classification = latest['value_classification']
    
    # Цвет по значению
    if value < 25:
        color = "#ff4444"
        emoji = "😱"
    elif value < 50:
        color = "#ffaa00"
        emoji = "😰"
    elif value < 75:
        color = "#88cc00"
        emoji = "😊"
    else:
        color = "#00ff88"
        emoji = "🤑"
    
    st.markdown(f"""
    <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #1a1a2e, #16213e); 
                border-radius: 15px; margin: 10px 0;">
        <h3 style="color: #888;">Fear & Greed Index</h3>
        <div style="font-size: 3rem; color: {color};">{emoji} {value}</div>
        <div style="color: {color}; font-size: 1.2rem;">{classification}</div>
    </div>
    """, unsafe_allow_html=True)


def render_trend_prediction(df: pd.DataFrame):
    """Предсказание тренда"""
    st.markdown("### 🎯 Прогноз тренда (ML классификация)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        horizon = st.selectbox("Горизонт прогноза", [7, 14, 30], index=0, format_func=lambda x: f"{x} дней")
    
    trend_pred = get_trend_prediction(df, horizon)
    
    if trend_pred is None:
        st.info("Недостаточно данных для классификации тренда")
        return
    
    trend = trend_pred['trend']
    confidence = trend_pred['confidence']
    probs = trend_pred['probabilities']
    
    with col2:
        if trend == 'UP':
            st.markdown(f'<div class="trend-up">📈 {trend} ({confidence:.1%})</div>', unsafe_allow_html=True)
        elif trend == 'DOWN':
            st.markdown(f'<div class="trend-down">📉 {trend} ({confidence:.1%})</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="trend-flat">➡️ {trend} ({confidence:.1%})</div>', unsafe_allow_html=True)
    
    # Вероятности
    st.markdown("**Вероятности:**")
    prob_df = pd.DataFrame({'Тренд': list(probs.keys()), 'Вероятность': list(probs.values())})
    fig = px.bar(prob_df, x='Тренд', y='Вероятность', color='Тренд',
                color_discrete_map={'UP': '#00ff88', 'DOWN': '#ff4444', 'FLAT': '#ffaa00'})
    fig.update_layout(height=250, showlegend=False, template='plotly_dark')
    st.plotly_chart(fig, use_container_width=True)
    
    # Метрики модели
    with st.expander("📊 Метрики модели"):
        metrics = trend_pred.get('metrics', {})
        c1, c2 = st.columns(2)
        c1.metric("Train Accuracy", f"{metrics.get('train_accuracy', 0):.2%}")
        c2.metric("Test Accuracy", f"{metrics.get('test_accuracy', 0):.2%}")


def render_correlation():
    """Корреляция между криптовалютами"""
    st.markdown("### 🔗 Корреляция криптовалют")
    
    all_df = load_all_crypto()
    if all_df.empty or len(all_df['coin_id'].unique()) < 2:
        st.info("Недостаточно данных для анализа корреляции. Запустите DAG crypto_etl.")
        return
    
    # Pivot для корреляции
    pivot_df = all_df.pivot(index='event_date', columns='coin_symbol', values='close_price_usd')
    corr_matrix = pivot_df.corr()
    
    fig = px.imshow(corr_matrix, text_auto='.2f', aspect='auto',
                   color_continuous_scale='RdYlGn', zmin=-1, zmax=1,
                   title="Матрица корреляции цен")
    fig.update_layout(height=400, template='plotly_dark')
    st.plotly_chart(fig, use_container_width=True)
    
    # Сравнительный график
    st.markdown("**Сравнение динамики (нормализовано):**")
    normalized = pivot_df.apply(lambda x: (x - x.iloc[0]) / x.iloc[0] * 100)
    fig2 = px.line(normalized.reset_index().melt(id_vars='event_date'),
                  x='event_date', y='value', color='coin_symbol',
                  labels={'value': 'Изменение %', 'event_date': 'Дата'})
    fig2.update_layout(height=400, template='plotly_dark')
    st.plotly_chart(fig2, use_container_width=True)


def render_kpi(df: pd.DataFrame, coin_id: str):
    if df.empty:
        return
    
    latest = df.iloc[-1]
    previous = df.iloc[-2] if len(df) > 1 else latest
    
    price_delta = latest['close_price_usd'] - previous['close_price_usd']
    price_pct = (price_delta / previous['close_price_usd']) * 100 if previous['close_price_usd'] > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Цена", f"${latest['close_price_usd']:,.2f}", f"{price_pct:+.2f}%")
    col2.metric("📈 Макс", f"${latest['max_price_usd']:,.2f}")
    col3.metric("📉 Мин", f"${latest['min_price_usd']:,.2f}")
    
    volatility = (latest['max_price_usd'] - latest['min_price_usd']) / latest['avg_price_usd'] * 100
    col4.metric("🎢 Волатильность", f"{volatility:.1f}%")
    
    # Volume и Market Cap
    if 'volume_usd' in df.columns and latest.get('volume_usd', 0) > 0:
        col5, col6 = st.columns(2)
        col5.metric("📊 Объём торгов", f"${latest['volume_usd']:,.0f}")
        if latest.get('market_cap_usd', 0) > 0:
            col6.metric("💎 Market Cap", f"${latest['market_cap_usd']:,.0f}")


def render_price_chart(df: pd.DataFrame, coin_id: str, coin_name: str):
    color = CRYPTO_COLORS.get(coin_id, "#f7931a")
    chart_type = st.radio("Тип", ["Линейный", "Candlestick", "Область"], horizontal=True)
    
    if chart_type == "Candlestick":
        fig = go.Figure(data=[go.Candlestick(
            x=df['event_date'], open=df['open_price_usd'],
            high=df['max_price_usd'], low=df['min_price_usd'],
            close=df['close_price_usd'],
            increasing_line_color='#00ff88', decreasing_line_color='#ff4444'
        )])
    elif chart_type == "Область":
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['event_date'], y=df['max_price_usd'],
                                fill=None, mode='lines', line_color='rgba(0,255,136,0.3)', name='Max'))
        fig.add_trace(go.Scatter(x=df['event_date'], y=df['min_price_usd'],
                                fill='tonexty', mode='lines', fillcolor='rgba(247,147,26,0.2)', name='Min'))
        fig.add_trace(go.Scatter(x=df['event_date'], y=df['avg_price_usd'],
                                mode='lines', line=dict(color=color, width=2), name='Avg'))
    else:
        fig = px.line(df, x="event_date", y=["close_price_usd", "avg_price_usd"],
                     color_discrete_sequence=[color, "#888888"])
    
    fig.update_layout(title=f"{coin_name} - Курс", template="plotly_dark", height=450)
    st.plotly_chart(fig, use_container_width=True)


def render_predictions(predictions: Dict, coin_id: str):
    st.markdown("### 🔮 Прогнозы цены (XGBoost)")
    
    if predictions is None:
        st.info("Недостаточно данных для прогнозирования")
        return
    
    current = predictions['current_price']
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Сейчас", f"${current:,.2f}")
    
    for col, (h, label) in zip([col2, col3, col4], [('1d', 'Завтра'), ('7d', 'Неделя'), ('30d', 'Месяц')]):
        p = predictions.get(h)
        if p:
            change = p['change_pct']
            col.metric(label, f"${p['prediction']:,.2f}", f"{change:+.2f}%",
                      delta_color="normal" if change >= 0 else "inverse")


def render_data_table(df: pd.DataFrame):
    st.markdown("### 📋 Данные")
    display = df.copy()
    display['event_date'] = display['event_date'].dt.strftime('%Y-%m-%d')
    cols = ['event_date', 'open_price_usd', 'close_price_usd', 'avg_price_usd', 'max_price_usd', 'min_price_usd']
    if 'volume_usd' in display.columns:
        cols.append('volume_usd')
    st.dataframe(display[cols].sort_values('event_date', ascending=False), use_container_width=True, hide_index=True)
    
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 CSV", data=csv, file_name="crypto_data.csv", mime="text/csv")


def main():
    coin_id, coin_name, df = render_sidebar()
    render_header(coin_name)
    
    if df.empty:
        st.warning("⚠️ Нет данных. Запустите DAG crypto_etl в Airflow.")
        st.info("Airflow: http://localhost:8080 | Grafana: http://localhost:3000")
        return
    
    # Вкладки
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Аналитика", "🔮 ML Прогнозы", "🎯 Тренд", "🔗 Корреляция", "📋 Данные"])
    
    with tab1:
        c1, c2 = st.columns([3, 1])
        with c1:
            render_kpi(df, coin_id)
            st.markdown("---")
            render_price_chart(df, coin_id, coin_name)
        with c2:
            render_fear_greed()
    
    with tab2:
        predictions = get_predictions(df)
        render_predictions(predictions, coin_id)
    
    with tab3:
        render_trend_prediction(df)
    
    with tab4:
        render_correlation()
    
    with tab5:
        render_data_table(df)
    
    st.markdown("---")
    st.caption("🕐 Обновление: каждые 6 часов | Grafana: http://localhost:3000 | Airflow: http://localhost:8080")


if __name__ == "__main__":
    main()
