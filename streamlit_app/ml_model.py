"""
ML модели для прогнозирования цен и трендов криптовалют
- XGBoost регрессия для предсказания цены
- XGBoost классификация для предсказания тренда (UP/DOWN/FLAT)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score, 
    classification_report,
    mean_absolute_error, 
    mean_squared_error, 
    r2_score,
    confusion_matrix
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, LabelEncoder
import xgboost as xgb

logger = logging.getLogger(__name__)


class CryptoPricePredictor:
    """Модель регрессии для предсказания цены"""
    
    def __init__(self, lookback_days: int = 30):
        self.lookback_days = lookback_days
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = []
        self.is_trained = False
        self.metrics = {}
        
    def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создание признаков для ML модели"""
        df = df.copy()
        df = df.sort_values('event_date').reset_index(drop=True)
        
        price_col = 'close_price_usd'
        
        # Лаговые признаки
        for lag in [1, 2, 3, 5, 7, 14, 21, 30]:
            if lag <= self.lookback_days:
                df[f'price_lag_{lag}'] = df[price_col].shift(lag)
        
        # Скользящие средние
        for window in [3, 7, 14, 21, 30]:
            df[f'ma_{window}'] = df[price_col].rolling(window=window).mean()
            df[f'ma_{window}_ratio'] = df[price_col] / df[f'ma_{window}']
        
        # Волатильность
        for window in [7, 14, 30]:
            df[f'volatility_{window}'] = df[price_col].rolling(window=window).std()
        
        # Returns
        df['daily_return'] = df[price_col].pct_change()
        df['return_3d'] = df[price_col].pct_change(periods=3)
        df['return_7d'] = df[price_col].pct_change(periods=7)
        df['return_14d'] = df[price_col].pct_change(periods=14)
        df['return_30d'] = df[price_col].pct_change(periods=30)
        
        # Min/Max за период
        df['min_7d'] = df[price_col].rolling(window=7).min()
        df['max_7d'] = df[price_col].rolling(window=7).max()
        df['range_7d'] = df['max_7d'] - df['min_7d']
        df['range_7d_pct'] = df['range_7d'] / df[price_col]
        
        # RSI
        delta = df[price_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        # Временные признаки
        df['day_of_week'] = pd.to_datetime(df['event_date']).dt.dayofweek
        df['month'] = pd.to_datetime(df['event_date']).dt.month
        df['day_of_month'] = pd.to_datetime(df['event_date']).dt.day
        
        # Тренд
        df['trend'] = df['ma_7'] - df['ma_30']
        df['trend_strength'] = df['trend'] / df['ma_30']
        
        # Volume и Market Cap если есть
        if 'volume_usd' in df.columns:
            df['volume_ma_7'] = df['volume_usd'].rolling(window=7).mean()
            df['volume_ratio'] = df['volume_usd'] / df['volume_ma_7']
        
        if 'market_cap_usd' in df.columns:
            df['mcap_change'] = df['market_cap_usd'].pct_change()
        
        # Целевые переменные
        df['target_1d'] = df[price_col].shift(-1)
        df['target_7d'] = df[price_col].shift(-7)
        df['target_30d'] = df[price_col].shift(-30)
        
        return df
    
    def prepare_data(self, df: pd.DataFrame, target_days: int = 1) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        target_col = f'target_{target_days}d'
        exclude_cols = ['event_date', 'target_1d', 'target_7d', 'target_30d', 
                       'processed_at', 'coin_id', 'coin_symbol', 'coin_name', 'id']
        
        feature_cols = [c for c in df.columns if c not in exclude_cols 
                       and not c.startswith('target_')]
        
        df_clean = df.dropna(subset=feature_cols + [target_col])
        
        if len(df_clean) < 50:
            raise ValueError(f"Недостаточно данных: {len(df_clean)} строк")
        
        X = df_clean[feature_cols].values
        y = df_clean[target_col].values
        
        return X, y, feature_cols
    
    def train(self, df: pd.DataFrame, target_days: int = 1, test_size: float = 0.2) -> Dict:
        df_features = self.create_features(df)
        X, y, feature_cols = self.prepare_data(df_features, target_days)
        self.feature_columns = feature_cols
        
        split_idx = int(len(X) * (1 - test_size))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        params = {
            'objective': 'reg:squarederror',
            'max_depth': 6,
            'learning_rate': 0.05,
            'n_estimators': 200,
            'min_child_weight': 3,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'n_jobs': -1
        }
        
        self.model = xgb.XGBRegressor(**params)
        self.model.fit(X_train_scaled, y_train, eval_set=[(X_test_scaled, y_test)], verbose=False)
        
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)
        
        self.metrics = {
            'train_mae': mean_absolute_error(y_train, y_pred_train),
            'train_rmse': np.sqrt(mean_squared_error(y_train, y_pred_train)),
            'train_r2': r2_score(y_train, y_pred_train),
            'test_mae': mean_absolute_error(y_test, y_pred_test),
            'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
            'test_r2': r2_score(y_test, y_pred_test),
            'test_mape': np.mean(np.abs((y_test - y_pred_test) / y_test)) * 100,
        }
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        self.metrics['feature_importance'] = dict(
            sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
        )
        
        self.is_trained = True
        return self.metrics
    
    def predict(self, df: pd.DataFrame) -> Tuple[float, float, float]:
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        df_features = self.create_features(df)
        last_row = df_features.iloc[-1:][self.feature_columns]
        
        if last_row.isnull().any().any():
            last_row = last_row.fillna(df_features[self.feature_columns].mean())
        
        X_scaled = self.scaler.transform(last_row.values)
        prediction = self.model.predict(X_scaled)[0]
        current_price = df['close_price_usd'].iloc[-1]
        confidence = self.metrics.get('test_rmse', prediction * 0.05)
        
        return current_price, prediction, confidence
    
    def predict_multiple_horizons(self, df: pd.DataFrame) -> Dict:
        results = {}
        current_price = df['close_price_usd'].iloc[-1]
        
        for horizon in [1, 7, 30]:
            try:
                model = CryptoPricePredictor(lookback_days=30)
                model.train(df, target_days=horizon)
                _, pred, conf = model.predict(df)
                change_pct = ((pred - current_price) / current_price) * 100
                
                results[f'{horizon}d'] = {
                    'prediction': pred,
                    'confidence': conf,
                    'change_pct': change_pct,
                    'metrics': model.metrics
                }
            except Exception as e:
                logger.error(f"Ошибка прогноза на {horizon} дней: {e}")
                results[f'{horizon}d'] = None
        
        results['current_price'] = current_price
        return results


class TrendClassifier:
    """Модель классификации тренда: UP / DOWN / FLAT"""
    
    def __init__(self, threshold_pct: float = 2.0):
        """
        Args:
            threshold_pct: Порог изменения цены для определения тренда (%)
                          Если изменение > threshold_pct -> UP
                          Если изменение < -threshold_pct -> DOWN
                          Иначе -> FLAT
        """
        self.threshold_pct = threshold_pct
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.feature_columns = []
        self.is_trained = False
        self.metrics = {}
        self.classes = ['DOWN', 'FLAT', 'UP']
    
    def _create_trend_label(self, current_price: float, future_price: float) -> str:
        """Определение класса тренда"""
        change_pct = ((future_price - current_price) / current_price) * 100
        if change_pct > self.threshold_pct:
            return 'UP'
        elif change_pct < -self.threshold_pct:
            return 'DOWN'
        else:
            return 'FLAT'
    
    def create_features(self, df: pd.DataFrame, horizon_days: int = 7) -> pd.DataFrame:
        """Создание признаков с метками тренда"""
        df = df.copy()
        df = df.sort_values('event_date').reset_index(drop=True)
        
        price_col = 'close_price_usd'
        
        # Лаговые признаки
        for lag in [1, 2, 3, 5, 7, 14, 21, 30]:
            df[f'price_lag_{lag}'] = df[price_col].shift(lag)
        
        # Скользящие средние
        for window in [3, 7, 14, 21, 30]:
            df[f'ma_{window}'] = df[price_col].rolling(window=window).mean()
            df[f'ma_{window}_ratio'] = df[price_col] / df[f'ma_{window}']
        
        # Волатильность
        for window in [7, 14, 30]:
            df[f'volatility_{window}'] = df[price_col].rolling(window=window).std()
            df[f'volatility_{window}_pct'] = df[f'volatility_{window}'] / df[price_col] * 100
        
        # Returns
        df['daily_return'] = df[price_col].pct_change() * 100
        df['return_3d'] = df[price_col].pct_change(periods=3) * 100
        df['return_7d'] = df[price_col].pct_change(periods=7) * 100
        df['return_14d'] = df[price_col].pct_change(periods=14) * 100
        
        # Momentum
        df['momentum_7d'] = df[price_col] - df[price_col].shift(7)
        df['momentum_14d'] = df[price_col] - df[price_col].shift(14)
        
        # RSI
        delta = df[price_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        df['bb_middle'] = df[price_col].rolling(window=20).mean()
        df['bb_std'] = df[price_col].rolling(window=20).std()
        df['bb_upper'] = df['bb_middle'] + 2 * df['bb_std']
        df['bb_lower'] = df['bb_middle'] - 2 * df['bb_std']
        df['bb_position'] = (df[price_col] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
        
        # Тренд
        df['trend_7_21'] = df['ma_7'] - df['ma_21']
        df['trend_7_30'] = df['ma_7'] - df['ma_30']
        df['trend_strength'] = df['trend_7_30'] / df['ma_30'] * 100
        
        # Volume если есть
        if 'volume_usd' in df.columns:
            df['volume_ma_7'] = df['volume_usd'].rolling(window=7).mean()
            df['volume_ratio'] = df['volume_usd'] / df['volume_ma_7']
            df['volume_change'] = df['volume_usd'].pct_change() * 100
        
        # Временные признаки
        df['day_of_week'] = pd.to_datetime(df['event_date']).dt.dayofweek
        df['month'] = pd.to_datetime(df['event_date']).dt.month
        
        # Целевая переменная - тренд через N дней
        df['future_price'] = df[price_col].shift(-horizon_days)
        df['trend_label'] = df.apply(
            lambda row: self._create_trend_label(row[price_col], row['future_price']) 
            if pd.notna(row['future_price']) else None, 
            axis=1
        )
        
        return df
    
    def train(self, df: pd.DataFrame, horizon_days: int = 7, test_size: float = 0.2) -> Dict:
        """Обучение классификатора тренда"""
        df_features = self.create_features(df, horizon_days)
        
        exclude_cols = ['event_date', 'future_price', 'trend_label', 
                       'processed_at', 'coin_id', 'coin_symbol', 'coin_name', 'id',
                       'bb_std', 'bb_middle', 'bb_upper', 'bb_lower']
        
        feature_cols = [c for c in df_features.columns if c not in exclude_cols]
        self.feature_columns = feature_cols
        
        df_clean = df_features.dropna(subset=feature_cols + ['trend_label'])
        
        if len(df_clean) < 50:
            raise ValueError(f"Недостаточно данных: {len(df_clean)} строк")
        
        X = df_clean[feature_cols].values
        y = df_clean['trend_label'].values
        
        # Encode labels
        y_encoded = self.label_encoder.fit_transform(y)
        
        split_idx = int(len(X) * (1 - test_size))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y_encoded[:split_idx], y_encoded[split_idx:]
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        params = {
            'objective': 'multi:softprob',
            'num_class': 3,
            'max_depth': 5,
            'learning_rate': 0.05,
            'n_estimators': 150,
            'min_child_weight': 3,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42,
            'n_jobs': -1
        }
        
        self.model = xgb.XGBClassifier(**params)
        self.model.fit(X_train_scaled, y_train, eval_set=[(X_test_scaled, y_test)], verbose=False)
        
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)
        y_pred_proba = self.model.predict_proba(X_test_scaled)
        
        # Метрики
        self.metrics = {
            'train_accuracy': accuracy_score(y_train, y_pred_train),
            'test_accuracy': accuracy_score(y_test, y_pred_test),
            'confusion_matrix': confusion_matrix(y_test, y_pred_test).tolist(),
            'classes': self.label_encoder.classes_.tolist(),
        }
        
        # Распределение классов
        unique, counts = np.unique(y_test, return_counts=True)
        self.metrics['class_distribution'] = {
            self.label_encoder.classes_[i]: int(c) 
            for i, c in zip(unique, counts)
        }
        
        # Feature importance
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        self.metrics['feature_importance'] = dict(
            sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
        )
        
        self.is_trained = True
        logger.info(f"Классификатор обучен. Accuracy: {self.metrics['test_accuracy']:.2%}")
        
        return self.metrics
    
    def predict(self, df: pd.DataFrame, horizon_days: int = 7) -> Dict:
        """Предсказание тренда"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        df_features = self.create_features(df, horizon_days)
        last_row = df_features.iloc[-1:][self.feature_columns]
        
        if last_row.isnull().any().any():
            last_row = last_row.fillna(df_features[self.feature_columns].mean())
        
        X_scaled = self.scaler.transform(last_row.values)
        
        pred_class = self.model.predict(X_scaled)[0]
        pred_proba = self.model.predict_proba(X_scaled)[0]
        
        trend = self.label_encoder.classes_[pred_class]
        
        return {
            'trend': trend,
            'confidence': float(max(pred_proba)),
            'probabilities': {
                self.label_encoder.classes_[i]: float(p) 
                for i, p in enumerate(pred_proba)
            },
            'current_price': float(df['close_price_usd'].iloc[-1])
        }


def predict_trend(df: pd.DataFrame, horizon_days: int = 7, threshold_pct: float = 2.0) -> Dict:
    """Удобная функция для предсказания тренда"""
    classifier = TrendClassifier(threshold_pct=threshold_pct)
    metrics = classifier.train(df, horizon_days=horizon_days)
    prediction = classifier.predict(df, horizon_days)
    prediction['metrics'] = metrics
    return prediction


def train_and_predict(df: pd.DataFrame, target_days: int = 30) -> Dict:
    """Удобная функция для обучения и прогноза цены"""
    predictor = CryptoPricePredictor(lookback_days=30)
    metrics = predictor.train(df, target_days=target_days)
    current_price, prediction, confidence = predictor.predict(df)
    
    return {
        'current_price': current_price,
        'prediction': prediction,
        'confidence': confidence,
        'change_pct': ((prediction - current_price) / current_price) * 100,
        'metrics': metrics
    }
