"""
ML Model Training Module.

This module provides:
- Random Forest and XGBoost classifiers for buy/sell signals
- Model training with time-series cross-validation
- Feature importance analysis
- Model persistence (save/load)

Usage:
    from ml_models.train import train_model, predict
    model = train_model(symbol="EVN.AX", model_type="random_forest")
    predictions = predict(model, data)
"""

import logging
import os
import pickle
from datetime import datetime
from typing import Any, Optional

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = "asx_stocks.duckdb"
MODELS_DIR = "ml_models"


def get_training_data(symbol: str = None, days: int = 365 * 3) -> pd.DataFrame:
    """Load training data from DuckDB."""
    try:
        con = duckdb.connect(DUCKDB_PATH)

        if symbol:
            query = f"""
                SELECT * FROM analytics.mart_ml_training_dataset
                WHERE symbol = '{symbol}'
                AND price_date >= CURRENT_DATE - INTERVAL '{days} days'
                ORDER BY symbol, price_date
            """
        else:
            query = f"""
                SELECT * FROM analytics.mart_ml_training_dataset
                WHERE price_date >= CURRENT_DATE - INTERVAL '{days} days'
                ORDER BY symbol, price_date
            """

        df = con.execute(query).df()
        con.close()
        return df

    except Exception as e:
        logger.error(f"Error loading training data: {e}")
        return pd.DataFrame()


def prepare_features(df: pd.DataFrame) -> tuple[list[str], pd.DataFrame]:
    """Prepare feature columns for ML model."""
    feature_cols = [
        # Price
        "daily_range",
        # SMAs
        "sma_7",
        "sma_14",
        "sma_30",
        "sma_50",
        "sma_200",
        # RSI
        "rsi_14",
        # MACD
        "macd_line",
        "macd_signal",
        "macd_histogram",
        # Bollinger Bands
        "bb_position",
        # ATR
        "atr_14",
        # Volume
        "volume_ratio",
        "obv",
        # Volatility
        "volatility_20",
        # Lag features
        "lag_1_day_return",
        "lag_3_day_return",
        "lag_5_day_return",
        "lag_7_day_return",
        # Momentum
        "momentum_7",
        "momentum_14",
        "momentum_30",
        "momentum_1",
        # Cross signals
        "ma_crossover_distance",
        "short_term_trend",
        # Support/Resistance
        "price_position_20",
        "price_position_50",
        # Sector
        "sector_code",
    ]

    # Filter to existing columns
    available_features = [col for col in feature_cols if col in df.columns]

    return available_features, df


def create_binary_target(df: pd.DataFrame, threshold: float = 1.0) -> pd.Series:
    """
    Create binary target: 1 = BUY (return > threshold), 0 = SELL/HOLD.

    Args:
        df: DataFrame with next_day_return
        threshold: Minimum return percentage to consider as BUY signal
    """
    return (df["next_day_return"] > threshold).astype(int)


def train_random_forest(
    X: pd.DataFrame,
    y: pd.Series,
    test_size: float = 0.2,
    random_state: int = 42,
    **kwargs,
) -> tuple[Any, dict]:
    """
    Train Random Forest classifier.

    Returns:
        tuple: (model, metrics)
    """
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import (
        accuracy_score,
        classification_report,
        f1_score,
        precision_score,
        recall_score,
    )
    from sklearn.model_selection import train_test_split

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, shuffle=False
    )

    # Default params
    params = {
        "n_estimators": 100,
        "max_depth": 10,
        "min_samples_split": 10,
        "min_samples_leaf": 5,
        "random_state": random_state,
        "n_jobs": -1,
    }
    params.update(kwargs)

    # Train model
    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
    }

    logger.info(f"Random Forest trained. Accuracy: {metrics['accuracy']:.3f}")

    return model, metrics


def train_xgboost(
    X: pd.DataFrame,
    y: pd.Series,
    test_size: float = 0.2,
    random_state: int = 42,
    **kwargs,
) -> tuple[Any, dict]:
    """
    Train XGBoost classifier.

    Returns:
        tuple: (model, metrics)
    """
    from sklearn.metrics import (
        accuracy_score,
        classification_report,
        f1_score,
        precision_score,
        recall_score,
    )
    from sklearn.model_selection import train_test_split

    try:
        import xgboost as xgb
    except ImportError:
        logger.warning("XGBoost not installed, falling back to Random Forest")
        return train_random_forest(X, y, test_size, random_state, **kwargs)

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, shuffle=False
    )

    # Default params
    params = {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "random_state": random_state,
        "use_label_encoder": False,
        "eval_metric": "logloss",
    }
    params.update(kwargs)

    # Train model
    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
    }

    logger.info(f"XGBoost trained. Accuracy: {metrics['accuracy']:.3f}")

    return model, metrics


def train_model(
    symbol: str = None,
    model_type: str = "random_forest",
    threshold: float = 1.0,
    save_model: bool = True,
    **kwargs,
) -> dict:
    """
    Train an ML model for stock prediction.

    Args:
        symbol: Specific stock to train on, or None for all
        model_type: "random_forest" or "xgboost"
        threshold: Return threshold for BUY signal
        save_model: Whether to save model to disk
        **kwargs: Additional model parameters

    Returns:
        dict: {model, metrics, feature_importance, symbol, model_type}
    """
    logger.info(f"Training {model_type} for {symbol or 'all stocks'}")

    # Load data
    df = get_training_data(symbol)
    if df.empty:
        raise ValueError("No training data available")

    # Prepare features
    feature_cols, df = prepare_features(df)

    if len(df) < 100:
        raise ValueError("Not enough training data (need 100+ rows)")

    # Create target
    y = create_binary_target(df, threshold)

    # Filter valid rows
    X = df[feature_cols].copy()
    valid_mask = ~X.isnull().any(axis=1) & ~y.isnull()
    X = X[valid_mask]
    y = y[valid_mask]

    if len(X) < 50:
        raise ValueError("Not enough valid data after filtering")

    # Handle any remaining NaN
    X = X.fillna(0)

    # Train model
    if model_type == "xgboost":
        model, metrics = train_xgboost(X, y, **kwargs)
    else:
        model, metrics = train_random_forest(X, y, **kwargs)

    # Get feature importance
    feature_importance = dict(zip(feature_cols, model.feature_importances_))
    feature_importance = dict(sorted(feature_importance.items(), key=lambda x: x[1], reverse=True))

    result = {
        "model": model,
        "metrics": metrics,
        "feature_importance": feature_importance,
        "symbol": symbol,
        "model_type": model_type,
        "threshold": threshold,
        "feature_cols": feature_cols,
    }

    # Save model
    if save_model:
        save_path = save_model_to_disk(result)
        result["save_path"] = save_path
        logger.info(f"Model saved to {save_path}")

    return result


def predict(model: Any, X: pd.DataFrame) -> np.ndarray:
    """Make predictions with trained model."""
    X = X.fillna(0)
    return model.predict(X)


def predict_proba(model: Any, X: pd.DataFrame) -> np.ndarray:
    """Get prediction probabilities."""
    X = X.fillna(0)
    return model.predict_proba(X)


def save_model_to_disk(model_info: dict) -> str:
    """Save model to disk."""
    os.makedirs(MODELS_DIR, exist_ok=True)

    symbol = model_info.get("symbol", "all")
    model_type = model_info.get("model_type", "rf")
    timestamp = datetime.now().strftime("%Y%m%d")

    filename = f"{symbol}_{model_type}_{timestamp}.pkl"
    filepath = os.path.join(MODELS_DIR, filename)

    with open(filepath, "wb") as f:
        pickle.dump(model_info, f)

    return filepath


def load_model_from_disk(filepath: str) -> dict:
    """Load model from disk."""
    with open(filepath, "rb") as f:
        return pickle.load(f)


def get_latest_model(symbol: str = None) -> Optional[dict]:
    """Load the latest saved model for a symbol."""
    if not os.path.exists(MODELS_DIR):
        return None

    pattern = f"{symbol or 'all'}_*.pkl" if symbol else "*.pkl"
    files = [f for f in os.listdir(MODELS_DIR) if f.endswith(".pkl")]

    if not files:
        return None

    # Sort by modification time
    files.sort(key=lambda f: os.path.getmtime(os.path.join(MODELS_DIR, f)), reverse=True)

    filepath = os.path.join(MODELS_DIR, files[0])
    return load_model_from_disk(filepath)


def get_feature_importance_chart(feature_importance: dict) -> go.Figure:
    """Create feature importance bar chart."""
    import plotly.graph_objects as go

    # Get top 15 features
    top_features = dict(list(feature_importance.items())[:15])

    fig = go.Figure(
        go.Bar(
            x=list(top_features.values()),
            y=list(top_features.keys()),
            orientation="h",
            marker=dict(color="#00FFFF"),
        )
    )

    fig.update_layout(
        title="Top 15 Feature Importance",
        xaxis_title="Importance",
        yaxis_title="Feature",
        height=400,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        yaxis=dict(autorange="reversed"),
    )

    return fig


def cross_validate(
    symbol: str = None,
    model_type: str = "random_forest",
    n_folds: int = 5,
) -> dict:
    """
    Perform time-series cross-validation.

    Returns:
        dict: {fold_metrics, average_metrics}
    """
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

    # Load data
    df = get_training_data(symbol)
    if df.empty:
        raise ValueError("No training data available")

    # Prepare features
    feature_cols, df = prepare_features(df)
    y = create_binary_target(df)

    # Filter valid rows
    X = df[feature_cols].copy()
    valid_mask = ~X.isnull().any(axis=1) & ~y.isnull()
    X = X[valid_mask]
    y = y[valid_mask]
    X = X.fillna(0)

    # Time-series split manually
    n_samples = len(X)
    fold_size = n_samples // (n_folds + 1)

    fold_metrics = []

    for fold in range(n_folds):
        # Use earlier data for training, later for testing
        train_end = (fold + 1) * fold_size
        test_start = train_end
        test_end = test_start + fold_size

        if test_end > n_samples:
            break

        X_train = X.iloc[:train_end]
        X_test = X.iloc[test_start:test_end]
        y_train = y.iloc[:train_end]
        y_test = y.iloc[test_start:test_end]

        if len(X_train) < 50 or len(X_test) < 10:
            continue

        # Train
        if model_type == "xgboost":
            model, _ = train_xgboost(X_train, y_train)
        else:
            model, _ = train_random_forest(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        metrics = {
            "fold": fold + 1,
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, zero_division=0),
            "recall": recall_score(y_test, y_pred, zero_division=0),
            "f1": f1_score(y_test, y_pred, zero_division=0),
        }
        fold_metrics.append(metrics)

    # Calculate averages
    avg_metrics = {
        "accuracy": np.mean([m["accuracy"] for m in fold_metrics]),
        "precision": np.mean([m["precision"] for m in fold_metrics]),
        "recall": np.mean([m["recall"] for m in fold_metrics]),
        "f1": np.mean([m["f1"] for m in fold_metrics]),
    }

    return {"fold_metrics": fold_metrics, "average_metrics": avg_metrics}
