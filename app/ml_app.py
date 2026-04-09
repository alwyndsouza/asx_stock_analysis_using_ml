"""
ML Model Training Streamlit Application.

This app provides:
- Model training with Random Forest and XGBoost
- Cross-validation results
- Feature importance visualization
- Model predictions

Usage:
    streamlit run app/ml_app.py
"""

import logging
from datetime import datetime

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from ml_models.train import (
    cross_validate,
    get_feature_importance_chart,
    get_latest_model,
    get_training_data,
    load_model_from_disk,
    prepare_features,
    predict,
    predict_proba,
    train_model,
)

logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = "asx_stocks.duckdb"

# ASX Stocks
ASX_STOCKS = {
    "EVN.AX": "Evolution Mining (Gold)",
    "NST.AX": "Northern Star (Gold)",
    "RRL.AX": "Rex (Gold)",
    "SBM.AX": "St Barbara (Gold)",
    "WDS.AX": "Woodside Energy (Oil)",
    "STO.AX": "Santos (Oil)",
    "BPT.AX": "Beach Energy (Oil)",
    "KAR.AX": "Karoon Energy (Oil)",
    "SVL.AX": "Silver Lake Resources (Silver)",
    "S32.AX": "South32 (Silver)",
}


@st.cache_data(ttl=3600)
def load_training_data(symbol: str = None, days: int = 365 * 3) -> pd.DataFrame:
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


def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="ASX ML Model Training",
        page_icon="🤖",
        layout="wide",
    )

    st.title("🤖 ASX Stock ML Model Training")

    # Sidebar
    st.sidebar.header("Configuration")

    # Tab selection
    tab = st.sidebar.radio(
        "Select Tab",
        ["Train Model", "Cross-Validate", "Model Info", "Predictions"],
    )

    # Stock selector
    selected_stock = st.sidebar.selectbox(
        "Select Stock",
        options=["All Stocks"] + list(ASX_STOCKS.keys()),
        format_func=lambda x: x if x == "All Stocks" else f"{x} - {ASX_STOCKS.get(x, x)}",
    )

    symbol = None if selected_stock == "All Stocks" else selected_stock

    if tab == "Train Model":
        st.subheader("Train ML Model")

        col1, col2 = st.columns(2)

        with col1:
            model_type = st.selectbox(
                "Model Type",
                ["random_forest", "xgboost"],
                format_func=lambda x: x.replace("_", " ").title(),
            )

            threshold = st.slider(
                "Buy Signal Threshold (% return)",
                min_value=0.5,
                max_value=5.0,
                value=1.0,
                step=0.5,
            )

        with col2:
            n_estimators = st.slider("Number of Trees", 50, 200, 100)
            max_depth = st.slider("Max Depth", 3, 20, 10)

        train_button = st.button("Train Model", type="primary")

        if train_button:
            with st.spinner("Training model..."):
                try:
                    result = train_model(
                        symbol=symbol,
                        model_type=model_type,
                        threshold=threshold,
                        n_estimators=n_estimators,
                        max_depth=max_depth,
                    )

                    # Show metrics
                    st.success("Model trained successfully!")

                    m = result["metrics"]
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Accuracy", f"{m['accuracy']:.2%}")
                    with col2:
                        st.metric("Precision", f"{m['precision']:.2%}")
                    with col3:
                        st.metric("Recall", f"{m['recall']:.2%}")
                    with col4:
                        st.metric("F1 Score", f"{m['f1']:.2%}")

                    # Feature importance
                    st.subheader("Feature Importance")
                    fig = get_feature_importance_chart(result["feature_importance"])
                    st.plotly_chart(fig, use_container_width=True)

                    st.info(f"Model saved to: {result.get('save_path', 'N/A')}")

                except Exception as e:
                    st.error(f"Training failed: {e}")

    elif tab == "Cross-Validate":
        st.subheader("Time-Series Cross-Validation")

        cv_folds = st.slider("Number of Folds", 3, 10, 5)

        cv_button = st.button("Run Cross-Validation")

        if cv_button:
            with st.spinner("Running cross-validation..."):
                try:
                    result = cross_validate(symbol=symbol, n_folds=cv_folds)

                    # Show fold metrics
                    st.subheader("Fold Results")
                    fold_df = pd.DataFrame(result["fold_metrics"])
                    st.dataframe(fold_df, use_container_width=True)

                    # Show averages
                    st.subheader("Average Metrics")
                    avg = result["average_metrics"]

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Accuracy", f"{avg['accuracy']:.2%}")
                    with col2:
                        st.metric("Precision", f"{avg['precision']:.2%}")
                    with col3:
                        st.metric("Recall", f"{avg['recall']:.2%}")
                    with col4:
                        st.metric("F1 Score", f"{avg['f1']:.2%}")

                except Exception as e:
                    st.error(f"Cross-validation failed: {e}")

    elif tab == "Model Info":
        st.subheader("Saved Models")

        # Load latest model
        model_info = get_latest_model(symbol)

        if model_info:
            st.success("Found saved model!")

            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Model Type:** {model_info.get('model_type')}")
                st.write(f"**Symbol:** {model_info.get('symbol')}")
                st.write(f"**Threshold:** {model_info.get('threshold')}%")
            with col2:
                st.write(f"**Features:** {len(model_info.get('feature_cols', []))}")
                st.write(f"**Save Path:** {model_info.get('save_path')}")

            # Show metrics
            m = model_info.get("metrics", {})
            if m:
                st.subheader("Model Metrics")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Accuracy", f"{m.get('accuracy', 0):.2%}")
                with col2:
                    st.metric("Precision", f"{m.get('precision', 0):.2%}")
                with col3:
                    st.metric("Recall", f"{m.get('recall', 0):.2%}")
                with col4:
                    st.metric("F1 Score", f"{m.get('f1', 0):.2%}")

            # Feature importance
            fi = model_info.get("feature_importance", {})
            if fi:
                st.subheader("Feature Importance")
                fig = get_feature_importance_chart(fi)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No saved models found. Train a model first.")

    elif tab == "Predictions":
        st.subheader("Model Predictions")

        # Load model
        model_info = get_latest_model(symbol)

        if not model_info:
            st.error("No trained model found. Please train a model first.")
            return

        model = model_info.get("model")
        feature_cols = model_info.get("feature_cols", [])

        # Load data
        df = load_training_data(symbol, days=365)

        if df.empty:
            st.error("No data available")
            return

        # Prepare features
        _, df = prepare_features(df)
        X = df[feature_cols].fillna(0)

        # Make predictions
        predictions = predict(model, X)
        probabilities = predict_proba(model, X)

        # Add to dataframe
        result_df = df[["symbol", "price_date", "close_price", "next_day_return"]].copy()
        result_df["prediction"] = predictions
        result_df["buy_probability"] = (
            probabilities[:, 1] if probabilities.shape[1] > 1 else probabilities
        )

        # Show latest predictions
        st.subheader("Latest Predictions")
        st.dataframe(
            result_df.tail(20).style.format(
                {
                    "close_price": "${:.2f}",
                    "next_day_return": "{:+.2f}%",
                    "buy_probability": "{:.2%}",
                }
            ),
            use_container_width=True,
        )

        # Summary
        st.subheader("Prediction Summary")
        buy_count = (predictions == 1).sum()
        sell_count = (predictions == 0).sum()

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Buy Signals", buy_count)
        with col2:
            st.metric("Sell/Hold Signals", sell_count)

    # Footer
    st.markdown("---")
    st.caption(f"Data last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("Disclaimer: This is for educational purposes only. Not financial advice.")


if __name__ == "__main__":
    main()
