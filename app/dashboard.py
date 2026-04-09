"""
ASX Stock Analysis Streamlit Application.

This app provides:
- Real-time stock data visualization
- Technical indicators (RSI, MACD, Bollinger Bands, Fibonacci)
- Buy/Sell signal generation
- Price forecasting (Prophet)
- Target price recommendations

Usage:
    streamlit run app/dashboard.py
"""

import logging
from datetime import datetime, timedelta
from typing import Any

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# Try to import Prophet, provide fallback if not available
PROPHET_AVAILABLE = False
PROPHET_ERROR = None

try:
    from prophet import Prophet

    PROPHET_AVAILABLE = True
except ImportError as e:
    PROPHET_ERROR = str(e)

# Configuration
DUCKDB_PATH = "asx_stocks.duckdb"
DATASET_NAME = "raw_asx_data"

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

logger = logging.getLogger(__name__)


@st.cache_data(ttl=3600)
def load_stock_data(symbol: str, days: int = 365) -> pd.DataFrame:
    """Load stock data from DuckDB."""
    try:
        con = duckdb.connect(DUCKDB_PATH)
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        query = f"""
            SELECT 
                date,
                symbol,
                sector,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            FROM {DATASET_NAME}.asx_stock_prices
            WHERE symbol = '{symbol}'
            AND date >= '{start_date}'
            ORDER BY date
        """

        df = con.execute(query).df()
        con.close()

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")

        return df

    except Exception as e:
        logger.error(f"Error loading data for {symbol}: {e}")
        return pd.DataFrame()


def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI indicator."""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_macd(prices: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate MACD indicator."""
    ema_12 = prices.ewm(span=12, adjust=False).mean()
    ema_26 = prices.ewm(span=26, adjust=False).mean()
    macd_line = ema_12 - ema_26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    return macd_line, signal_line, histogram


def calculate_bollinger_bands(prices: pd.Series, period: int = 20) -> tuple[pd.Series, pd.Series]:
    """Calculate Bollinger Bands."""
    sma = prices.rolling(window=period).mean()
    std = prices.rolling(window=period).std()
    upper = sma + (2 * std)
    lower = sma - (2 * std)

    return upper, lower


def calculate_obv(prices: pd.Series, volumes: pd.Series) -> pd.Series:
    """Calculate On-Balance Volume."""
    obv = [0]
    for i in range(1, len(prices)):
        if prices.iloc[i] > prices.iloc[i - 1]:
            obv.append(obv[-1] + volumes.iloc[i])
        elif prices.iloc[i] < prices.iloc[i - 1]:
            obv.append(obv[-1] - volumes.iloc[i])
        else:
            obv.append(obv[-1])

    return pd.Series(obv, index=prices.index)


def calculate_fibonacci_retracement(high: float, low: float) -> dict:
    """Calculate Fibonacci retracement levels."""
    diff = high - low
    levels = {
        "0.0% (Low)": low,
        "23.6%": low + (diff * 0.236),
        "38.2%": low + (diff * 0.382),
        "50.0%": low + (diff * 0.5),
        "61.8%": low + (diff * 0.618),
        "78.6%": low + (diff * 0.786),
        "100% (High)": high,
    }
    return levels


def generate_signals(df: pd.DataFrame) -> dict:
    """Generate buy/sell signals based on multiple indicators."""
    signals = {
        "rsi_buy": False,
        "rsi_sell": False,
        "macd_buy": False,
        "macd_sell": False,
        "bb_buy": False,
        "bb_sell": False,
        "obv_buy": False,
        "obv_sell": False,
    }

    if df.empty or len(df) < 30:
        return signals

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # RSI Signals
    if "rsi" in df.columns:
        if latest["rsi"] < 30:
            signals["rsi_buy"] = True
        elif latest["rsi"] > 70:
            signals["rsi_sell"] = True

    # MACD Signals
    if "macd" in df.columns and "macd_signal" in df.columns:
        if latest["macd"] > latest["macd_signal"] and prev["macd"] <= prev["macd_signal"]:
            signals["macd_buy"] = True
        elif latest["macd"] < latest["macd_signal"] and prev["macd"] >= prev["macd_signal"]:
            signals["macd_sell"] = True

    # Bollinger Band Signals
    if "bb_lower" in df.columns and "bb_upper" in df.columns:
        if latest["close_price"] < latest["bb_lower"]:
            signals["bb_buy"] = True
        elif latest["close_price"] > latest["bb_upper"]:
            signals["bb_sell"] = True

    # OBV Signals (if we have OBV data)
    if "obv" in df.columns and len(df) > 20:
        obv_slope = (df["obv"].iloc[-1] - df["obv"].iloc[-20]) / 20
        if obv_slope > 0:
            signals["obv_buy"] = True
        elif obv_slope < 0:
            signals["obv_sell"] = True

    return signals


def calculate_target_price(df: pd.DataFrame, method: str = "fibonacci") -> dict:
    """Calculate target price based on different methods."""
    if df.empty:
        return {}

    latest = df.iloc[-1]
    current_price = latest["close_price"]

    targets = {
        "current_price": current_price,
    }

    if method == "fibonacci" and len(df) >= 30:
        # Use last 30 days for Fibonacci
        high_30 = df["high_price"].tail(30).max()
        low_30 = df["low_price"].tail(30).min()
        fib_levels = calculate_fibonacci_retracement(high_30, low_30)

        targets["fib_38"] = fib_levels["38.2%"]
        targets["fib_61"] = fib_levels["61.8%"]
        targets["fib_78"] = fib_levels["78.6%"]
        targets["fib_high"] = fib_levels["100% (High)"]

        # Support/Resistance based on recent range
        targets["support"] = low_30
        targets["resistance"] = high_30

    # Simple moving average targets
    if "sma_20" in df.columns:
        targets["sma_20"] = latest["sma_20"]
    if "sma_50" in df.columns:
        targets["sma_50"] = latest["sma_50"]

    # Calculate expected move based on ATR if available
    if "atr" in df.columns:
        atr = latest.get("atr", 0)
        targets["upside_target"] = current_price + (atr * 2)
        targets["downside_target"] = current_price - (atr * 2)

    return targets


def create_candlestick_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Create candlestick chart with technical indicators."""
    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(f"{symbol} - Price Action", "RSI (14)", "MACD", "OBV"),
        row_heights=[0.5, 0.15, 0.15, 0.15],
    )

    # Candlestick - use bright colors for dark mode visibility
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="OHLC",
            increasing_line_color="#00FF00",  # Bright green for up
            decreasing_line_color="#FF4444",  # Bright red for down
            increasing_fillcolor="#00CC00",  # Green fill
            decreasing_fillcolor="#CC0000",  # Red fill
        ),
        row=1,
        col=1,
    )

    # Bollinger Bands if available
    if "bb_upper" in df.columns and "bb_lower" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["bb_upper"],
                line=dict(color="red", width=1),
                name="BB Upper",
                showlegend=False,
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df["bb_lower"],
                line=dict(color="green", width=1),
                name="BB Lower",
                showlegend=False,
            ),
            row=1,
            col=1,
        )

    # SMA lines
    if "sma_20" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df["sma_20"], line=dict(color="orange", width=1), name="SMA 20"
            ),
            row=1,
            col=1,
        )
    if "sma_50" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df["sma_50"], line=dict(color="purple", width=1), name="SMA 50"
            ),
            row=1,
            col=1,
        )

    # RSI
    if "rsi" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["rsi"], line=dict(color="blue", width=1), name="RSI"),
            row=2,
            col=1,
        )
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

    # MACD
    if "macd" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["macd"], line=dict(color="blue", width=1), name="MACD"),
            row=3,
            col=1,
        )
        if "macd_signal" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["macd_signal"],
                    line=dict(color="orange", width=1),
                    name="Signal",
                ),
                row=3,
                col=1,
            )

    # OBV
    if "obv" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["obv"], line=dict(color="green", width=1), name="OBV"),
            row=4,
            col=1,
        )

    fig.update_layout(
        height=800,
        showlegend=True,
        xaxis_rangeslider_visible=False,
    )

    return fig


def create_fibonacci_chart(df: pd.DataFrame) -> go.Figure:
    """Create Fibonacci retracement chart."""
    if len(df) < 30:
        return None

    high = df["high_price"].tail(30).max()
    low = df["low_price"].tail(30).min()

    fib_levels = calculate_fibonacci_retracement(high, low)

    fig = go.Figure()

    # Add price line - use bright cyan for dark mode visibility
    fig.add_trace(
        go.Scatter(
            x=df.index, y=df["close_price"], line=dict(color="#00FFFF", width=2), name="Close Price"
        )
    )

    # Add Fibonacci levels - use bright colors for dark mode
    colors = ["#FF4444", "#FF8844", "#FFCC00", "#44FF44", "#4488FF", "#AA44FF", "#FF0000"]
    for i, (level, price) in enumerate(fib_levels.items()):
        fig.add_hline(
            y=price,
            line_dash="dash",
            line_color=colors[i],
            annotation_text=level,
            annotation_position="top right",
            annotation_font=dict(color=colors[i], size=10),
        )

    fig.update_layout(
        title="Fibonacci Retracement Levels (Last 30 Days)",
        yaxis_title="Price (AUD)",
        height=500,  # Increased height for better visibility
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(showgrid=True, gridcolor="#333333"),
        yaxis=dict(showgrid=True, gridcolor="#333333"),
    )

    return fig


def create_prophet_forecast(
    df: pd.DataFrame, forecast_days: int = 30
) -> tuple[go.Figure, dict[str, float]]:
    """
    Create Prophet price forecast.

    Returns:
        tuple: (figure, forecast_metrics)
    """
    if not PROPHET_AVAILABLE:
        return None, {}

    if len(df) < 60:
        return None, {}

    try:
        # Prepare data for Prophet (requires 'ds' and 'y' columns)
        prophet_df = df.reset_index()[["date", "close_price"]].copy()
        prophet_df.columns = ["ds", "y"]
        prophet_df = prophet_df.dropna()

        if len(prophet_df) < 60:
            return None, {}

        # Create and fit Prophet model
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=True,
            changepoint_prior_scale=0.05,
        )
        model.fit(prophet_df)

        # Create future dataframe
        future = model.make_future_dataframe(periods=forecast_days)
        forecast = model.predict(future)

        # Create figure
        fig = go.Figure()

        # Historical data
        fig.add_trace(
            go.Scatter(
                x=prophet_df["ds"],
                y=prophet_df["y"],
                mode="lines",
                name="Historical",
                line=dict(color="#00FFFF", width=2),
            )
        )

        # Forecast
        fig.add_trace(
            go.Scatter(
                x=forecast["ds"],
                y=forecast["yhat"],
                mode="lines",
                name="Forecast",
                line=dict(color="#FFD700", width=2),
            )
        )

        # Confidence interval
        fig.add_trace(
            go.Scatter(
                x=forecast["ds"].tolist() + forecast["ds"].tolist()[::-1],
                y=forecast["yhat_upper"].tolist() + forecast["yhat_lower"].tolist()[::-1],
                fill="toself",
                fillcolor="rgba(255, 215, 0, 0.2)",
                line=dict(color="rgba(255, 215, 0, 0)"),
                name="Confidence Interval",
                showlegend=True,
            )
        )

        fig.update_layout(
            title=f"Prophet Price Forecast (Next {forecast_days} Days)",
            yaxis_title="Price (AUD)",
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#FFFFFF"),
            xaxis=dict(showgrid=True, gridcolor="#333333"),
            yaxis=dict(showgrid=True, gridcolor="#333333"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )

        # Calculate forecast metrics
        current_price = df["close_price"].iloc[-1]
        forecast_price = forecast["yhat"].iloc[-1]
        forecast_change = ((forecast_price - current_price) / current_price) * 100

        metrics = {
            "current_price": current_price,
            "forecast_price": forecast_price,
            "forecast_change_pct": forecast_change,
            "min_forecast": forecast["yhat_lower"].iloc[-1],
            "max_forecast": forecast["yhat_upper"].iloc[-1],
        }

        return fig, metrics

    except Exception as e:
        logger.error(f"Prophet forecast error: {e}")
        return None, {}


def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="ASX Stock Analysis",
        page_icon="📈",
        layout="wide",
    )

    st.title("📈 ASX Stock Analysis & Prediction")

    # Sidebar
    st.sidebar.header("Configuration")

    # Stock selector
    selected_stock = st.sidebar.selectbox(
        "Select Stock",
        options=list(ASX_STOCKS.keys()),
        format_func=lambda x: f"{x} - {ASX_STOCKS[x]}",
    )

    # Time period
    days = st.sidebar.slider("Days to Analyze", 30, 730, 365)

    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh data", value=False)

    if auto_refresh:
        st.rerun()

    # Load data
    df = load_stock_data(selected_stock, days=days)

    if df.empty:
        st.error(f"No data available for {selected_stock}")
        st.info("Run the extraction pipeline first: python -m ingestion.asx_extraction.extract")
        return

    # Calculate indicators
    df["rsi"] = calculate_rsi(df["close_price"])
    df["macd"], df["macd_signal"], df["macd_histogram"] = calculate_macd(df["close_price"])
    df["bb_upper"], df["bb_lower"] = calculate_bollinger_bands(df["close_price"])
    df["obv"] = calculate_obv(df["close_price"], df["volume"])
    df["sma_20"] = df["close_price"].rolling(20).mean()
    df["sma_50"] = df["close_price"].rolling(50).mean()

    # Calculate ATR
    df["tr"] = df.apply(
        lambda x: max(
            x["high_price"] - x["low_price"],
            abs(x["high_price"] - df.loc[x.name, "close_price"]) if x.name in df.index else 0,
            abs(x["low_price"] - df.loc[x.name, "close_price"]) if x.name in df.index else 0,
        ),
        axis=1,
    )
    df["atr"] = df["tr"].rolling(14).mean()

    # Current data
    latest = df.iloc[-1]
    prev_day = df.iloc[-2] if len(df) > 1 else latest

    # Price change
    price_change = latest["close_price"] - prev_day["close_price"]
    price_change_pct = (price_change / prev_day["close_price"]) * 100

    # Header metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Current Price",
            f"${latest['close_price']:.2f}",
            f"{price_change:+.2f} ({price_change_pct:+.2f}%)",
        )

    with col2:
        st.metric("RSI (14)", f"{latest['rsi']:.1f}")

    with col3:
        st.metric("MACD", f"{latest['macd']:.3f}")

    with col4:
        st.metric("Volume", f"{latest['volume']:,}")

    with col5:
        sector = latest.get("sector", "N/A").upper()
        st.metric("Sector", sector)

    # Main chart
    st.subheader("Price Chart with Technical Indicators")
    fig = create_candlestick_chart(df, selected_stock)
    st.plotly_chart(fig, use_container_width=True)

    # Fibonacci Chart
    st.subheader("Fibonacci Retracement")
    fib_fig = create_fibonacci_chart(df)
    if fib_fig:
        st.plotly_chart(fib_fig, use_container_width=True)

    # Prophet Forecast
    if PROPHET_AVAILABLE:
        st.subheader("Prophet Price Forecast")
        forecast_days = st.slider("Forecast Days", 7, 90, 30)

        with st.spinner("Generating forecast..."):
            forecast_fig, forecast_metrics = create_prophet_forecast(df, forecast_days)

        if forecast_fig:
            st.plotly_chart(forecast_fig, use_container_width=True)

            # Forecast metrics
            if forecast_metrics:
                fc1, fc2, fc3 = st.columns(3)
                with fc1:
                    st.metric(
                        "Current Price",
                        f"${forecast_metrics.get('current_price', 0):.2f}",
                    )
                with fc2:
                    st.metric(
                        f"Forecast ({forecast_days}d)",
                        f"${forecast_metrics.get('forecast_price', 0):.2f}",
                        f"{forecast_metrics.get('forecast_change_pct', 0):+.1f}%",
                    )
                with fc3:
                    st.metric(
                        "Range",
                        f"${forecast_metrics.get('min_forecast', 0):.2f} - ${forecast_metrics.get('max_forecast', 0):.2f}",
                    )
        else:
            st.info("Not enough historical data for forecasting (need 60+ days)")
    else:
        st.info("Prophet is not installed. Install with: uv add prophet")

    # Signals and Targets
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Trading Signals")

        signals = generate_signals(df)

        # Signal summary
        buy_signals = []
        sell_signals = []

        if signals["rsi_buy"]:
            buy_signals.append("RSI < 30 (Oversold)")
        if signals["rsi_sell"]:
            sell_signals.append("RSI > 70 (Overbought)")
        if signals["macd_buy"]:
            buy_signals.append("MACD Cross Up")
        if signals["macd_sell"]:
            sell_signals.append("MACD Cross Down")
        if signals["bb_buy"]:
            buy_signals.append("Below Lower BB")
        if signals["bb_sell"]:
            sell_signals.append("Above Upper BB")
        if signals["obv_buy"]:
            buy_signals.append("OBV Rising")
        if signals["obv_sell"]:
            sell_signals.append("OBV Falling")

        # Display signals
        if buy_signals:
            st.success("**BUY SIGNALS:**")
            for signal in buy_signals:
                st.write(f"  ✅ {signal}")

        if sell_signals:
            st.error("**SELL SIGNALS:**")
            for signal in sell_signals:
                st.write(f"  🔴 {signal}")

        if not buy_signals and not sell_signals:
            st.info("**NEUTRAL** - No clear signals")

        # Overall recommendation
        total_buy = len(buy_signals)
        total_sell = len(sell_signals)

        if total_buy > total_sell:
            st.success(f"**OVERALL: BUY** ({total_buy} vs {total_sell} signals)")
        elif total_sell > total_buy:
            st.error(f"**OVERALL: SELL** ({total_sell} vs {total_buy} signals)")
        else:
            st.info(f"**OVERALL: HOLD** ({total_buy} buy, {total_sell} sell)")

    with col2:
        st.subheader("🎯 Price Targets")

        targets = calculate_target_price(df)

        # Current price
        st.write(f"**Current Price:** ${targets.get('current_price', 0):.2f}")

        # Fibonacci targets
        if "fib_38" in targets:
            st.write(f"**Fibonacci Levels:**")
            st.write(f"  • 38.2%: ${targets['fib_38']:.2f}")
            st.write(f"  • 61.8%: ${targets['fib_61']:.2f}")
            st.write(f"  • 78.6%: ${targets['fib_78']:.2f}")

        # Support/Resistance
        if "support" in targets:
            st.write(f"**Support:** ${targets['support']:.2f}")
            st.write(f"**Resistance:** ${targets['resistance']:.2f}")

        # Moving averages
        if "sma_20" in targets:
            st.write(f"**SMA 20:** ${targets['sma_20']:.2f}")
        if "sma_50" in targets:
            st.write(f"**SMA 50:** ${targets['sma_50']:.2f}")

        # Expected move
        if "upside_target" in targets:
            st.write(f"**Upside Target (2x ATR):** ${targets['upside_target']:.2f}")
            st.write(f"**Downside Target (2x ATR):** ${targets['downside_target']:.2f}")

    # Recent price data
    st.subheader("Recent Price Data")
    display_df = df[["open_price", "high_price", "low_price", "close_price", "volume"]].tail(10)
    display_df = display_df.round(2)
    st.dataframe(display_df, use_container_width=True)

    # Footer
    st.markdown("---")
    st.caption(f"Data last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("Disclaimer: This is for educational purposes only. Not financial advice.")


if __name__ == "__main__":
    main()
