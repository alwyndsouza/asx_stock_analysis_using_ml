"""
ASX Trading Signals Overview.

This app shows trading signals for all ASX stocks at once.
Displaying buy/sell signals, RSI, MACD, and price targets for all stocks.

Usage:
    streamlit run app/signals.py
"""

import logging
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

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
def load_all_stock_data(days: int = 365) -> pd.DataFrame:
    """Load all stock data from DuckDB."""
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
            WHERE date >= '{start_date}'
            ORDER BY symbol, date
        """

        df = con.execute(query).df()
        con.close()

        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values(["symbol", "date"])

        return df

    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return pd.DataFrame()


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate technical indicators for all stocks."""
    # Group by symbol and calculate indicators
    result = []

    for symbol in df["symbol"].unique():
        stock_df = df[df["symbol"] == symbol].copy()
        stock_df = stock_df.set_index("date")

        if len(stock_df) < 14:
            continue

        # RSI
        delta = stock_df["close_price"].diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        stock_df["rsi"] = 100 - (100 / (1 + rs))

        # MACD
        ema_12 = stock_df["close_price"].ewm(span=12, adjust=False).mean()
        ema_26 = stock_df["close_price"].ewm(span=26, adjust=False).mean()
        stock_df["macd"] = ema_12 - ema_26
        stock_df["macd_signal"] = stock_df["macd"].ewm(span=9, adjust=False).mean()

        # SMA
        stock_df["sma_20"] = stock_df["close_price"].rolling(20).mean()
        stock_df["sma_50"] = stock_df["close_price"].rolling(50).mean()

        # Bollinger Bands
        stock_df["bb_middle"] = stock_df["close_price"].rolling(20).mean()
        bb_std = stock_df["close_price"].rolling(20).std()
        stock_df["bb_upper"] = stock_df["bb_middle"] + (2 * bb_std)
        stock_df["bb_lower"] = stock_df["bb_middle"] - (2 * bb_std)

        # ATR
        high_low = stock_df["high_price"] - stock_df["low_price"]
        high_close = abs(stock_df["high_price"] - stock_df["close_price"].shift())
        low_close = abs(stock_df["low_price"] - stock_df["close_price"].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        stock_df["atr"] = tr.rolling(14).mean()

        # Get latest row
        latest = stock_df.iloc[-1]
        prev = stock_df.iloc[-2] if len(stock_df) > 1 else latest

        # Generate signals
        signals = {}

        # RSI
        if latest["rsi"] < 30:
            signals["rsi"] = "BUY"
        elif latest["rsi"] > 70:
            signals["rsi"] = "SELL"
        else:
            signals["rsi"] = "NEUTRAL"

        # MACD
        if latest["macd"] > latest["macd_signal"] and prev["macd"] <= prev["macd_signal"]:
            signals["macd"] = "BUY"
        elif latest["macd"] < latest["macd_signal"] and prev["macd"] >= prev["macd_signal"]:
            signals["macd"] = "SELL"
        else:
            signals["macd"] = "NEUTRAL"

        # Bollinger
        if latest["close_price"] < latest["bb_lower"]:
            signals["bb"] = "BUY"
        elif latest["close_price"] > latest["bb_upper"]:
            signals["bb"] = "SELL"
        else:
            signals["bb"] = "NEUTRAL"

        # Price vs SMAs
        if latest["close_price"] > latest["sma_20"] and latest["close_price"] > latest["sma_50"]:
            signals["trend"] = "BULLISH"
        elif latest["close_price"] < latest["sma_20"] and latest["close_price"] < latest["sma_50"]:
            signals["trend"] = "BEARISH"
        else:
            signals["trend"] = "NEUTRAL"

        # Calculate overall signal score
        buy_count = sum(1 for v in signals.values() if v in ["BUY", "BULLISH"])
        sell_count = sum(1 for v in signals.values() if v in ["SELL", "BEARISH"])

        if buy_count > sell_count + 1:
            signals["overall"] = "STRONG BUY"
        elif buy_count > sell_count:
            signals["overall"] = "BUY"
        elif sell_count > buy_count + 1:
            signals["overall"] = "STRONG SELL"
        elif sell_count > buy_count:
            signals["overall"] = "SELL"
        else:
            signals["overall"] = "HOLD"

        # Add to result
        result.append(
            {
                "symbol": symbol,
                "name": ASX_STOCKS.get(symbol, symbol),
                "sector": latest.get("sector", "Unknown"),
                "close_price": latest["close_price"],
                "change_pct": (
                    (latest["close_price"] - stock_df.iloc[-2]["close_price"])
                    / stock_df.iloc[-2]["close_price"]
                    * 100
                )
                if len(stock_df) > 1
                else 0,
                "rsi": latest["rsi"],
                "macd": latest["macd"],
                "macd_signal": latest["macd_signal"],
                "sma_20": latest["sma_20"],
                "sma_50": latest["sma_50"],
                "atr": latest["atr"],
                "support": latest["bb_lower"],
                "resistance": latest["bb_upper"],
                "rsi_signal": signals["rsi"],
                "macd_signal": signals["macd"],
                "bb_signal": signals["bb"],
                "trend_signal": signals["trend"],
                "overall_signal": signals["overall"],
                "last_updated": latest.name,
            }
        )

    return pd.DataFrame(result)


def create_signal_heatmap(df: pd.DataFrame) -> go.Figure:
    """Create a heatmap of signals."""
    if df.empty:
        return None

    # Create signal matrix
    signal_cols = ["rsi_signal", "macd_signal", "bb_signal", "trend_signal"]

    # Map signals to numbers for heatmap
    signal_map = {"STRONG BUY": 2, "BUY": 1, "NEUTRAL": 0, "SELL": -1, "STRONG SELL": -2}

    fig = go.Figure()

    for i, col in enumerate(signal_cols):
        signal_name = col.replace("_signal", "").upper()
        values = df[col].map(signal_map).tolist()

        fig.add_trace(
            go.Bar(
                x=df["symbol"],
                y=[signal_name] * len(df),
                marker=dict(
                    color=values,
                    colorscale=[
                        [-2, "#FF0000"],  # Strong sell
                        [-1, "#FF6666"],  # Sell
                        [0, "#888888"],  # Neutral
                        [1, "#66FF66"],  # Buy
                        [2, "#00FF00"],  # Strong buy
                    ],
                    showscale=False,
                ),
                orientation="h",
                name=signal_name,
            )
        )

    fig.update_layout(
        title="Signal Heatmap",
        barmode="group",
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#FFFFFF"),
        xaxis=dict(showgrid=True, gridcolor="#333333"),
        yaxis=dict(showgrid=True, gridcolor="#333333"),
    )

    return fig


def main():
    """Main Streamlit app."""
    st.set_page_config(
        page_title="ASX Trading Signals",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 ASX Trading Signals Overview")

    # Load data
    with st.spinner("Loading stock data..."):
        df = load_all_stock_data()

    if df.empty:
        st.error("No data available. Run the extraction pipeline first.")
        return

    # Calculate indicators
    with st.spinner("Calculating indicators..."):
        signals_df = calculate_indicators(df)

    if signals_df.empty:
        st.error("No signals calculated. Not enough data.")
        return

    # Summary metrics
    st.subheader("Market Summary")

    col1, col2, col3, col4 = st.columns(4)

    buy_count = len(signals_df[signals_df["overall_signal"].isin(["BUY", "STRONG BUY"])])
    sell_count = len(signals_df[signals_df["overall_signal"].isin(["SELL", "STRONG SELL"])])
    hold_count = len(signals_df[signals_df["overall_signal"] == "HOLD"])

    with col1:
        st.metric("Buy Signals", buy_count, delta_color="normal")
    with col2:
        st.metric("Sell Signals", sell_count, delta_color="inverse")
    with col3:
        st.metric("Hold Signals", hold_count)
    with col4:
        sector_counts = signals_df["sector"].value_counts().to_dict()
        st.metric("Sectors", len(sector_counts))

    # Signal by sector
    st.subheader("Signals by Sector")
    sector_summary = (
        signals_df.groupby("sector")["overall_signal"].value_counts().unstack(fill_value=0)
    )
    st.dataframe(sector_summary, use_container_width=True)

    # Overall signals table
    st.subheader("All Stock Signals")

    # Color code the signals
    def color_signal(val):
        if val in ["STRONG BUY", "BUY"]:
            return "color: green; font-weight: bold"
        elif val in ["STRONG SELL", "SELL"]:
            return "color: red; font-weight: bold"
        else:
            return "color: gray"

    display_cols = [
        "symbol",
        "sector",
        "close_price",
        "change_pct",
        "rsi",
        "rsi_signal",
        "macd_signal",
        "bb_signal",
        "trend_signal",
        "overall_signal",
    ]

    # Apply styling - use map instead of deprecated applymap
    styled_df = signals_df[display_cols].copy()
    styled_df["overall_signal"] = styled_df["overall_signal"].map(color_signal)

    st.dataframe(
        signals_df[display_cols],
        use_container_width=True,
        height=400,
    )

    # Charts
    st.subheader("Signal Visualization")

    col1, col2 = st.columns(2)

    with col1:
        # Signal distribution pie chart
        signal_counts = signals_df["overall_signal"].value_counts()
        fig_pie = go.Figure(
            data=[
                go.Pie(
                    labels=signal_counts.index,
                    values=signal_counts.values,
                    hole=0.4,
                    marker=dict(colors=["#00FF00", "#FF0000", "#888888", "#66FF66", "#FF6666"]),
                )
            ]
        )
        fig_pie.update_layout(
            title="Signal Distribution",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#FFFFFF"),
            height=300,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # RSI vs Price chart
        fig = px.scatter(
            signals_df,
            x="rsi",
            y="close_price",
            color="overall_signal",
            size="atr",
            hover_name="symbol",
            color_discrete_map={
                "STRONG BUY": "#00FF00",
                "BUY": "#66FF66",
                "HOLD": "#888888",
                "SELL": "#FF6666",
                "STRONG SELL": "#FF0000",
            },
        )
        fig.update_layout(
            title="RSI vs Price (size = ATR)",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#FFFFFF"),
            xaxis=dict(showgrid=True, gridcolor="#333333"),
            yaxis=dict(showgrid=True, gridcolor="#333333"),
            height=300,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Detailed stock selector
    st.subheader("Stock Detail View")

    selected = st.selectbox(
        "Select Stock",
        options=signals_df["symbol"].tolist(),
        format_func=lambda x: f"{x} - {ASX_STOCKS.get(x, x)}",
    )

    if selected:
        stock_data = signals_df[signals_df["symbol"] == selected].iloc[0]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Price", f"${stock_data['close_price']:.2f}", f"{stock_data['change_pct']:+.2f}%"
            )
        with col2:
            st.metric("RSI (14)", f"{stock_data['rsi']:.1f}", stock_data["rsi_signal"])
        with col3:
            st.metric("MACD", f"{stock_data['macd']:.3f}", stock_data["macd_signal"])
        with col4:
            st.metric("Overall", stock_data["overall_signal"])

        # Show price targets
        st.write("**Price Targets:**")
        st.write(f"  Support: ${stock_data['support']:.2f}")
        st.write(f"  Resistance: ${stock_data['resistance']:.2f}")
        st.write(f"  SMA 20: ${stock_data['sma_20']:.2f}")
        st.write(f"  SMA 50: ${stock_data['sma_50']:.2f}")

    # Footer
    st.markdown("---")
    st.caption(f"Data last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("Disclaimer: This is for educational purposes only. Not financial advice.")


if __name__ == "__main__":
    main()
