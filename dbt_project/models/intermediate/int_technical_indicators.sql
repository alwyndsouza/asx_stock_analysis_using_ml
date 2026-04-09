/*
 Intermediate model: Technical Indicators for ASX Stocks
 Calculates common technical indicators used for ML feature engineering:
 - Simple Moving Averages (SMA): 7, 14, 30, 50, 200 day
 - RSI (Relative Strength Index): 14 period
 - MACD: 12-day EMA - 26-day EMA with signal line
 - Bollinger Bands: SMA ± 2×standard deviation
 - ATR (Average True Range): 14 period
 - Volume indicators: OBV, volume SMA
 
 Incremental mode: updates only new records based on price_date
*/

{{ config(
    materialized='incremental',
    alias='int_technical_indicators',
    unique_key=['symbol', 'price_date'],
    on_schema_change='fail'
) }}

WITH price_data AS (
    SELECT
        symbol,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        
        -- Daily range
        high_price - low_price AS daily_range,
        
        -- Basic price changes
        close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) AS price_change,
        close_price - open_price AS intraday_return
        
    FROM {{ ref('stg_asx_stock_prices') }}
    
    {% if is_incremental() %}
    -- For incremental, we need lookback data for window functions
    -- Fetch records from 200 days before the new data (for SMA_200)
    WHERE price_date >= (
        SELECT MAX(price_date) - INTERVAL 200 DAY FROM {{ this }}
    )
    {% endif %}
),

sma_calc AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- Simple Moving Averages
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sma_7,
        
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS sma_14,
        
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30,
        
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200
        
    FROM price_data
),

rsi_calc AS (
    SELECT
        symbol,
        price_date,
        close_price,
        price_change,
        
        -- Calculate gains and losses
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
        
    FROM price_data
),

rsi_final AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- RSI requires average gains/losses over 14 periods
        -- Using exponential moving average approach for smoother RSI
        AVG(gain) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        
        AVG(loss) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
        
    FROM rsi_calc
),

macd_calc AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- EMA calculation using exponential weighted window
        -- 12-day EMA
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS ema_12_raw,
        
        -- 26-day EMA  
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) AS ema_26_raw
        
    FROM price_data
),

macd_final AS (
    SELECT
        symbol,
        price_date,
        
        -- MACD Line = 12-day EMA - 26-day EMA
        ema_12_raw - ema_26_raw AS macd_line,
        
        -- Signal Line = 9-day EMA of MACD
        AVG(ema_12_raw - ema_26_raw) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) AS macd_signal,
        
        -- MACD Histogram = MACD Line - Signal Line
        (ema_12_raw - ema_26_raw) - AVG(ema_12_raw - ema_26_raw) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) AS macd_histogram
        
    FROM macd_calc
),

bollinger_calc AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- 20-day SMA for Bollinger Bands
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS bb_sma_20,
        
        -- 20-day standard deviation
        STDDEV(close_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS bb_std_20
        
    FROM price_data
),

atr_calc AS (
    SELECT
        symbol,
        price_date,
        high_price,
        low_price,
        
        LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close,
        
        -- True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
        GREATEST(
            high_price - low_price,
            ABS(high_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date)),
            ABS(low_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date))
        ) AS true_range
        
    FROM price_data
),

atr_final AS (
    SELECT
        symbol,
        price_date,
        
        -- ATR = 14-day average of True Range
        AVG(true_range) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14
        
    FROM atr_calc
),

volume_calc AS (
    SELECT
        symbol,
        price_date,
        volume,
        close_price,
        
        -- Volume SMA
        AVG(volume) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volume_sma_20,
        
        -- Price direction for OBV
        CASE 
            WHEN close_price > LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) THEN volume
            WHEN close_price < LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) THEN -volume
            ELSE 0
        END AS volume_change
        
    FROM price_data
),

obv_calc AS (
    SELECT
        symbol,
        price_date,
        volume,
        
        -- On-Balance Volume (cumulative)
        SUM(volume_change) OVER (
            PARTITION BY symbol ORDER BY price_date
        ) AS obv
        
    FROM volume_calc
),

volatility_calc AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- First calculate daily returns in a subquery/CTE
        daily_return
        
    FROM (
        SELECT
            symbol,
            price_date,
            close_price,
            
            -- Daily return = (close - prev_close) / prev_close
            (close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date)) / 
                NULLIF(LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date), 0) AS daily_return
            
        FROM price_data
    ) AS daily_returns
),

volatility_final AS (
    SELECT
        symbol,
        price_date,
        
        -- 20-day rolling standard deviation of daily returns
        STDDEV(daily_return) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volatility_20
        
    FROM volatility_calc
)

SELECT
    -- Primary key
    s.symbol,
    s.price_date,
    
    -- Original OHLCV data
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.volume,
    s.daily_range,
    
    -- Simple Moving Averages
    sma.sma_7,
    sma.sma_14,
    sma.sma_30,
    sma.sma_50,
    sma.sma_200,
    
    -- RSI
    CASE 
        WHEN rsi.avg_loss = 0 THEN 100
        ELSE 100 - (100 / (1 + rsi.avg_gain / rsi.avg_loss))
    END AS rsi_14,
    
    -- MACD
    macd.macd_line,
    macd.macd_signal,
    macd.macd_histogram,
    
    -- Bollinger Bands
    bb.bb_sma_20 AS bb_middle,
    bb.bb_sma_20 + (2 * bb.bb_std_20) AS bb_upper,
    bb.bb_sma_20 - (2 * bb.bb_std_20) AS bb_lower,
    -- Bollinger Band Position: (close - lower) / (upper - lower)
    CASE 
        WHEN (bb.bb_sma_20 + (2 * bb.bb_std_20)) - (bb.bb_sma_20 - (2 * bb.bb_std_20)) = 0 THEN 0
        ELSE (s.close_price - (bb.bb_sma_20 - (2 * bb.bb_std_20))) / 
             ((bb.bb_sma_20 + (2 * bb.bb_std_20)) - (bb.bb_sma_20 - (2 * bb.bb_std_20)))
    END AS bb_position,
    
    -- ATR
    atr.atr_14,
    
    -- Volume indicators
    vol.volume_sma_20,
    obv.obv,
    CASE 
        WHEN vol.volume_sma_20 > 0 THEN s.volume / vol.volume_sma_20 
        ELSE 1
    END AS volume_ratio,
    
    -- Volatility
    vol_stat.volatility_20
    
FROM price_data s

LEFT JOIN sma_calc sma 
    ON s.symbol = sma.symbol AND s.price_date = sma.price_date

LEFT JOIN rsi_final rsi 
    ON s.symbol = rsi.symbol AND s.price_date = rsi.price_date

LEFT JOIN macd_final macd 
    ON s.symbol = macd.symbol AND s.price_date = macd.price_date

LEFT JOIN bollinger_calc bb 
    ON s.symbol = bb.symbol AND s.price_date = bb.price_date

LEFT JOIN atr_final atr 
    ON s.symbol = atr.symbol AND s.price_date = atr.price_date

LEFT JOIN volume_calc vol 
    ON s.symbol = vol.symbol AND s.price_date = vol.price_date

LEFT JOIN obv_calc obv 
    ON s.symbol = obv.symbol AND s.price_date = obv.price_date

LEFT JOIN volatility_final vol_stat 
    ON s.symbol = vol_stat.symbol AND s.price_date = vol_stat.price_date

ORDER BY 
    s.symbol,
    s.price_date