/*
 Mart model: ML Training Dataset
 Final feature table ready for ML with:
 - All technical indicators and ML features
 - Target variables: next_day_return, next_week_return
 - Train/test split marker
 - No nulls in key features
*/

{{ config(
    materialized='table',
    alias='mart_ml_training_dataset'
) }}

WITH features AS (
    SELECT * FROM {{ ref('int_ml_features') }}
),

target_calc AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- Target: Next day return (percentage)
        (
            LEAD(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) - close_price
        ) / close_price * 100 AS next_day_return,
        
        -- Target: Next week return (5 trading days)
        (
            LEAD(close_price, 5) OVER (PARTITION BY symbol ORDER BY price_date) - close_price
        ) / close_price * 100 AS next_week_return,
        
        -- Also calculate next month for flexibility (21 trading days)
        (
            LEAD(close_price, 21) OVER (PARTITION BY symbol ORDER BY price_date) - close_price
        ) / close_price * 100 AS next_month_return
        
    FROM features
),

train_test_split AS (
    SELECT
        f.*,
        t.next_day_return,
        t.next_week_return,
        t.next_month_return,
        
        -- Date-based train/test split (80% train, 20% test by time)
        -- Use a fixed cutoff date for reproducibility
        CASE 
            WHEN f.price_date < '2024-01-01' THEN 'train'
            WHEN f.price_date < '2025-01-01' THEN 'validation'
            ELSE 'test'
        END AS split,
        
        -- For time-series cross-validation, add fold number
        -- Each year is a fold (2021-2025)
        CASE 
            WHEN f.price_date >= '2021-01-01' AND f.price_date < '2022-01-01' THEN 1
            WHEN f.price_date >= '2022-01-01' AND f.price_date < '2023-01-01' THEN 2
            WHEN f.price_date >= '2023-01-01' AND f.price_date < '2024-01-01' THEN 3
            WHEN f.price_date >= '2024-01-01' AND f.price_date < '2025-01-01' THEN 4
            WHEN f.price_date >= '2025-01-01' THEN 5
            ELSE 0
        END AS fold_number
        
    FROM features f
    JOIN target_calc t 
        ON f.symbol = t.symbol AND f.price_date = t.price_date
),

feature_list AS (
    SELECT
        symbol,
        price_date,
        
        -- Price data
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        daily_range,
        
        -- SMAs
        sma_7,
        sma_14,
        sma_30,
        sma_50,
        sma_200,
        
        -- RSI
        rsi_14,
        
        -- MACD
        macd_line,
        macd_signal,
        macd_histogram,
        
        -- Bollinger Bands
        bb_middle,
        bb_upper,
        bb_lower,
        bb_position,
        
        -- ATR
        atr_14,
        
        -- Volume
        volume_sma_20,
        obv,
        volume_ratio,
        
        -- Volatility
        volatility_20,
        
        -- Lag features
        lag_1_day_return,
        lag_3_day_return,
        lag_5_day_return,
        lag_7_day_return,
        
        -- Momentum
        momentum_7,
        momentum_14,
        momentum_30,
        momentum_1,
        
        -- Cross signals
        golden_cross_signal,
        death_cross_signal,
        ma_crossover_distance,
        short_term_trend,
        
        -- Support/Resistance
        support_20,
        resistance_20,
        support_50,
        resistance_50,
        
        -- Price position
        price_position_20,
        price_position_50,
        
        -- Sector (encoded as numeric for ML)
        CASE sector
            WHEN 'gold' THEN 1
            WHEN 'oil' THEN 2
            WHEN 'silver' THEN 3
            ELSE 0
        END AS sector_code,
        
        -- Targets
        next_day_return,
        next_week_return,
        next_month_return,
        
        -- Split info
        split,
        fold_number
        
    FROM train_test_split
)

SELECT
    -- Primary key
    symbol,
    price_date,
    
    -- Feature columns
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_range,
    
    sma_7,
    sma_14,
    sma_30,
    sma_50,
    sma_200,
    
    rsi_14,
    
    macd_line,
    macd_signal,
    macd_histogram,
    
    bb_middle,
    bb_upper,
    bb_lower,
    bb_position,
    
    atr_14,
    
    volume_sma_20,
    obv,
    volume_ratio,
    
    volatility_20,
    
    lag_1_day_return,
    lag_3_day_return,
    lag_5_day_return,
    lag_7_day_return,
    
    momentum_7,
    momentum_14,
    momentum_30,
    momentum_1,
    
    golden_cross_signal,
    death_cross_signal,
    ma_crossover_distance,
    short_term_trend,
    
    support_20,
    resistance_20,
    support_50,
    resistance_50,
    
    price_position_20,
    price_position_50,
    
    sector_code,
    
    -- Target variables
    next_day_return,
    next_week_return,
    next_month_return,
    
    -- Split info
    split,
    fold_number
    
FROM feature_list

WHERE
    -- Remove rows where targets are NULL (last days without future data)
    next_day_return IS NOT NULL
    AND next_week_return IS NOT NULL
    
    -- Remove rows with NULL in key features (not enough history)
    AND sma_7 IS NOT NULL
    AND sma_14 IS NOT NULL
    AND sma_30 IS NOT NULL
    AND sma_50 IS NOT NULL
    AND sma_200 IS NOT NULL
    AND rsi_14 IS NOT NULL
    AND macd_line IS NOT NULL
    AND bb_position IS NOT NULL
    AND atr_14 IS NOT NULL
    AND volatility_20 IS NOT NULL
    
ORDER BY 
    symbol,
    price_date