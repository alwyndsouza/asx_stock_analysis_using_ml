/*
 Intermediate model: ML Features for ASX Stocks
 Creates advanced features for ML price prediction:
 - Lag features (1, 3, 5, 7 day price changes)
 - Price momentum (rate of change)
 - Golden cross / death cross signals
 - Support/resistance levels
 - Sector correlation features
*/



WITH base_indicators AS (
    SELECT * FROM "asx_stocks"."analytics"."int_technical_indicators"
),

lag_features AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- Lag features: price changes over different periods
        (close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS lag_1_day_return,
            
        (close_price - LAG(close_price, 3) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 3) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS lag_3_day_return,
            
        (close_price - LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS lag_5_day_return,
            
        (close_price - LAG(close_price, 7) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 7) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS lag_7_day_return,
            
        -- Lag close prices for future feature calculation
        LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close_1,
        LAG(close_price, 3) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close_3,
        LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close_5,
        LAG(close_price, 7) OVER (PARTITION BY symbol ORDER BY price_date) AS prev_close_7
        
    FROM base_indicators
),

momentum AS (
    SELECT
        symbol,
        price_date,
        
        -- Momentum: rate of change over different periods
        (close_price - LAG(close_price, 7) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 7) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS momentum_7,
            
        (close_price - LAG(close_price, 14) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 14) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS momentum_14,
            
        (close_price - LAG(close_price, 30) OVER (PARTITION BY symbol ORDER BY price_date)) / 
            LAG(close_price, 30) OVER (PARTITION BY symbol ORDER BY price_date) * 100 AS momentum_30,
            
        -- Stochastic momentum
        close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY price_date) AS momentum_1
        
    FROM base_indicators
),

cross_signals AS (
    SELECT
        symbol,
        price_date,
        
        -- Golden Cross: Short SMA crosses above Long SMA
        -- Buy signal when SMA_50 crosses above SMA_200
        CASE 
            WHEN sma_50 > sma_200 
                 AND LAG(sma_50, 1) OVER (PARTITION BY symbol ORDER BY price_date) <= 
                     LAG(sma_200, 1) OVER (PARTITION BY symbol ORDER BY price_date)
            THEN 1 
            ELSE 0 
        END AS golden_cross_signal,
        
        -- Death Cross: Short SMA crosses below Long SMA  
        -- Sell signal when SMA_50 crosses below SMA_200
        CASE 
            WHEN sma_50 < sma_200 
                 AND LAG(sma_50, 1) OVER (PARTITION BY symbol ORDER BY price_date) >= 
                     LAG(sma_200, 1) OVER (PARTITION BY symbol ORDER BY price_date)
            THEN 1 
            ELSE 0 
        END AS death_cross_signal,
        
        -- Simple moving average trends
        sma_50 - sma_200 AS ma_crossover_distance,
        sma_7 - sma_14 AS short_term_trend
        
    FROM base_indicators
),

support_resistance AS (
    SELECT
        symbol,
        price_date,
        high_price,
        low_price,
        close_price,
        
        -- Rolling min/max for support/resistance
        MIN(low_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_20_low,  -- Support level
        
        MAX(high_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_20_high,  -- Resistance level
        
        MIN(low_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS rolling_50_low,
        
        MAX(high_price) OVER (
            PARTITION BY symbol ORDER BY price_date 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS rolling_50_high
        
    FROM base_indicators
),

price_position AS (
    SELECT
        symbol,
        price_date,
        close_price,
        
        -- Where is current price relative to 20-day range
        CASE 
            WHEN rolling_20_high - rolling_20_low = 0 THEN 0.5
            ELSE (close_price - rolling_20_low) / (rolling_20_high - rolling_20_low)
        END AS price_position_20,
        
        CASE 
            WHEN rolling_50_high - rolling_50_low = 0 THEN 0.5
            ELSE (close_price - rolling_50_low) / (rolling_50_high - rolling_50_low)
        END AS price_position_50
        
    FROM support_resistance
),

sector_data AS (
    SELECT
        symbol,
        price_date,
        
        -- Get sector for this stock
        FIRST_VALUE(sector) OVER (
            PARTITION BY symbol ORDER BY price_date
        ) AS sector
        
    FROM "asx_stocks"."analytics"."stg_asx_stock_prices"
    
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, price_date ORDER BY price_date) = 1
),

sector_momentum AS (
    SELECT
        s.symbol,
        s.price_date,
        
        -- Average return for sector on each day
        AVG(lf.lag_1_day_return) OVER (
            PARTITION BY s.sector, s.price_date
        ) AS sector_1_day_return,
        
        AVG(lf.lag_3_day_return) OVER (
            PARTITION BY s.sector, s.price_date
        ) AS sector_3_day_return,
        
        AVG(lf.lag_7_day_return) OVER (
            PARTITION BY s.sector, s.price_date
        ) AS sector_7_day_return
        
    FROM lag_features lf
    JOIN sector_data s 
        ON lf.symbol = s.symbol AND lf.price_date = s.price_date
),

final_join AS (
    SELECT
        b.symbol,
        b.price_date,
        
        -- Technical indicators from base
        b.open_price,
        b.high_price,
        b.low_price,
        b.close_price,
        b.volume,
        b.daily_range,
        
        -- SMAs
        b.sma_7,
        b.sma_14,
        b.sma_30,
        b.sma_50,
        b.sma_200,
        
        -- RSI
        b.rsi_14,
        
        -- MACD
        b.macd_line,
        b.macd_signal,
        b.macd_histogram,
        
        -- Bollinger Bands
        b.bb_middle,
        b.bb_upper,
        b.bb_lower,
        b.bb_position,
        
        -- ATR
        b.atr_14,
        
        -- Volume
        b.volume_sma_20,
        b.obv,
        b.volume_ratio,
        
        -- Volatility
        b.volatility_20,
        
        -- Lag features
        lf.lag_1_day_return,
        lf.lag_3_day_return,
        lf.lag_5_day_return,
        lf.lag_7_day_return,
        
        -- Momentum
        m.momentum_7,
        m.momentum_14,
        m.momentum_30,
        m.momentum_1,
        
        -- Cross signals
        cs.golden_cross_signal,
        cs.death_cross_signal,
        cs.ma_crossover_distance,
        cs.short_term_trend,
        
        -- Support/Resistance
        sr.rolling_20_low AS support_20,
        sr.rolling_20_high AS resistance_20,
        sr.rolling_50_low AS support_50,
        sr.rolling_50_high AS resistance_50,
        
        -- Price position
        pp.price_position_20,
        pp.price_position_50,
        
        -- Sector
        sd.sector
        
    FROM base_indicators b
    
    LEFT JOIN lag_features lf 
        ON b.symbol = lf.symbol AND b.price_date = lf.price_date
        
    LEFT JOIN momentum m 
        ON b.symbol = m.symbol AND b.price_date = m.price_date
        
    LEFT JOIN cross_signals cs 
        ON b.symbol = cs.symbol AND b.price_date = cs.price_date
        
    LEFT JOIN support_resistance sr 
        ON b.symbol = sr.symbol AND b.price_date = sr.price_date
        
    LEFT JOIN price_position pp 
        ON b.symbol = pp.symbol AND b.price_date = pp.price_date
        
    LEFT JOIN sector_data sd 
        ON b.symbol = sd.symbol AND b.price_date = sd.price_date
)

SELECT
    -- Primary key
    symbol,
    price_date,
    
    -- All columns from joined data
    * EXCLUDE (symbol, price_date)
    
FROM final_join

ORDER BY 
    symbol,
    price_date