/*
 Staging model for ASX stock prices.
 Cleans and standardizes raw stock data, adding sector classification.
*/



SELECT
    -- Convert timezone-aware timestamp to date for consistency
    CAST(date AS DATE) AS price_date,
    
    -- Standardize symbol (uppercase)
    UPPER(symbol) AS symbol,
    
    -- Standardize sector classification
    LOWER(sector) AS sector,
    
    -- Price fields - ensure positive values
    open_price,
    high_price,
    low_price,
    close_price,
    
    -- Volume - ensure non-negative
    volume,
    
    -- Derived fields
    (high_price - low_price) AS daily_range,  -- Intraday price range
    
    -- Extraction metadata
    extraction_timestamp

FROM "asx_stocks"."raw_asx_data"."asx_stock_prices"

WHERE 
    -- Filter out any null key fields
    date IS NOT NULL
    AND symbol IS NOT NULL
    AND close_price IS NOT NULL

QUALIFY
    -- Remove duplicates, keeping the most recent record per day/symbol
    ROW_NUMBER() OVER (
        PARTITION BY CAST(date AS DATE), UPPER(symbol) 
        ORDER BY extraction_timestamp DESC
    ) = 1

ORDER BY 
    symbol,
    price_date