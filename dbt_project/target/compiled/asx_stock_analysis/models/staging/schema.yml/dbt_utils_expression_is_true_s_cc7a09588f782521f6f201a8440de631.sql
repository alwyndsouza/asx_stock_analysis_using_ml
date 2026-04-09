



select
    1
from "asx_stocks"."analytics"."stg_asx_stock_prices"

where not(close_price >= 0)

