



select
    1
from "asx_stocks"."analytics"."stg_asx_stock_prices"

where not(high_price >= 0)

