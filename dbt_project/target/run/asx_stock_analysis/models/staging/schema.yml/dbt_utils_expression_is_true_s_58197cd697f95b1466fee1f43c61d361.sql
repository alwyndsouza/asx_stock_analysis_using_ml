
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "asx_stocks"."analytics"."stg_asx_stock_prices"

where not(high_price >= 0)


  
  
      
    ) dbt_internal_test