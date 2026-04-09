
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "asx_stocks"."analytics"."stg_asx_stock_prices"
where symbol is null



  
  
      
    ) dbt_internal_test