
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sector
from "asx_stocks"."raw_asx_data"."asx_stock_prices"
where sector is null



  
  
      
    ) dbt_internal_test