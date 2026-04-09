
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "asx_stocks"."analytics"."int_technical_indicators"
where symbol is null



  
  
      
    ) dbt_internal_test