
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_date
from "asx_stocks"."analytics"."int_ml_features"
where price_date is null



  
  
      
    ) dbt_internal_test