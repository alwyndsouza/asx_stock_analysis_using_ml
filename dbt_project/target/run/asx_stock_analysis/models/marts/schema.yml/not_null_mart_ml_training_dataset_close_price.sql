
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close_price
from "asx_stocks"."analytics"."mart_ml_training_dataset"
where close_price is null



  
  
      
    ) dbt_internal_test