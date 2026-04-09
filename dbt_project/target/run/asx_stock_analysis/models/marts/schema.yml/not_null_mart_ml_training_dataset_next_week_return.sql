
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select next_week_return
from "asx_stocks"."analytics"."mart_ml_training_dataset"
where next_week_return is null



  
  
      
    ) dbt_internal_test