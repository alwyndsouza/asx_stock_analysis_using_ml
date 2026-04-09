
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fold_number
from "asx_stocks"."analytics"."mart_ml_training_dataset"
where fold_number is null



  
  
      
    ) dbt_internal_test