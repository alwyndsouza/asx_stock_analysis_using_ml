
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        split as value_field,
        count(*) as n_records

    from "asx_stocks"."analytics"."mart_ml_training_dataset"
    group by split

)

select *
from all_values
where value_field not in (
    'train','validation','test'
)



  
  
      
    ) dbt_internal_test