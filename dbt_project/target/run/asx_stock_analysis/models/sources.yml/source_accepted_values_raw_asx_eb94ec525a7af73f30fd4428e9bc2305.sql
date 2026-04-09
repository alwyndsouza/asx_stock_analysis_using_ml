
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        sector as value_field,
        count(*) as n_records

    from "asx_stocks"."raw_asx_data"."asx_stock_prices"
    group by sector

)

select *
from all_values
where value_field not in (
    'gold','oil','silver'
)



  
  
      
    ) dbt_internal_test