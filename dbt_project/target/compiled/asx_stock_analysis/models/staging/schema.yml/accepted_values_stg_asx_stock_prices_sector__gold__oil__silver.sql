
    
    

with all_values as (

    select
        sector as value_field,
        count(*) as n_records

    from "asx_stocks"."analytics"."stg_asx_stock_prices"
    group by sector

)

select *
from all_values
where value_field not in (
    'gold','oil','silver'
)


