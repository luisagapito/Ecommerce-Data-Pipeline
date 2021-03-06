user_table_insert = ("""
SELECT distinct user_id, gender, cast(seniorcitizen as int) as senior_citizen, cast(seniorcitizen as int) as senior_citizen,
cast((case when dependents='Yes' then 1 else 0 end) as int) as dependents , 
cast((case when phoneservice='Yes' then 1 else 0 end) as int) as phone_service, 
cast((case when internetservice='No' then 0 else 1 end) as int) as  internet_service 
FROM staging_customer
""")
    

products_table_insert = ("""
select distinct product_id, name, trim(product_type), description
from staging_products
""")


time_table_insert = ("""
SELECT cast(event_time as timestamp) as event_time,
extract(hour from cast(event_time as timestamp)) as hour_time,  
extract(day from cast(event_time as timestamp)) as day_time, 
extract(week from cast(event_time as timestamp)) as week_time,  
extract(month from cast(event_time as timestamp)) as month_time,  
extract(year from cast(event_time as timestamp)) as year_time,  
extract(weekday from cast(event_time as timestamp)) as weekday_time  
from staging_events  
where event_type = 'purchase'  
and event_time is not null
""")