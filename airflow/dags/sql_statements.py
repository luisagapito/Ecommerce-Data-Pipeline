user_table_insert = ("""
SELECT distinct user_id, gender, cast(seniorcitizen as int) as senior_citizen,
partner, dependents, phoneservice as phone_service,internetservice as internet_service  
FROM staging_customer
""")
    

products_table_insert = ("""
select distinct product_id, name, product_type, description
from staging_products
""")

    
'''
artist_table_insert = ("""
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")
'''

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