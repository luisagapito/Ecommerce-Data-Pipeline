CREATE TABLE IF NOT EXISTS artists ( 
artist_id varchar sortkey, 
name varchar , 
location varchar,  
latitude varchar, 
longitude varchar) 
diststyle all;

CREATE TABLE IF NOT EXISTS songplays (
songplay_id bigint identity(0, 1) sortkey, 
start_time varchar, 
user_id int , 
level varchar, 
song_id varchar NOT NULL distkey, 
artist_id varchar NOT NULL , 
session_id int ,
location varchar, 
user_agent varchar);

CREATE TABLE IF NOT EXISTS songs ( 
song_id varchar sortkey, 
title varchar, 
artist_id varchar, 
year int, 
duration numeric) 
diststyle all;

CREATE TABLE IF NOT EXISTS staging_events (
artist varchar , 
auth varchar, 
firstName varchar, 
gender varchar, 
itemInSession int, 
lastName varchar, 
length numeric , 
level varchar,
location varchar,
method varchar, 
page varchar sortkey,
registration bigint, 
sessionId bigint, 
song varchar distkey, 
status bigint, 
ts bigint, 
userAgent varchar, 
userId bigint );

CREATE TABLE IF NOT EXISTS staging_songs (
num_songs bigint, 
artist_id varchar distkey, 
artist_latitude varchar,
artist_longitude varchar, 
artist_location varchar,  
artist_name varchar, 
song_id varchar sortkey, 
title varchar,
duration numeric, 
year bigint);

CREATE TABLE IF NOT EXISTS time ( 
start_time varchar sortkey distkey, 
hour varchar, 
day varchar, 
week varchar, 
month varchar, 
year varchar, 
weekday varchar);

CREATE TABLE IF NOT EXISTS users ( 
user_id int sortkey distkey, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar);





