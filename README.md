
# Project Overview

The idea of the project is to analysis the activity on a music app by artists, songs, users over time. We do so using a star schema data warehouse architecture with dimensions: artists, songs, users, time with facts being the activity by the user of playing songs from the app. We are implementing the star schema architecture on columnar store database: redshift. We do the following in the project:

1. Create a redshift database cluster on AWS.
2. Connect to the database
3. Create all the tables required: staging, fact and dimension tables. Define proper data types for the dimension and fact tables.
4. We load the data from S3 into the staging tables as it is, hence the data type of all the columns in the staging tables is varchar.
5. Insert data into fact and dimension tables using the staging tables.

# Commands

> Create the database and tables.
>> python create_table.py
 
> perform etl and insert data into tables
>>python etl.py

# Files

 - ***create_tables.py***: this file creates the database and the tables (fact and dimension table of the star schema), it uses the file: *sql_queries*.
 - ***etl.py***: this file execute the etl process for our project: loading the files in dataframes, modifying the data and inserting the data in the tables.
 - ***sql_queries.py***: this file contain all the sql queries for creating, inserting, and droping the tables in the database and required selection query for our design
 - ***dwh.cfg***: this is the configuration file for the cluster



# Design

## Staging Table

- ***staging_events***:
>Columns: event_key, artist, auth , firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
>Primary key: event_key
- ***staging_songs***:
>Columns: song_key, artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year
>Primary key: song_key

## Fact Table

 - ***songplay***: 
 > Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
 > Primary key: songplay_id

## Dimension Tables

 - ***artist***:
 > Columns: artist_id, name, location, latitude, longitude
 > Primary key: artist_id
 
 - ***song***:
 > Columns: song_id, title, artist_id, year, duration
 > Primary key: song_id
 
 - ***users***:
 > Columns: user_id, first_name, last_name, gender, level
 > Primary key: user_id
 
 - ***time***:
 > Columns: start_time, hour, day, week, month, year, weekday
 Primary key: start_time
 
 