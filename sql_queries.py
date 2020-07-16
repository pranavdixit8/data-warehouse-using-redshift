import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay "
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events( 
                                          event_key bigint IDENTITY(0,1), 
                                          artist varchar, 
                                          auth varchar,
                                          firstName varchar, 
                                          gender varchar,
                                          itemInSession varchar, 
                                          lastName varchar, 
                                          length varchar, 
                                          level varchar, 
                                          location varchar, 
                                          method varchar, 
                                          page varchar, 
                                          registration varchar, 
                                          sessionId varchar, 
                                          song varchar,
                                          status varchar, 
                                          ts varchar, 
                                          userAgent varchar, 
                                          userId varchar, 
                                          primary key(event_key)
                                          )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs( 
                                        song_key bigint IDENTITY(0,1), 
                                        artist_id varchar, 
                                        artist_latitude varchar, 
                                        artist_location varchar, 
                                        artist_longitude varchar, 
                                        artist_name varchar, 
                                        duration varchar, 
                                        num_songs varchar, 
                                        song_id varchar, 
                                        title varchar, 
                                        year varchar,
                                        primary key(song_key)
                                        )
""")

#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
                                     songplay_id bigint IDENTITY(0,1), 
                                     start_time timestamp not null, 
                                     user_id int not null, 
                                     level varchar, 
                                     song_id varchar not null, 
                                     artist_id varchar not null, 
                                     session_id int, 
                                     location varchar, 
                                     user_agent varchar,
                                     primary key(songplay_id)
                                     )
""")

#user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
                                    user_id int primary key, 
                                    first_name varchar, 
                                    last_name varchar, 
                                    gender varchar, 
                                    level varchar
                                    
                                  )
""")

#song_id, title, artist_id, year, duration

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
                                song_id varchar primary key, 
                                title varchar, 
                                artist_id varchar, 
                                year int, 
                                duration float
                                )
""")

#artist_id, name, location, lattitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
                                  artist_id varchar primary key, 
                                  name varchar, 
                                  location varchar, 
                                  latitude float, 
                                  longitude float
                                  )

""")

#start_time, hour, day, week, month, year, weekday
time_table_create = ("""

CREATE TABLE IF NOT EXISTS time(
                                start_time timestamp primary key , 
                                hour int, 
                                day int, 
                                week int, 
                                month int, 
                                year int, 
                                weekday int
                                )

""")

# STAGING TABLES

staging_events_copy = ("""

copy staging_events from {}
    iam_role {}
    format as json {} region 'us-west-2';

""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
    iam_role {}
    format as json 'auto' region 'us-west-2';

""").format(SONG_DATA,ARN)




# FINAL TABLES

songplay_table_insert = ("""

INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  timestamp 'epoch' + e.ts::bigint/1000 * interval '1 second'  as start_time,
        e.userId::int as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId::int as session_id,
        e.location,
        e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs s ON e.song = s.title and e.artist = s.artist_name
WHERE e.page = 'NextSong' and start_time is not null and user_id is not null and artist_id is not null and song_id is not null

""")




user_table_insert = ("""

INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  DISTINCT userId::int    as user_id,
        firstName      as first_name,
        lastName       as last_name,
        gender,
        level
FROM  staging_events
where user_id  is not null and page = 'NextSong'

""")



song_table_insert = ("""

INSERT INTO song  (song_id, title, artist_id, year, duration) 
SELECT  DISTINCT song_id, 
        title, 
        artist_id, 
        year::int, 
        duration::float     
FROM staging_songs
where song_id is not null

""")


artist_table_insert = ("""

INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, 
        artist_name                as name,
        artist_location            as location, 
        artist_latitude::float     as latitude,
        artist_longitude::float    as longitude
FROM staging_songs
where artist_id is not null

""")



time_table_insert = ("""

INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT timestamp 'epoch' + ts::bigint/1000 * interval '1 second'  as start_time                                                              ,
       EXTRACT(hour FROM start_time )     as hour,
       EXTRACT(day FROM start_time)       as day,
       EXTRACT(week FROM start_time)      as week,
       EXTRACT(month FROM start_time)     as month,
       EXTRACT(year FROM start_time)      as year,
       EXTRACT(dow FROM start_time)    as weekday
FROM staging_events

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
