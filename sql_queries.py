import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= (""" create table if not exists staging_events (artist text, auth text, firstname text, gender text, iteminsession int, lastname text, length numeric, level text, location text, method text, page text, registration numeric, session_id int, song text, status int, ts bigint, useragent text, user_id int)
""")

staging_songs_table_create = (""" create table if not exists staging_songs (artist_id text, artist_latitude text, artist_location text, artist_longitude text, artist_name text, duration numeric, num_songs int, song_id text, title text, year int)
""")

songplay_table_create = (""" create table if not exists songplays (songplay_id int IDENTITY(0,1) Primary Key, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)
""")

user_table_create = (""" create table if not exists users (user_id int Primary Key, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = (""" create table if not exists songs (song_id varchar Primary Key, title varchar, artist_id varchar, year int, duration numeric)
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar Primary Key, name varchar, location varchar, latitude numeric, longitude numeric)
""")

time_table_create = ("""create table if not exists time (start_time timestamp, hour numeric , day numeric, week numeric, month numeric, year numeric, weekday numeric)
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} credentials 'aws_iam_role={}' format as json 'auto'
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = (""" copy staging_songs from {} credentials 'aws_iam_role={}' format as json 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" Insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
(SELECT Distinct(timestamp 'epoch' + events.ts / 1000*INTERVAL '1 second') AS start_time,
events.user_id AS user_id,
events.level AS level,
songs.song_id AS song_id,
songs.artist_id AS artist_id,
events.session_id AS session_id,
events.location AS location,
events.useragent AS user_agent
FROM staging_events AS events
JOIN staging_songs AS songs
ON (events.artist = songs.artist_name)
AND (events.song = songs.title)
AND (events.length = songs.duration)
WHERE events.page = 'NextSong') 
""")

user_table_insert = (""" Insert into users (user_id, first_name, last_name, gender, level)
(Select Distinct(user_id), firstname as first_name, lastname as last_name, gender, level from staging_events where page = 'NextSong') 
""")

song_table_insert = ("""Insert into songs (song_id, title, artist_id, year, duration) 
(select Distinct(song_id), title, artist_id, year, duration from staging_songs) 
""")

artist_table_insert = ("""Insert into artists (artist_id, name, location, latitude, longitude) 
(Select Distinct(artist_id), artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude from staging_songs) 
""")

time_table_insert = (""" Insert into time (start_time, hour, day, week, month, year, weekday) 
(Select distinct(a.start_time), 
EXTRACT (HOUR FROM a.start_time), 
EXTRACT (DAY FROM a.start_time), 
EXTRACT (WEEK FROM a.start_time), 
EXTRACT (MONTH FROM a.start_time), 
EXTRACT (YEAR FROM a.start_time), 
EXTRACT (WEEKDAY FROM a.start_time) 
from (Select start_time from songplays)a) 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
