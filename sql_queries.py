import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS public.events_stage;"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.songs_stage;"
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays;"
user_table_drop = "DROP TABLE IF EXISTS public.users;"
song_table_drop = "DROP TABLE IF EXISTS public.songs;"
artist_table_drop = "DROP TABLE IF EXISTS public.artists;"
time_table_drop = "DROP TABLE IF EXISTS public.time;"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE public.events_stage(
    artist_id VARCHAR ENCODE ZSTD,
    auth VARCHAR ENCODE ZSTD,
    first_name VARCHAR ENCODE ZSTD,
    gender VARCHAR ENCODE ZSTD,
    item_in_session VARCHAR ENCODE ZSTD,
    last_name VARCHAR ENCODE ZSTD,
    length FLOAT8 ENCODE ZSTD,
    level VARCHAR ENCODE ZSTD,
    location VARCHAR ENCODE ZSTD,
    method VARCHAR ENCODE ZSTD,
    page VARCHAR ENCODE ZSTD,
    registration VARCHAR ENCODE ZSTD,
    session_id VARCHAR ENCODE ZSTD,
    song_title VARCHAR ENCODE ZSTD,
    status VARCHAR ENCODE ZSTD,
    ts VARCHAR ENCODE ZSTD,
    user_agent VARCHAR ENCODE ZSTD,
    user_id VARCHAR ENCODE ZSTD
);
""")

staging_songs_table_create = ("""
CREATE TABLE public.songs_stage(
    num_songs INT ENCODE ZSTD,
    artist_id VARCHAR ENCODE ZSTD,
    artist_latitude FLOAT8 ENCODE ZSTD,
    artist_longitude FLOAT8 ENCODE ZSTD,
    artist_location VARCHAR ENCODE ZSTD,
    artist_name VARCHAR ENCODE ZSTD,
    song_id VARCHAR ENCODE ZSTD,
    title VARCHAR ENCODE ZSTD,
    duration FLOAT8 ENCODE ZSTD,
    year INT ENCODE ZSTD
);
""")

songplay_table_create = ("""
CREATE TABLE public.songplays(
    songplay_id INT IDENTITY(0,1) PRIMARY KEY ENCODE ZSTD,
    start_time TIMESTAMP NOT NULL ENCODE RAW,
    user_id VARCHAR NOT NULL ENCODE RAW DISTKEY,
    level VARCHAR ENCODE ZSTD,
    song_id VARCHAR NOT NULL ENCODE ZSTD,
    artist_id VARCHAR NOT NULL ENCODE ZSTD,
    session_id VARCHAR ENCODE ZSTD,
    location VARCHAR ENCODE ZSTD,
    user_agent VARCHAR ENCODE ZSTD
)
SORTKEY(start_time);
""")

user_table_create = ("""
CREATE TABLE public.users(
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR ENCODE ZSTD,
    last_name VARCHAR ENCODE ZSTD,
    gender VARCHAR(1)ENCODE ZSTD,
    level VARCHAR ENCODE ZSTD
)
DISTSTYLE AUTO
""")

song_table_create = ("""
CREATE TABLE public.songs (
    song_id VARCHAR PRIMARY KEY ENCODE ZSTD,
    title VARCHAR NOT NULL ENCODE RAW,
    artist_id VARCHAR NOT NULL DISTKEY ENCODE RAW,
    year INT4 ENCODE ZSTD,
    duration FLOAT8 ENCODE ZSTD
)
SORTKEY(title);
""")

artist_table_create = ("""
CREATE TABLE public.artists (
    artist_id VARCHAR PRIMARY KEY ENCODE ZSTD,
    name VARCHAR NOT NULL ENCODE ZSTD,
    location VARCHAR ENCODE ZSTD,
    lattitude FLOAT8 ENCODE ZSTD,
    longitude FLOAT8 ENCODE ZSTD
)
DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE public.time (
    start_time TIMESTAMP PRIMARY KEY ENCODE ZSTD,
    hour INT ENCODE ZSTD, 
    day INT ENCODE ZSTD, 
    week INT ENCODE ZSTD, 
    month INT ENCODE ZSTD, 
    year INT ENCODE ZSTD, 
    weekday INT ENCODE ZSTD
)DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY public.events_stage
FROM {}
CREDENTIALS {}
FORMAT AS JSON {} REGION 'us-west-2';

""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY public.songs_stage
FROM {}
CREDENTIALS {}
FORMAT AS JSON 'auto' REGION 'us-west-2' TRUNCATECOLUMNS;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO public.songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
    e.user_id,
    e.level,
    s.song_id,
    e.artist_id,
    e.session_id,
    e.location,
    e.user_agent    
FROM public.events_stage e
LEFT JOIN public.songs_stage s
ON e.song_title = s.title
AND e.artist_id = s.artist_id
WHERE page = 'NextSong';
""")

# using the window function here in the CTE
# enables us to filter and pick only the latest of the duplicate user entries
user_table_insert = ("""
INSERT INTO public.users(user_id, first_name, last_name, gender, level)
WITH unique_user AS (
    SELECT user_id,
    first_name,
    last_name,
    gender,
    level,
    ROW_NUMBER() over (partition by user_id order by ts desc ) as index
FROM public.events_stage
)
SELECT user_id,
    first_name,
    last_name,
    gender,
    level
FROM unique_user
WHERE COALESCE(user_id, '') <> '' and unique_user.index = 1
""")

song_table_insert = ("""
INSERT INTO public.songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,    
    duration
FROM public.songs_stage;
""")

artist_table_insert = ("""
INSERT INTO public.artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,    
    artist_longitude
FROM public.songs_stage;
""")

time_table_insert = ("""
INSERT INTO public.time(start_time, hour, day, week, month, year, weekday)
WITH time_parse AS
(
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
    FROM public.events_stage
)
SELECT
    start_time AS start_time,
    EXTRACT (hour from start_time) AS hour,
    EXTRACT (day from start_time) AS day,
    EXTRACT (week from start_time) AS week,
    EXTRACT (month from start_time) AS month,
    EXTRACT (year from start_time) AS year,
    EXTRACT (dow from start_time) AS weekday
FROM time_parse;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, song_table_insert, user_table_insert, artist_table_insert,
                        time_table_insert]
