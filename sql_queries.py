import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        itemInSession int,
        last_name varchar,
        length float,
        level varchar,
        artist_location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        song_id varchar not null,
        num_songs int,
        title varchar,
        artist_name varchar,
        artist_latitude float,
        year int,
        duration float,
        artist_id varchar not null,
        artist_longitude float,
        artist_location varchar)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id int identity(0,1) primary key sortkey,
        start_time timestamp not null,
        user_id int not null,
        level varchar,
        song_id varchar not null,
        artist_id varchar not null,
        session_id int,
        artist_location varchar,
        user_agent varchar)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar not null primary key,
        first_name varchar not null,
        last_name varchar not null,
        gender char,
        level varchar)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar not null primary key,
        title varchar not null,
        artist_id varchar not null,
        year int,
        duration float)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar not null primary key,
        artist_name varchar not null,
        artist_location varchar,
        artist_latitude float,
        artist_longitude float)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, artist_location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                    se.userId as user_id,
                    se.level as level,
                    ss.song_id as song_id,
                    ss.artist_id as artist_id,
                    se.sessionId as session_id,
                    se.artist_location as artist_location,
                    se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id,
                    first_name AS first_name,
                    last_name AS last_name,
                    gender AS gender,
                    level AS level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id AS song_id,
                    title AS title,
                    artist_id AS artist_id,
                    year AS year,
                    duration AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT artist_id AS artist_id,
                    artist_name AS artist_name,
                    artist_location AS artist_location,
                    artist_latitude AS artist_latitude,
                    artist_longitude AS artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts,
                    EXTRACT(hour FROM ts),
                    EXTRACT(day FROM ts),
                    EXTRACT(week FROM ts),
                    EXTRACT(month FROM ts),
                    EXTRACT(year FROM ts),
                    EXTRACT(weekday FROM ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
