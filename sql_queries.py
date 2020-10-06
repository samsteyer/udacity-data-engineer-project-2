import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS
        staging_events (
            artist text,
            auth text,
            firstName text,
            gender text,
            itemInSession integer,
            lastName text,
            length decimal,
            level text,
            location text,
            method text,
            page text,
            registration double precision,
            sessionId integer,
            song text,
            status integer,
            ts bigint,
            userAgent text,
            userId integer
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        staging_songs (
            num_songs integer,
            artist_id text,
            artist_latitude decimal,
            artist_longitude decimal,
            artist_location text,
            artist_name text,
            song_id text,
            title text,
            duration decimal,
            year integer
        )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        songplays (
            songplay_id integer identity,
            start_time timestamp sortkey distkey,
            user_id integer,
            level text,
            song_id text,
            artist_id text,
            session_id text,
            location text,
            user_agent text
        )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        users (
            user_id integer not null,
            first_name text,
            last_name text,
            gender text,
            level text
        )
        diststyle all
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        songs (
            song_id text not null,
            title text,
            artist_id text,
            year integer,
            duration decimal
        )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        artists (
            artist_id text not null,
            name text,
            location text,
            latitude integer,
            longitude integer
        )
        diststyle all
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        time (
            start_time timestamp not null sortkey distkey,
            hour integer,
            day integer,
            week integer,
            month integer,
            year integer,
            weekday integer
        )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY
        staging_events
    FROM
        {}
    CREDENTIALS
        'aws_iam_role={}'
    REGION
        'us-west-2'
    JSON
        {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY
        staging_songs
    FROM
        {}
    CREDENTIALS
        'aws_iam_role={}'
    REGION
        'us-west-2'
    JSON
        'auto'
    TRUNCATECOLUMNS
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO
        songplays (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
    SELECT
        timestamp 'epoch' + e.ts * interval '1 second',
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM
        staging_events e
    JOIN
        staging_songs s
    ON
        e.song = s.title
    AND
        e.artist = s.artist_name
    AND
        e.length = s.duration
    WHERE
        e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO
        users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
            userId,
            firstName,
            lastName,
            gender,
            level
    FROM
        staging_events
    WHERE
        page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO
        songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        staging_songs
    WHERE
        song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO
        artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        staging_songs
    WHERE
        artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO
        time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(dayofweek FROM start_time)
    FROM
        songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
