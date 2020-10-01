import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS sonplays"
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
            registration text,
            sessionId text,
            song text,
            status text,
            ts text,
            userAgent text,
            userId integer
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        staging_songs (
            num_songs int,
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
            songplay_id integer not null,
            start_time timestamp,
            user_id integer,
            level text,
            song_id integer,
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
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS
        time (
            start_time timestamp not null,
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
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    GZIP REGION 'us-west-1'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    GZIP REGION 'us-west-1'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
