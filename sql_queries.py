"""Contains all the SQL queries used in the ETL job."""
# flake8: noqa
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events";'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs";'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays";'
user_table_drop = 'DROP TABLE IF EXISTS "dimUser";'
song_table_drop = 'DROP TABLE IF EXISTS "dimSong";'
artist_table_drop = 'DROP TABLE IF EXISTS "dimArtist";'
time_table_drop = 'DROP TABLE IF EXISTS "dimTime";'

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS "staging_events" (
        artist        text,
        auth          character varying(20),
        firstName     character varying(100),
        gender        char(1),
        itemInSession smallint,
        lastName      character varying(100),
        length        double precision,
        level         character varying(30),
        location      text,
        method        character varying(10),
        page          character varying(30),
        registration  bigint,
        sessionId     int,
        song          text,
        status        smallint,
        ts            bigint,
        userAgent     text,
        userId        int
    );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs smallint,
        artist_id character varying(30),
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location text,
        artist_name text,
        song_id character varying(30),
        title text,
        duration double precision,
        year smallint
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS "songplays" (
        id            bigint IDENTITY(0, 1) NOT NULL,
        time_key      timestamp NOT NULL,
        user_key      int NOT NULL,
        level         character varying(30) NOT NULL,
        song_id      character varying(30),
        artist_key    character varying(30),
        session_id    int NOT NULL,
        location      text,
        user_agent    text,
        primary key(id),
        foreign key(time_key)   references dimTime(time_key),
        foreign key(user_key)   references dimUser(user_key),
        foreign key(song_id)   references dimSong(song_id),
        foreign key(artist_key) references dimArtist(artist_key)
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS "dimUser" (
        user_key      int NOT NULL,
        first_name    character varying(100),
        last_name     character varying(100),
        gender        char(1),
        level         character varying(30) NOT NULL,
        primary key(user_key)
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS "dimSong" (
        song_id       character varying(30) NOT NULL,
        title         text,
        artist_key    character varying(30),
        year          smallint NOT NULL,
        duration      double precision NOT NULL,
        primary key(song_id)
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS "dimArtist" (
        artist_key    character varying(30) NOT NULL,
        artist_name   text,
        artist_loc    text,
        artist_lat    double precision,
        artist_long   double precision,
        primary key(artist_key)
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS "dimTime" (
        time_key      timestamp NOT NULL,
        hour          smallint NOT NULL,
        day           smallint NOT NULL,
        week          smallint NOT NULL,
        month         smallint NOT NULL,
        year          smallint NOT NULL,
        weekday       smallint NOT NULL,
        primary key(time_key)
    );
"""

# STAGING TABLES

staging_events_copy = """
    copy staging_events
    from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 's3://udacity-dend/log_json_path.json';
""".format(
    DWH_ROLE_ARN
)

staging_songs_copy = """
    copy staging_songs
    from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto';
""".format(
    DWH_ROLE_ARN
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (time_key, user_key, level, song_id, artist_key, session_id, location, user_agent)
SELECT
    se.start_ts     AS time_key,
    se.userId       AS user_key,
    se.level        AS level,
    ss.song_id      AS song_id,
    ss.artist_id    AS artist_key,
    se.sessionId    AS session_id,
    se.location     AS location,
    se.userAgent    AS user_agent
FROM (
    SELECT TIMESTAMP 'epoch' + staging_events.ts / 1000 * INTERVAL '1 second' AS start_ts, *
    FROM staging_events
    WHERE page='NextSong'
) se
LEFT JOIN staging_songs ss
ON (
    se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
);
"""

user_table_insert = """
INSERT INTO dimUser (user_key, first_name, last_name, gender, level)
SELECT
    DISTINCT(se.userId)       AS user_key,
    se.firstName    AS first_name,
    se.lastName     AS last_name,
    se.gender       AS gender,
    se.level        AS level
FROM staging_events se
WHERE page='NextSong';
"""

song_table_insert = """
INSERT INTO dimSong (song_id, title, artist_key, year, duration)
SELECT
    DISTINCT(ss.song_id)    AS song_id,
    ss.title                AS title,
    ss.artist_id            AS artist_key,
    ss.year                 AS year,
    ss.duration             AS duration
FROM staging_songs ss
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO dimArtist (artist_key, artist_name, artist_loc, artist_lat, artist_long)
SELECT
    DISTINCT(ss.artist_id)  AS artist_key,
    ss.artist_name          AS artist_name,
    ss.artist_location      AS artist_loc,
    ss.artist_latitude      AS artist_lat,
    ss.artist_longitude     AS artist_long
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL;
"""

time_table_insert = """
INSERT INTO dimTime (time_key, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(time_key)              AS time_key,
    EXTRACT(hour FROM time_key)     AS hour,
    EXTRACT(day FROM time_key)      AS day,
    EXTRACT(week FROM time_key)     AS week,
    EXTRACT(month FROM time_key)    AS month,
    EXTRACT(year FROM time_key)     AS year,
    EXTRACT(weekday FROM time_key)  AS weekday
FROM songplays;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
