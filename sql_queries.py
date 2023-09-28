"""Contains all the SQL queries used in the ETL job."""
# flake8: noqa
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events";'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs";'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays";'
user_table_drop = 'DROP TABLE IF EXISTS "users";'
song_table_drop = 'DROP TABLE IF EXISTS "songs";'
artist_table_drop = 'DROP TABLE IF EXISTS "artists";'
time_table_drop = 'DROP TABLE IF EXISTS "time";'

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS "staging_events" (
        artist        TEXT,
        auth          CHARACTER VARYING(20),
        firstName     CHARACTER VARYING(100),
        gender        CHAR(1),
        itemInSession SMALLINT,
        lastName      CHARACTER VARYING(100),
        length        DOUBLE PRECISION,
        level         CHARACTER VARYING(30),
        location      TEXT,
        method        CHARACTER VARYING(10),
        page          CHARACTER VARYING(30),
        registration  BIGINT,
        sessionId     INT,
        song          TEXT,
        status        SMALLINT,
        ts            BIGINT,
        userAgent     TEXT,
        userId        INT
    );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS "staging_songs" (
        num_songs           SMALLINT,
        artist_id           CHARACTER VARYING(30),
        artist_latitude     DOUBLE PRECISION,
        artist_longitude    DOUBLE PRECISION,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             CHARACTER VARYING(30),
        title               TEXT,
        duration            DOUBLE PRECISION,
        year                SMALLINT
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS "songplays" (
        id                      BIGINT IDENTITY(0, 1) NOT NULL,
        time_key                TIMESTAMP NOT NULL,
        user_key                INT NOT NULL,
        level                   CHARACTER VARYING(30) NOT NULL,
        song_id                 CHARACTER VARYING(30),
        artist_key              CHARACTER VARYING(30),
        session_id              INT NOT NULL,
        location                TEXT,
        user_agent              TEXT,
        primary key(id),
        foreign key(time_key)   references time(time_key),
        foreign key(user_key)   references users(user_key),
        foreign key(song_id)    references songs(song_id),
        foreign key(artist_key) references artists(artist_key)
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS "users" (
        user_key      INT NOT NULL,
        first_name    CHARACTER VARYING(100),
        last_name     CHARACTER VARYING(100),
        gender        CHAR(1),
        level         CHARACTER VARYING(30) NOT NULL,
        primary key(user_key)
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS "songs" (
        song_id       CHARACTER VARYING(30) NOT NULL,
        title         TEXT,
        artist_key    CHARACTER VARYING(30),
        year          SMALLINT NOT NULL,
        duration      DOUBLE PRECISION NOT NULL,
        primary key(song_id)
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS "artists" (
        artist_key    CHARACTER VARYING(30) NOT NULL,
        artist_name   TEXT,
        artist_loc    TEXT,
        artist_lat    DOUBLE PRECISION,
        artist_long   DOUBLE PRECISION,
        primary key(artist_key)
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS "time" (
        time_key      TIMESTAMP NOT NULL,
        hour          SMALLINT NOT NULL,
        day           SMALLINT NOT NULL,
        week          SMALLINT NOT NULL,
        month         SMALLINT NOT NULL,
        year          SMALLINT NOT NULL,
        weekday       SMALLINT NOT NULL,
        primary key(time_key)
    );
"""

# STAGING TABLES

staging_events_copy = """
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON {};
""".format(
    LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH
)

staging_songs_copy = """
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto';
""".format(
    SONG_DATA, DWH_ROLE_ARN
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
INSERT INTO users (user_key, first_name, last_name, gender, level)
SELECT
    DISTINCT(se.userId) AS user_key,
    se.firstName        AS first_name,
    se.lastName         AS last_name,
    se.gender           AS gender,
    se.level            AS level
FROM staging_events se
WHERE page='NextSong';
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_key, year, duration)
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
INSERT INTO artists (artist_key, artist_name, artist_loc, artist_lat, artist_long)
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
INSERT INTO time (time_key, hour, day, week, month, year, weekday)
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
