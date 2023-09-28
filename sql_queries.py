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
        name                TEXT,
        song_id             CHARACTER VARYING(30),
        title               TEXT,
        duration            DOUBLE PRECISION,
        year                SMALLINT
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS "songplays" (
        songplay_id                 BIGINT IDENTITY(0, 1) NOT NULL,
        start_time                  TIMESTAMP NOT NULL,
        user_id                     INT NOT NULL,
        level                       CHARACTER VARYING(30) NOT NULL,
        song_id                     CHARACTER VARYING(30),
        artist_id                   CHARACTER VARYING(30),
        session_id                  INT NOT NULL,
        location                    TEXT,
        user_agent                  TEXT,
        primary key(songplay_id),
        foreign key(start_time)     references time(start_time),
        foreign key(user_id)        references users(user_id),
        foreign key(song_id)        references songs(song_id),
        foreign key(artist_id)      references artists(artist_id)
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS "users" (
        user_id     INT NOT NULL,
        first_name  CHARACTER VARYING(100),
        last_name   CHARACTER VARYING(100),
        gender      CHAR(1),
        level       CHARACTER VARYING(30) NOT NULL,
        primary key(user_id)
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS "songs" (
        song_id     CHARACTER VARYING(30) NOT NULL,
        title       TEXT,
        artist_id   CHARACTER VARYING(30),
        year        SMALLINT NOT NULL,
        duration    DOUBLE PRECISION NOT NULL,
        primary key(song_id)
    );
"""

# NOTE: project spec has typo: "lattitude" is incorrect spelling.
# I intentionally chose the correct spelling for use in column below
artist_table_create = """
    CREATE TABLE IF NOT EXISTS "artists" (
        artist_id   CHARACTER VARYING(30) NOT NULL,
        name        TEXT,
        location    TEXT,
        latitude    DOUBLE PRECISION,
        longitude   DOUBLE PRECISION,
        primary key(artist_id)
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS "time" (
        start_time  TIMESTAMP NOT NULL,
        hour        SMALLINT NOT NULL,
        day         SMALLINT NOT NULL,
        week        SMALLINT NOT NULL,
        month       SMALLINT NOT NULL,
        year        SMALLINT NOT NULL,
        weekday     SMALLINT NOT NULL,
        primary key(start_time)
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
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    se.start_ts     AS start_time,
    se.userId       AS user_id,
    se.level        AS level,
    ss.song_id      AS song_id,
    ss.artist_id    AS artist_id,
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
    AND se.artist = ss.name
    AND se.length = ss.duration
);
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT(se.userId) AS user_id,
    se.firstName        AS first_name,
    se.lastName         AS last_name,
    se.gender           AS gender,
    se.level            AS level
FROM staging_events se
WHERE page='NextSong';
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT(ss.song_id)    AS song_id,
    ss.title                AS title,
    ss.artist_id            AS artist_id,
    ss.year                 AS year,
    ss.duration             AS duration
FROM staging_songs ss
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT(ss.artist_id)  AS artist_id,
    ss.name             AS name,
    ss.artist_location  AS location,
    ss.artist_latitude  AS latitude,
    ss.artist_longitude AS longitude
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(start_time)                AS start_time,
    EXTRACT(hour FROM start_time)       AS hour,
    EXTRACT(day FROM start_time)        AS day,
    EXTRACT(week FROM start_time)       AS week,
    EXTRACT(month FROM start_time)      AS month,
    EXTRACT(year FROM start_time)       AS year,
    EXTRACT(weekday FROM start_time)    AS weekday
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
