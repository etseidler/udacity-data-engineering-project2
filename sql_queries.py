"""Contains all the SQL queries used in the ETL job."""
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events";'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs";'
songplay_table_drop = 'DROP TABLE IF EXISTS "factsSongplay";'
user_table_drop = 'DROP TABLE IF EXISTS "dimUser";'
song_table_drop = 'DROP TABLE IF EXISTS "dimSong";'
artist_table_drop = 'DROP TABLE IF EXISTS "dimArtist";'
time_table_drop = 'DROP TABLE IF EXISTS "dimTime";'

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS "staging_events" (
        "id"            bigint IDENTITY(0, 1) NOT NULL,
        "artist"        text,
        "auth"          character varying(20) NOT NULL,
        "firstName"     character varying(100),
        "gender"        char(1),
        "itemInSession" smallint NOT NULL,
        "lastName"      character varying(100),
        "length"        double precision,
        "level"         character varying(30) NOT NULL,
        "location"      text,
        "method"        character varying(10) NOT NULL,
        "page"          character varying(30) NOT NULL,
        "registration"  bigint,
        "sessionId"     int NOT NULL,
        "song"          text,
        "status"        smallint NOT NULL,
        "ts"            bigint NOT NULL,
        "userAgent"     text,
        "userId"        int,
        primary key(id)
    );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        "num_songs" smallint,
        "artist_id" character varying(30) NOT NULL,
        "artist_latitude" double precision,
        "artist_longitude" double precision,
        "artist_location" text,
        "artist_name" text,
        "song_id" character varying(30) NOT NULL,
        "title" text NOT NULL,
        "duration" double precision NOT NULL,
        "year" smallint NOT NULL,
        primary key(song_id)
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS "factsSongplay" (
        "id"            bigint IDENTITY(0, 1) NOT NULL,
        "time_key"      timestamp NOT NULL,
        "user_key"      int NOT NULL,
        "level"         character varying(30) NOT NULL,
        "song_key"      character varying(30) NOT NULL,
        "artist_key"    character varying(30) NOT NULL,
        "session_id"    int NOT NULL,
        "location"      text,
        "user_agent"    text,
        primary key(id),
        foreign key(time_key)   references dimTime(time_key),
        foreign key(user_key)   references dimUser(user_key),
        foreign key(song_key)   references dimSong(song_key),
        foreign key(artist_key) references dimArtist(artist_key)
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS "dimUser" (
        "user_key"      int NOT NULL,
        "first_name"    character varying(100),
        "last_name"     character varying(100),
        "gender"        char(1),
        "level"         character varying(30) NOT NULL,
        primary key(user_key)
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS "dimSong" (
        "song_key"      character varying(30) NOT NULL,
        "title"         text,
        "artist_key"    character varying(30) NOT NULL,
        "year"          smallint NOT NULL,
        "duration"      double precision NOT NULL,
        primary key(song_key)
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS "dimArtist" (
        "artist_key"    character varying(30) NOT NULL,
        "artist_name"   text,
        "artist_loc"    text,
        "artist_lat"    double precision,
        "artist_long"   double precision,
        primary key(artist_key)
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS "dimTime" (
        "time_key"      timestamp NOT NULL,
        "hour"          smallint NOT NULL,
        "day"           smallint NOT NULL,
        "week"          smallint NOT NULL,
        "month"         smallint NOT NULL,
        "year"          smallint NOT NULL,
        "weekday"       smallint NOT NULL,
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
"""

user_table_insert = """
"""

song_table_insert = """
"""

artist_table_insert = """
"""

time_table_insert = """
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
