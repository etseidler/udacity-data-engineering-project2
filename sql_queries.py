import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

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
"""

staging_songs_table_create = """
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
        "first_name"    character varying(100) NOT NULL,
        "last_name"     character varying(100) NOT NULL,
        "gender"        char(1) NOT NULL,
        "level"         character varying(30) NOT NULL,
        primary key(user_key)
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS "dimSong" (
        "song_key"      character varying(30) NOT NULL,
        "title"         text NOT NULL,
        "artist_key"    character varying(30) NOT NULL,
        "year"          smallint NOT NULL,
        "duration"      double precision NOT NULL,
        primary key(song_key)
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS "dimArtist" (
        "artist_key"    character varying(30) NOT NULL,
        "artist_name"   text NOT NULL,
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

staging_events_copy = (
    """
"""
).format()

staging_songs_copy = (
    """
"""
).format()

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

# create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
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
