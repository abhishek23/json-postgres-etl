# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id BIGSERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR(10),
        song_id VARCHAR(20),
        artist_id VARCHAR(20),
        session_id INT,
        location VARCHAR(100),
        user_agent VARCHAR(200)
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender VARCHAR(2),
        level VARCHAR(10)
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(20) PRIMARY KEY,
        title VARCHAR(100),
        artist_id VARCHAR(20),
        year INT,
        duration NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(100),
        location VARCHAR(100),
        latitude DECIMAL, 
        longitude DECIMAL
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday VARCHAR(9),
        start_time_ms BIGINT
    )
""")

# INSERT RECORDS

PREPARED_DATA_DIRS = {
    'songplays': 'data/prepared_data/songplays.csv',
    'users': 'data/prepared_data/users.csv',
    'songs': 'data/prepared_data/songs.csv',
    'artists': 'data/prepared_data/artists.csv',
    'time': 'data/prepared_data/time.csv'
}

songplay_table_insert = (f"""
    COPY songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    FROM stdin DELIMITER ',' CSV HEADER;
""")

user_table_insert = (f"""
    COPY users FROM stdin DELIMITER ',' CSV HEADER;
""")

song_table_insert = (f"""
    COPY songs FROM stdin DELIMITER ',' CSV HEADER;
""")

artist_table_insert = (f"""
    COPY artists FROM stdin DELIMITER ',' CSV HEADER;
""")


time_table_insert = (f"""
    COPY time FROM stdin DELIMITER ',' CSV HEADER;
""")

# DELETE RECORDS

users_table_delete = """
    DELETE FROM users
    WHERE user_id IN %s
"""

artists_table_delete = """
    DELETE FROM artists
    WHERE artist_id IN %s
"""

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, songs.title AS song, songs.artist_id, songs.duration AS length, artists.name AS artist
    FROM songs
    JOIN artists USING (artist_id)
    WHERE songs.title IN %(song_titles)s
""")

song_select_with_id = ("""
    SELECT songs.song_id
    FROM songs
    WHERE song_id IN %(song_ids)s
""")

# FIND TIME

time_select = ("""
    SELECT start_time, start_time_ms
    FROM time
    WHERE start_time_ms IN %(ts)s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]