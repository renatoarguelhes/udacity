songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
# CREATE TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES TIME(start_time),
    user_id INT NOT NULL REFERENCES users(user_id),
    level TEXT,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    location TEXT,
    user_agent TEXT
);
""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender VARCHAR,
    level TEXT
);
""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration NUMERIC NOT NULL
);
""")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    artist_name TEXT NOT NULL,
    artist_location TEXT,
    artist_latitude REAL,
    artist_longitude REAL
);
""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR
);
""")
# INSERT RECORDS
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, location, user_agent) \
VALUES (%s, %s, %s, %s, %s, %s, %s);
""")
user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level) \
    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;    
""")
song_table_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration) \
    VALUES (%s, %s, %s, %s, %s);
""")
artist_table_insert = ("""
INSERT INTO artists (
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude) \
    VALUES (%s, %s, %s, %s, %s);
""")
time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday) \
    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO UPDATE 
                 SET hour=EXCLUDED.hour,day=EXCLUDED.day,week=EXCLUDED.week, month=EXCLUDED.month,year=EXCLUDED.year,weekday=EXCLUDED.weekday;
""")
# FIND SONGS
song_select = ("""
SELECT songs.song_id, artists.artist_id FROM artists JOIN songs ON artists.artist_id = songs.artist_id WHERE songs.title = %s
AND artists.artist_name = %s AND songs.duration = %s
""")
# QUERY LISTS
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
