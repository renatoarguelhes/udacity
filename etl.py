import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files

        # Inserted records into artist and song table

def process_song_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)
    df.head(1)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)

		# Inserted records into artist and song table    
    
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

		# filter by NextSong action
    df = df[df['page']=='NextSong']
    t = pd.to_datetime(df['ts'], unit='ms')
    t.head()
 # here you are filtering where page = NextSong in the data frame
		# convert timestamp column to datetime

		
		# insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday], axis=1)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_data.columns = column_labels
    time_df = time_data
    time_df.head()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()

		# load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

		# insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

		# insert songplay records
    for index, row in df.iterrows():
            # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

				
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

				# insert songplay record
        songplay_data =  (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.location, row.userAgent) 
        cur.execute(songplay_table_insert, songplay_data)
# Attetion, it is necessary to converte ts to milisecond otherwise will return an error.

def process_data(cur, conn, filepath, func):
		# get all files matching extension from directory
   """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
        all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

		# get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

		# iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
    print('{}/{} files processed.'.format(i, num_files))

def main():
	conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
	cur = conn.cursor()
	process_data(cur, conn, filepath='data/song_data', func=process_song_file)
	process_data(cur, conn, filepath='data/log_data', func=process_log_file)
	conn.close()
    
if __name__ == "__main__":
    main()
