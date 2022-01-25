import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def _insert_into_songs(cur, df, conn):
    """Populates the `songs` table.
    
    Extracts the fields required to populate `songs` table, 
    excluds the song records that already exists in the `songs` table
    and finally inserts the records in table using `copy_expert` method.
    """
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # `songs` table has `song_id` as primary key,
    # inserting records where `song_id` already exists in the table will result in an error.
    # Exclude such records from our dataset
    df_existing_songs = pd.read_sql(song_select_with_id, params={'song_ids': tuple(song_data.song_id)}, con=conn)
    song_data = song_data[~song_data.song_id.isin(df_existing_songs.song_id)]
    
    song_data.to_csv(PREPARED_DATA_DIRS['songs'], index=False)
    cur.copy_expert(song_table_insert, open(PREPARED_DATA_DIRS['songs'], 'r'))
    

def _upsert_into_artists(cur, df):
    """Populates the `artists` table
    
    Extracts the fields required to populate `artists` table.
    The existing `artists` records are updated by deleting the existing ones 
    and inserting the new records with updated values.
    """
    
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude']]
    
    # Some fields in `artists` data such as `artist_name`, `artist_location`, etc. can change 
    # For example, 'Lady Antebellum' recently changed their name to 'Lady A'
    # This can be handled by deleting the existing `artists` records with matching `artist_id`
    # ..and these records will get automatically updated when `artist_data` will be added to the `artists` table.
    cur.execute(artists_table_delete, [tuple(artist_data.artist_id)])
    
    artist_data.to_csv(PREPARED_DATA_DIRS['artists'], index=False)
    cur.copy_expert(artist_table_insert, open(PREPARED_DATA_DIRS['artists'], 'r'))


def process_song_file(cur, filepath, conn):
    """Processes song files and extracts data fields required to populate `songs` & `artists` tables."""
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    _insert_into_songs(cur, df, conn)
    
    # upsert artist record
    _upsert_into_artists(cur, df)


def _insert_into_time(df, cur, conn):
    """Populates the `time` table using `ts` field of in log files.
    
    The related timestamp units are extracted from the `start_time` field 
    obtained by converting milliseconds in `ts` field to datetime.
    """
    
    # convert timestamp column to datetime and extract other timestamp parameters
    df['start_time'] = pd.to_datetime(df.ts, unit='ms')
    time_df = pd.DataFrame({'start_time': df['start_time'], 
                            'hour': df['start_time'].dt.hour, 
                            'day': df['start_time'].dt.day,
                            'week': df['start_time'].dt.week,
                            'month': df['start_time'].dt.month,
                            'year': df['start_time'].dt.year,
                            'weekday': df['start_time'].dt.weekday_name, 
                            'start_time_ms': df['ts']})
    
    # Excluding the time records that already exist in the DB to avoid primary key clashes
    df_existing_time = pd.read_sql(time_select, params={'ts': tuple(time_df['start_time_ms'])}, con=conn)
    time_df = time_df[~time_df.start_time_ms.isin(df_existing_time.start_time_ms)]
    
    # Remove duplicate date values
    time_df = time_df.drop_duplicates()
    
    # insert time data records
    time_df.to_csv(PREPARED_DATA_DIRS['time'], index=False)
    cur.copy_expert(time_table_insert, open(PREPARED_DATA_DIRS['time'], 'r'))
    
    return df
    

def _upsert_into_users(df, cur, conn):
    """Populates the `users` table.
    
    Extract the fields required for `users` table from log json files.
    The existing `users` records are updated by deleting the existing ones 
    and inserting the new records with updated values.
    """
    # If same user has multiple records in a same file, take the latest one according to the timestamp of event
    user_df = df.sort_values(by=['userId', 'ts']).drop_duplicates(subset=['userId'], keep='last')
    
    user_df = user_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.userId = user_df.userId.astype(int)
    user_df = user_df.rename(columns={'userId': 'user_id', 'firstName': 'first_name', 'lastName': 'last_name'})
    
    # Some fields in `users` data such as `level`, `last_name`, etc. can change 
    # This can be handled by deleting the existing `artists` records with matching `user_id`
    # ..and these records will get automatically updated when data in `user_df` will be added to the `users` table.
    cur.execute(users_table_delete, (tuple(user_df.user_id), ))
    
    user_df.to_csv(PREPARED_DATA_DIRS['users'], index=False)
    cur.copy_expert(user_table_insert, open(PREPARED_DATA_DIRS['users'], 'r'))
    
    
def process_log_file(cur, filepath, conn):
    """Processes song files and extracts data fields required to populate `users`, `time` & `songplays` tables."""
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']
    
    # Extract and Insert time data into DB
    df = _insert_into_time(df, cur, conn)

    # Upsert records into `users` table
    _upsert_into_users(df, cur, conn)
    
    # Get `songs` and `artists` primary key value to be used as a foreign key in `songplays` table
    df_songs_info = pd.read_sql(song_select, params={'song_titles': tuple(df.song)}, con=conn)
    df_songplay = df.merge(df_songs_info, how='left', on=['song', 'artist', 'length'])[[
        'start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent'
    ]].rename(columns={'userId': 'user_id', 'sessionId': 'session_id', 'userAgent': 'user_agent'})
    
    # Insert into songplays tables
    df_songplay.to_csv(PREPARED_DATA_DIRS['songplays'], index=False)
    cur.copy_expert(songplay_table_insert, open(PREPARED_DATA_DIRS['songplays'], 'r'))


def process_data(cur, conn, filepath, func):
    """Processes songs and logs data files to populate the tables in db.
        
    Iterates over songs and logs directories and prepares an exhaustive list of directories,
    then processes each file using respective function to populate the tables using these files.
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        if '.ipynb_checkpoints' in dirs:
            dirs.remove('.ipynb_checkpoints')
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, conn)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    pass


if __name__ == "__main__":
    main()