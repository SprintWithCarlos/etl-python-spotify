import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, time
import datetime
import sqlite3
import schedule
from time import sleep
import os
from dotenv import load_dotenv
load_dotenv()

""" FUNCTIONS DEFINITION """
def check_if_valid_data(df: pd.DataFrame) -> bool:
    #Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    # Primary Key Check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check error")
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null value found")
    #Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come form within the last 24 hours")
    return True   

""" EXTRACT STAGE """
DATABASE_LOCATION = os.environ.get('DATABASE_LOCATION')
USER_ID = os.environ.get('SPOTIFY_USER')
TOKEN = os.environ.get('SPOTIFY_TOKEN')
def cronjob():
    if __name__ == "__main__":
        headers = {
            "Accept":  "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token=TOKEN)
        }
        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
        data = r.json()
    
        song_names = []
        artists_names = []
        played_at_list = []
        timestamps = []

        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artists_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])
        
        song_dict = {
            "song_name": song_names,
            "artlist_name": artists_names,
            "played_at": played_at_list,
            "timestamp": timestamps
        }

        song_df = pd.DataFrame(song_dict, columns = ["song_name", "artlist_name", "played_at", "timestamp"])
        """ TRANSFORM STAGE """
        if check_if_valid_data(song_df):
            print("Data valid, proceed to Load stage") 
        """ LOAD STAGE """
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect('my_played_tracks.db')
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """

        cursor.execute(sql_query)
        print("Opened database successfully")
        #song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
        try:
            song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
        except:
            print("Data already exists in the database")
            
        conn.close()
        print("Closed database connection successfully")

schedule.every().day.at("00:00").do(cronjob)

while True:
    schedule.run_pending()
    sleep(10) 