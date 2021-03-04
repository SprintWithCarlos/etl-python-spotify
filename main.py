import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, time
import datetime
import sqlite3

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
            raise Exception("At leats one of the returned songs does not come form within the last 24 hours")
    return True  

""" EXTRACT STAGE """
DATABASE_LOCATION = "sqlite://my_played_tracks.sqlite"
USER_ID = """ Your username here"""
TOKEN = """ Put your token here """ 

if __name__ == "__main__":
    headers = {
        "Accept":  "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?before={time}".format(time=yesterday_unix_timestamp), headers = headers)
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
    print(song_df) 
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage") 