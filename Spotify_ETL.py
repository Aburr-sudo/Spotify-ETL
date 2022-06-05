#!/usr/bin/env python
# coding: utf-8


import json
import requests
import datetime
import pandas as pd
import psycopg2
import sqlalchemy
import pandas as pd
import secrets
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import datetime
# https://developer.spotify.com/console/get-recently-played/

def get_songs_df(data: dict)-> pd.DataFrame:
    artist_ids = []
    song_names = []
    artist_names =[]
    played_at_list = []
    timestamps =[]


    for song in data["items"]:
        artist_ids.append(song['track']['album']['artists'][0]['id'])
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])

    song_dict = {
    "artist_ids": artist_ids,
    "song_names": song_names,
    "artist_names": artist_names,
    "played_at_list": played_at_list,
    }

    song_df = pd.DataFrame(song_dict, columns =["song_names","artist_names","artist_ids", "played_at_list"])
    return song_df

# for analysis stage, get most freq artists then pull this
def get_artist_info(artist_ids: list)-> pd.DataFrame:
    genres = []
    names = []
    popularity = []
    images = []
    headers = {
        "Accept":"application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
    
    for artist_id in artist_ids:
        artist_req = requests.get("https://api.spotify.com/v1/artists/{a_id}".format(a_id=artist_id), 
                    headers=headers)
        artist_info = artist_req.json()
        genres.append(artist_info['genres'])
        names.append(artist_info['name'])
        popularity.append(artist_info['popularity'])
        images.append(artist_info['images'][1]['url'])
    
    artist_dict = {
    "artist_id": artist_ids,
    "genres": genres,
    "artist_name": names,
    "popularity": popularity,
    "image": images
    }
    artist_df = pd.DataFrame(artist_dict, columns =["artist_id","artist_name","genres", "popularity","image"])
    return artist_df

def refresh_access_token(refresh_token: str, auth_64: str) -> str:
    ###### refresh access token
    
    refresh_headers = {
        'Authorization': auth_64,
    }

    refresh_data = {
        "grant_type": "refresh_token",
        'refresh_token': refresh_token,
    }
    refresh = requests.post('https://accounts.spotify.com/api/token',headers=refresh_headers,data=refresh_data)
    refresh_content = refresh.json()
    access_token = refresh_content['access_token']
    return access_token


def request_recently_played_tracks(TOKEN: str) -> dict:
    headers = {
        "Accept":"application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) *1000
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), 
                    headers=headers)

    data = r.json()
    return data




def adjust_timezone(df : pd.DataFrame, time_column) -> pd.DataFrame:
    df[time_column] = df[time_column].dt.tz_convert('Australia/Melbourne')
    df['date_listened'] = df[time_column].dt.date
    return df


def transform_timedata(tracks_dataframe: pd.DataFrame) -> pd.DataFrame:
    tracks_dataframe['played_at_list'] = pd.to_datetime(tracks_dataframe['played_at_list'])
    tracks_dataframe = adjust_timezone(tracks_dataframe, 'played_at_list')
    return tracks_dataframe




######## TRANSFORM
def check_if_valid_data(df: pd.DataFrame) -> bool:
    ### for this use case, empty is valid, no songs
    if df.empty:
        print('No songs listened to. Finished execution')
        return False
    ### PRIMARY KEY CHECK
    if pd.Series(df['played_at_list']).is_unique:
        pass
    else:
        raise Exception("Primary Key check failed")
    ### NULL VALUES
    if df.isnull().values.any():
        raise Exception("Null value found")
        
    ## CHECK TIME FRAME - must be from yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamps = df["date_listened"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            print('All songs in valid date range')
            pass
            Exception("At least one song was not listened to yesterday")
    return True    



def push_to_db(df: pd.DataFrame, table: str) -> None:
    engine = sqlalchemy.create_engine('postgresql://XXX:XXXX@XXX:XXX/XXX')
    try:
        df.to_sql(table, engine, index=False, if_exists='append')
        print('Data successfully pushed to database')
    except:
        print('could not push to database')



def transform_load(table: str,df: pd.DataFrame, name: str)-> None:        
    if check_if_valid_data(song_df):
        print(name+' Contains valid data, proceeding to load stage')
        push_to_db(df, table)
    else:
        print(name+' Contains invalid data, aborting load stage')




def get_historical_data(table):
    engine = sqlalchemy.create_engine('postgresql://XXX:XXXX@XXX:XXX/XXX')
    query = """
        SELECT *
        FROM {};
    """.format(table)

    existing_entries = pd.read_sql(query, con=engine)
    return existing_entries



def check_against_historical_data(df, hist_df, pk):
    valid_entries = df[~df[pk].isin(hist_df[pk])]
    if(len(valid_entries)==0):
        print('no new data')
    return valid_entries


if __name__ == "__main__":
    ## secrets
    refresh_token_key = secrets.refresh_token
    auth_64 = secrets.auth_64
    TOKEN = refresh_access_token(refresh_token_key, auth_64)
    print('token refreshed')
    track_data = request_recently_played_tracks(TOKEN)
    song_df = get_songs_df(track_data)
    artist_ids = list(set(song_df['artist_ids']))
    artist_df  = get_artist_info(artist_ids)
    song_df = transform_timedata(song_df)
    transform_load("played_tracks",song_df, 'Song data')
    transform_load("artists",artist_df, 'Artist data')
