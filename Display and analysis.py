#!/usr/bin/env python
# coding: utf-8


import sqlalchemy
import psycopg2
import psycopg2.extras
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from IPython.display import Image
from IPython.core.display import HTML 


import datetime

def adjust_timezone(df : pd.DataFrame, time_column) -> pd.DataFrame:
    df[time_column] = df[time_column].dt.tz_convert('Australia/Melbourne')
    df['date_listened'] = df[time_column].dt.date
    return df

def transform_timedata(tracks_dataframe: pd.DataFrame) -> pd.DataFrame:
    tracks_dataframe['played_at_list'] = pd.to_datetime(tracks_dataframe['played_at_list'])
    tracks_dataframe = adjust_timezone(tracks_dataframe, 'played_at_list')
    return tracks_dataframe


conn = psycopg2.connect(
    host="XXXX",
    database="XXX",
    user="XXX",
    password="XXXX",
    port="XXXX")




fields = "song_names, COUNT(song_names) AS freq, artist_names, artist_ids, played_at_list, date_listened"
table = "played_tracks"
conditions = "EXTRACT(month from played_at_list) =  EXTRACT(month from CURRENT_TIMESTAMP)"
group_by = "song_names, artist_names, artist_ids, played_at_list, date_listened"
order_by = "freq DESC"

most_freq_current_mnth = (f"SELECT {fields} "
       f"FROM {table} "
       f"WHERE {conditions} "
       f"GROUP BY {group_by} "
       f"ORDER BY {order_by};")

most_freq_prev_mnth = (f"SELECT {fields} "
       f"FROM {table} "
       f"WHERE {conditions} -1 "
       f"GROUP BY {group_by} "
       f"ORDER BY {order_by};")



import pandas.io.sql as psql
connection = psycopg2.connect("host=XXX dbname=XXX user=XXXX password=XXXXX")

tracks_dataframe = psql.read_sql('SELECT * FROM played_tracks', connection)
artist_dataframe = psql.read_sql('SELECT * FROM artists', connection)
tracks_dataframe = transform_timedata(tracks_dataframe)


# In[61]:


def most_popular_genres(artist_info):
    genre_dict = {}
    genre_list = list(artist_info['genres'])
    for item in genre_list:
        entry = (item.split(','))
        for genre in entry:
            genre = genre.strip("{}")
            genre = genre.replace('"','')
            genre = genre.strip()
            if genre == '':
                genre = 'no genre'  
            if genre not in genre_dict:
                genre_dict[genre] = 1
            else:
                genre_dict[genre] += 1
    return genre_dict


# In[62]:


def get_most_pop_artist_query():
    fields = "artists.artist_name, artists.image, COUNT(played_tracks.artist_names) AS freq"
    table = "artists"
    join_conditions = "played_tracks ON played_tracks.artist_ids = artists.artist_id"
    group_by = "artists.artist_name, artists.image"
    order_by = "freq DESC"
    limit_by = "1"

    query = (f"SELECT {fields} "
           f"FROM {table} "
           f"JOIN {join_conditions} "
           f"GROUP BY {group_by} "
           f"ORDER BY {order_by} "
           f"LIMIT {limit_by};")
    return query

most_popular_artist_query = get_most_pop_artist_query()


# In[85]:


## Analytics
most_played_tracks_this_month =psql.read_sql(most_freq_current_mnth, connection)
most_played_tracks_last_month =psql.read_sql(most_freq_prev_mnth, connection)
most_popular_artist =  psql.read_sql(most_popular_artist_query, connection)
# most listened to this week


### SQL filters by time
#1. songs: most listened to this month
popular_song = most_played_tracks_this_month['song_names'].mode()
artist_list = most_played_tracks_this_month['artist_ids']
#2. artists: most listened to this months
popular_artists = most_played_tracks_this_month['artist_names'].mode()
most_pop_artist = artist_dataframe[artist_dataframe['artist_name'].isin(popular_artists)]
popular_artists = artist_dataframe[artist_dataframe['artist_name'].isin(popular_artists)]



num_unique_songs = most_played_tracks_this_month['song_names'].nunique()
num_unique_artists = most_played_tracks_this_month['artist_names'].nunique()
# get artist info - for time frame
artist_info = artist_dataframe[artist_dataframe['artist_id'].isin(artist_list)]
num_genre = artist_info['genres'].nunique()

## get genre information from artist_info table
genre_dict = most_popular_genres(artist_info)
genre_df = pd.DataFrame(list(genre_dict.items()), columns=['Genre', 'Count'])
most_pop_genre = genre_df['Genre'].max()
# get top ten cats
genre_df = genre_df.sort_values('Count', ascending=False)[:10]

# pi graph of genres
plt.figure(figsize=(12,12))
plt.pie(genre_df['Count'], labels = genre_df['Genre'],autopct= '%1.1f%%')
plt.title('Genres listened to in last 7 days', bbox={'facecolor':'0.8', 'pad':5})

# plt.figsize(10,10)
plt.show() 

#5. get image of most listened to artist
fav_artist_img = most_popular_artist['image'].iloc[0]
fav_artist_name = most_popular_artist['artist_name'].iloc[0]


# plot time listened to, so group by hour
tracks_per_hour = tracks_dataframe.played_at_list.groupby(tracks_dataframe.played_at_list.dt.hour).count()
first_entry = tracks_dataframe['played_at_list'].iloc[0]
last_entry = tracks_dataframe['played_at_list'].iloc[-1]
days = pd.Timedelta(last_entry - first_entry).days
# average out counts , divide by number of days
tracks_per_hour = tracks_per_hour.apply(lambda x : int(x/days))
tracks_per_hour.plot(xlabel = 'Time (24hr)', ylabel ='Songs per hour', kind='bar',
                     title= 'Average song play frequency in the {} days'.format(),
                    figsize=(10,10))
plt.show()



print('This month you have listened to {} songs from {} artists'.format(num_unique_songs, num_unique_artists))
print('Your favourite artist this month is {}'.format(fav_artist_name))
Image(url= fav_artist_img)