from dotenv import load_dotenv
import os
import base64
import requests
from requests import post, get
import json
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator

load_dotenv()

client_id = os.getenv("CLIENT_ID") 
client_secret = os.getenv("CLIENT_SECRET")
refresh_token = os.getenv("REFRESH_TOKEN")

def get_token():
    global refresh_token
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "client_credentials",
        "scope": "user-read-recently-played"
    }

    result = requests.post(url, headers=headers, data=data)
    json_result = json.loads(result.content)
    
    if 'access_token' in json_result:
        token = json_result['access_token']
        return token
    elif 'refresh_token' in json_result:
        new_access_token, new_refresh_token = refresh_the_token(refresh_token)
        refresh_token = new_refresh_token
        return new_access_token
    else:
        raise ValueError("Could not obtain access token or refresh token")

def refresh_the_token(refresh_token):
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = post(url, headers=headers, data=data)
    response_data = response.json()
    new_access_token = response_data.get('access_token')
    new_refresh_token = response_data.get('refresh_token', refresh_token)
    return new_access_token, new_refresh_token

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def validate_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("DataFrame is empty")
        return False

    if pd.Series(df['Track ID']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    if df.isnull().values.any():
        raise Exception("Null values found")
    
    return True

def get_playlist_tracks(token, playlist_id):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    headers = {"Authorization": "Bearer " + token}
    result = requests.get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result["items"]

def process_and_clean_data():
    def get_song_info(song, idx):
        try:
            if song['track'] is not None and song['track']['id'] is not None:
                track_id = song['track']['id']
                track_name = song['track']['name']
                artist_name = song['track']['artists'][0]['name']
                added_at = song['added_at']
                album_name = song['track']['album']['name']
                release_date = song['track']['album']['release_date']
                type_of_album = song['track']['album']['album_type']
                popularity = song['track']['popularity']

                return{
                    "Index": idx + 1,
                    "Track ID": track_id,
                    "Track Name": track_name,
                    "Artist Name": artist_name,
                    "Album Name": album_name,
                    "Release Date": release_date,
                    "Added At": added_at,
                    "Type of Album": type_of_album,
                    "Popularity": popularity
                }
            else:
                print(f"Song {idx + 1} cannot be played or has missing information")
                
                return{
                    "Index": idx + 1,
                    "Track ID": None,
                    "Track Name": None,
                    "Artist Name": None,
                    "Album Name": None,
                    "Release Date": None,
                    "Added At": None,
                    "Type of Album": None,
                    "Popularity": None,
                }
        except Exception as e:
            print(f"An error occurred while processing song {idx + 1}: {e}")

            return{
                "Index": idx + 1,
                "Track ID": None,
                "Track Name": None,
                "Artist Name": None,
                "Album Name": None,
                "Release Date": None,
                "Added At": None,
                "Type of Album": None,
                "Popularity": None,
            }

    token = get_token()
    playlist_id = "76z1HwJLDza5jwnD0B4KHs"   

    songs = get_playlist_tracks(token, playlist_id)
    songs_info = [get_song_info(song, idx) for idx, song in enumerate(songs)]

    df = pd.DataFrame(songs_info)

    df = df.dropna().reset_index(drop=True)

    popularity_bins = [0, 1, 20, 40, 60, 80, 100]
    popularity_labels = ['No Data', 'Very Low', 'Low', 'Moderate', 'High', 'Very High']
    df['Popularity Category'] = pd.cut(df['Popularity'], bins=popularity_bins, labels=popularity_labels, right=False)

    def process_datetime(datetime_str):
        if datetime_str is not None:
            cleaned_datetime = datetime_str.replace('T', ' ').replace('Z', '')
            date_part, _ = cleaned_datetime.split(' ')
            return date_part
        else:
            return None
        
    if 'Added At' in df.columns:
        df['Date Added'] = df['Added At'].apply(process_datetime)
        df.drop(columns=['Added At'], inplace=True)
    else:
        print("Column 'Added At' not found in DataFrame")

    if validate_data(df):
        pass

    dbname = "Playlist Spotify Database"
    user = "postgres"
    password = "030811"
    host = "host.docker.internal"
    port = "5432"

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    cur = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS songs (
        "Index" SERIAL,
        "Track ID" VARCHAR(255) PRIMARY KEY,
        "Track Name" VARCHAR(255),
        "Artist Name" VARCHAR(255),
        "Album Name" VARCHAR(255),
        "Release Date" VARCHAR(50),
        "Type of Album" VARCHAR(50),
        "Popularity" INT,
        "Popularity Category" VARCHAR(20),
        "Date_added" DATE
    );
    """
    cur.execute(create_table_query)
    print("Table created successfully")

    df.rename(columns={"Added At": "Date_added"}, inplace=True)

    df['Date Added'] = pd.to_datetime(df['Date Added'])

    insert_query = """
    INSERT INTO songs ("Track ID", "Track Name", "Artist Name", "Album Name", "Release Date", "Type of Album", "Popularity", "Popularity Category", "Date_added")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("Track ID") DO NOTHING;
    """

    for index, row in df.iterrows():
        values = (
            row["Track ID"],
            row["Track Name"],
            row["Artist Name"],
            row["Album Name"],
            row["Release Date"],
            row["Type of Album"],
            row["Popularity"],
            row["Popularity Category"],
            row["Date Added"]
        )
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 19),
    'email': ['Duong.NM216917@sis.hust.edu.vn'], 
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True, 
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'spotify_data_pipeline',
    default_args=default_args,
    description='A DAG to update Spotify data daily',
    schedule_interval=timedelta(days=1),
)

get_token_task = PythonOperator(
    task_id='get_token',
    python_callable=get_token,
    dag=dag,
)

refresh_token_task = PythonOperator(
    task_id='refresh_token',
    python_callable=refresh_the_token,
    op_kwargs={'refresh_token': refresh_token},
    dag=dag,
)

get_auth_header_task = PythonOperator(
    task_id='get_auth_header',
    python_callable=get_auth_header,
    op_kwargs={'token': '{{ task_instance.xcom_pull(task_ids="get_token_task") }}'},  
    dag=dag,
)

get_playlist_tracks_task = PythonOperator(
    task_id='get_playlist_tracks',
    python_callable=get_playlist_tracks,
    op_kwargs={'token': get_token(), 'playlist_id': '76z1HwJLDza5jwnD0B4KHs'},
    dag=dag,
)

process_and_clean_data_task = PythonOperator(
    task_id='process_and_clean_data_task',
    python_callable=process_and_clean_data,
    dag=dag,
)

email = EmailOperator(
    task_id='send_email',
    to='Duong.NM216917@sis.hust.edu.vn',
    subject='Airflow Alert',
    html_content='<p>Task finished successfully</p>',
    dag=dag,
)

get_token_task >> [refresh_token_task, get_auth_header_task]
refresh_token_task >> get_auth_header_task
get_auth_header_task >> get_playlist_tracks_task >> process_and_clean_data_task >> email
