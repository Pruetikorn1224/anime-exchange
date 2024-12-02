import requests
import pandas as pd
import time
import os

CLIENT_ID = '02c91975bedd64f2c165d6e45f141b21'

anime_data_fields = ",".join([
    "id", "title", "main_picture", "alternative_titles", "start_date", "end_date",
    "synopsis", "mean", "rank", "popularity", "num_list_users", "num_scoring_users",
    "nsfw", "media_type", "status", "genres", "num_episodes", "start_season", 
    "source", "average_episode_duration", "rating", "recommendations", "studios"
])

anime_list = []

limit = 10         # Number of entries per request (max 100 for MAL API)
offset = 0          # Starting point
batch_size = 1000   # Save data every 1,000 entries
batch_count = 0     # Track the number of entries fetched in the current batch
total_entries = 0   # Track total entries fetched

file_name = 'anime_data.csv'
directory_name = 'dataset'
if os.path.exists(directory_name) == False:
    os.makedirs(directory_name)
file_path = os.path.join(directory_name, file_name)

def fetch_anime_data(offset=0, limit=100):
    url = "https://api.myanimelist.net/v2/anime"
    headers = {'X-MAL-CLIENT-ID': CLIENT_ID}
    params = {
        'offset': offset,
        'limit': limit,
        'fields': anime_data_fields
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, Offset: {offset}, Message: {response.text}, Params: {params}")
        return None
    

while True:
    print(f"Fetching data from offset {offset}...")
    data = fetch_anime_data(offset=offset, limit=limit)
    if not data or 'data' not in data:
        break

    for anime in data['data']:
        node = anime['node']
        anime_list.append({
            'MAL_ID': node.get('id'),
            'Title': node.get('title'),
            'English_Name': node.get('alternative_titles', {}).get('en'),
            'Japanese_Name': node.get('alternative_titles', {}).get('jp'),
            'Image_URL': node.get('main_picture', {}).get('medium'),
            'Genres': ', '.join(genre['name'] for genre in node.get('genres', [])),
            'Start_Date': node.get('start_date'),
            'End_Date': node.get('end_date'),
            'Season': node.get('start_season', {}).get('season'),
            'Year': node.get('start_season', {}).get('year'),
            'Studios': ', '.join(studio['name'] for studio in node.get('studios', [])),
            'Synopsis': node.get('synopsis'),
            'Media_Type': node.get('media_type'),
            'Source': node.get('source'),
            'Airing_Status': node.get('status'),
            'Num_Episodes': node.get('num_episodes'),
            'Average_Episode_Duration': node.get('average_episode_duration'),
            'Rating': node.get('rating'),
            'NSFW': node.get('nsfw'),
            'Score': node.get('mean'),
            'Rank': node.get('rank'),
            'Num_Scoring_Users': node.get('num_scoring_users'),
            'Popularity': node.get('popularity'),
            'Num_List_Users': node.get('num_list_users'),
        })
        batch_count += 1
        total_entries += 1
    
    offset += limit

    if batch_count >= batch_size:
        print(f"Saving intermediate data... Total entries fetched so far: {total_entries}")
        anime_df = pd.DataFrame(anime_list)
        anime_df.to_csv(file_path, mode='a', header=not bool(total_entries - batch_count), index=False)
        anime_list.clear()
        batch_count = 0
        print("Intermediate data saved.")
    
    time.sleep(1)


if anime_list:
    print(f"Saving final data... Total entries fetched: {total_entries}")
    anime_df = pd.DataFrame(anime_list)
    anime_df.to_csv(file_path, mode='a', header=not bool(total_entries - batch_count), index=False)
    print("Final data saved.")