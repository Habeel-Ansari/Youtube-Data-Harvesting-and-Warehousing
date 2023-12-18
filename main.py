# Import modules
import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import mysql.connector
from datetime import datetime, timezone, timedelta
import re
import requests
from streamlit_lottie import st_lottie

#Function to get channel details
def Channel_details(youtube, Channel_name):
    request = youtube.search().list(
        part="id,snippet",
        channelType="any",
        maxResults=1,
        q=Channel_name
    )
    response = request.execute()
    channel_id = response['items'][0]['snippet']['channelId']

    request1 = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response1 = request1.execute()

    channel_id = response1["items"][0]["id"]
    Channel_name = response1["items"][0]["snippet"]["title"]
    Channel_description = response1["items"][0]["snippet"]["description"]
    Channel_subscribers = int(response1["items"][0]["statistics"]["subscriberCount"])
    Channel_view_Count = int(response1["items"][0]["statistics"]["viewCount"])
    channel_Video_Count = int(response1["items"][0]["statistics"]["videoCount"])
    Channel_Published_Date = response1["items"][0]["snippet"]["publishedAt"]
    Playlist_id = response1["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    return {
        'Channel_id': channel_id,
        'Channel_name': Channel_name,
        'Channel_description': Channel_description,
        'Channel_subscribers': Channel_subscribers,
        'Channel_view_Count': Channel_view_Count,
        'channel_Video_Count': channel_Video_Count,
        'Channel_Published_Date': Channel_Published_Date,
        'Playlist_id': Playlist_id
    }

def Video_id(youtube, playlist_id):
    video_ids = []

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append({'Channel_id': response['items'][0]['snippet']['channelId'], "video_id": item['contentDetails']['videoId']})

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append({"Channel_id": response['items'][0]['snippet']['channelId'], "video_id": item['contentDetails']['videoId']})
        next_page_token = response.get('nextPageToken')
    return video_ids


#Get Video Details
def get_video_details(youtube, video_ids):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'commentCount']
                             }
            video_info = {'video_id': video['id']}

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    video_info[v] = video[k][v]

            # Calculate video duration in seconds
            video_info['video_duration_seconds'] = get_video_duration_seconds(video['contentDetails']['duration'])

            all_video_info.append(video_info)

    return all_video_info

# Function to format the youtube video duration to seconds.
def get_video_duration_seconds(duration):
    # Parse ISO 8601 duration format (PT#H#M#S)
    duration_dict = {}
    for match in re.finditer(r'(\d+)([HMS])', duration):
        value, unit = match.groups()
        duration_dict[unit] = int(value)
    seconds = duration_dict.get('S', 0) + duration_dict.get('M', 0) * 60 + duration_dict.get('H', 0) * 3600

    return seconds


# Function to get YouTube data using the Google API
def get_youtube_data(api_key, channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    ChannelDetails = Channel_details(youtube, channel_id)
    videos = Video_id(youtube, ChannelDetails['Playlist_id'])
    video_ids = [i['video_id'] for i in videos]
    video_details = get_video_details(youtube, video_ids)

    data = {"ChannelDetails": ChannelDetails, "video_details": video_details}
    return data

# Function to insert data into MongoDB
def mongo_insert(data, mongo_url):
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        db = client.youtube
        mycol = db.data
        channel_name = data["ChannelDetails"]["Channel_name"]

        if mycol.find_one({'ChannelDetails.Channel_name': channel_name}) is None:
            mycol.insert_one(data)
            return "Data inserted into MongoDB"
        else:
            mycol.delete_many({'ChannelDetails.Channel_name': channel_name})
            mycol.insert_one(data)
            return "Data exists and updated in MongoDB"
    except Exception as e:
        return f"MongoDB connection error: {e}"
    finally:
        client.close()

# Clean and format the data

def dataChannelClean(channeldetail):
    if isinstance(channeldetail, pd.DataFrame):
        # Filling null values
        channeldetail['Channel_description'].fillna("", inplace=True)

        # Converting to string to int
        channeldetail['Channel_subscribers'] = channeldetail['Channel_subscribers'].fillna(0).astype(int)
        channeldetail['Channel_view_Count'] = channeldetail['Channel_view_Count'].fillna(0).astype(int)
        channeldetail['channel_Video_Count'] = channeldetail['channel_Video_Count'].fillna(0).astype(int)
    else:
        pass

    return channeldetail

def dataVideoClean(videodetail):
    if isinstance(videodetail, pd.DataFrame):
        videodetail['channelTitle'].fillna("", inplace=True)
        videodetail['title'].fillna("", inplace=True)
        videodetail['description'].fillna("", inplace=True)
        videodetail['publishedAt'] = videodetail['publishedAt'].astype(str)
        videodetail['viewCount'] = videodetail['viewCount'].fillna(0).astype(int)
        videodetail['likeCount'] = videodetail['likeCount'].fillna(0).astype(int)
        videodetail['commentCount'] = videodetail['commentCount'].fillna(0).astype(int)
        videodetail['video_duration_seconds'] = (pd.to_datetime(videodetail['publishedAt']) - pd.to_datetime(datetime.now(timezone.utc).isoformat())).dt.total_seconds()
    else:
        pass

    return videodetail

# Inserting the data into mysql.
def mysql_insert(channel_data, video_data, mysql_config):
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()
    cursor.execute("SET time_zone = '+00:00';")

    try:
        # Create ChannelDetails table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ChannelDetails (
                Channel_id VARCHAR(255),
                Channel_name VARCHAR(255),
                Channel_description TEXT,
                Channel_subscribers INT,
                Channel_view_Count INT,
                channel_Video_Count INT,
                Channel_Published_Date TEXT ,
                Playlist_id VARCHAR(255),
                PRIMARY KEY (Channel_id)
            )
        ''')

        # Insert ChannelDetails data
        cursor.execute('''
            INSERT INTO ChannelDetails
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            Channel_name = VALUES(Channel_name),
            Channel_description = VALUES(Channel_description)
        ''', (channel_data['Channel_id'], channel_data['Channel_name'],
              channel_data['Channel_description'], channel_data['Channel_subscribers'],
              channel_data['Channel_view_Count'], channel_data['channel_Video_Count'],
              channel_data['Channel_Published_Date'], channel_data['Playlist_id']))

        # Create videos table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255),
                channelTitle VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                publishedAt TEXT,
                viewCount INT,
                likeCount INT,
                commentCount INT,
                video_duration_seconds INT,
                PRIMARY KEY (video_id)
            )
        ''')

        # Insert Video data with check for existing entries
        for video_row in video_data:
            cursor.execute('''
                INSERT IGNORE INTO videos
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (video_row['video_id'], video_row['channelTitle'], video_row['title'],
                  video_row['description'], video_row['publishedAt'], video_row['viewCount'],
                  video_row['likeCount'], video_row['commentCount'], video_row.get('video_duration_seconds', 0)))

        connection.commit()
        return "Data successfully uploaded to MySQL server"

    except Exception as e:
        return f"Error: {e}"

    finally:
        cursor.close()
        connection.close()





# Function to display data in Streamlit
def display_data(mongo_url):
    client = MongoClient(mongo_url)
    st.subheader("Display Data from MongoDB:")
    data_from_mongo = pd.DataFrame(list(client.youtube.data.find()))
    st.write(data_from_mongo)

def load_lottieurl(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()


# Streamlit UI
def main():
    print("Entering the main function")
    st.set_page_config(page_title="YouTube Data Harvesting and Warehousing", page_icon= ":man_detective:")
    lottie_address = load_lottieurl("https://lottie.host/8a803d48-a4b4-4604-9921-a93779a4e9e4/2d7qtbcpQ2.json")
    st_lottie(lottie_address, height=150)

    st.title("YouTube Data Harvesting and Warehousing")

    # Preset values for API key, MongoDB URL, and MySQL config
    preset_api_key = 'Enter Youtube API Key Here'
    mongo_url = 'mongodb+srv://username:Password@cluster0.vtjucen.mongodb.net/?retryWrites=true&w=majority'
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'sqlpassword',
        'database': 'youtube_database'
    }

    # Input Channel ID
    channel_id = st.text_input("Enter YouTube Channel ID:")

    # MySQL connection
    connection = mysql.connector.connect(**mysql_config)

    if st.button("Submit", key="submit1"):
        server_placeholder = st.empty()
        if channel_id:
            fetch_message = "Fetching and Storing Data. Please wait..."
            server_placeholder.info(fetch_message)
            data = get_youtube_data(preset_api_key, channel_id)

            # Insert data into MongoDB
            mongo_message = mongo_insert(data, mongo_url)
            st.success(mongo_message)

            server_placeholder.empty()

    if st.button("Display Data"):
        display_data(mongo_url)

    if st.button("Upload to Server"):
        server_placeholder = st.empty()
        if channel_id:
            upload_message = "Uploading data to the server. Please wait..."
            server_placeholder.info(upload_message)
            data = get_youtube_data(preset_api_key, channel_id)


        # Insert data into MySQL
            mysql_message = mysql_insert(dataChannelClean(data["ChannelDetails"]),dataVideoClean( data["video_details"]), mysql_config)
            st.success(mysql_message)
            server_placeholder.empty()

    # Dropdowns for querying data
    st.subheader("Query Data:")

    cursor = connection.cursor()

    query = "SELECT DISTINCT Channel_name FROM ChannelDetails"
    cursor.execute(query)
    channel_names = pd.DataFrame(list(cursor.fetchall()))

    cursor.close()

    preset_questions = [
        "Which channel has the most number of subscribers?",
        "Which channel has the least number of subscribers?",
        "Which video has the most number of views?",
        "Which video has the least number of views?",
        "What is the average number of views for all videos?",
        "Which channel has the highest average views per video?",
        "What is the average number of likes per video?",
        "Which video has the highest engagement rate?",
        "What is the distribution of video durations across all channels?",
        "Which day of the week has the highest average view count for videos?"
    ]
    selected_question = st.selectbox("Select Question:", preset_questions)

    if st.button("Submit", key="submit2"):
        server_placeholder = st.empty()
        if not channel_names.empty:
            # Query based on the selected question
            query_status_message = "Querying data. Please wait..."
            server_placeholder.info(query_status_message)

            if selected_question == "Which channel has the most number of subscribers?":
                query = "SELECT Channel_name, Channel_subscribers FROM ChannelDetails ORDER BY Channel_subscribers DESC LIMIT 1"
                result = pd.read_sql(query, connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "Which channel has the least number of subscribers?":
                query = "SELECT Channel_name, Channel_subscribers FROM ChannelDetails ORDER BY Channel_subscribers ASC LIMIT 1;"
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "Which video has the most number of views?":
                query = """SELECT videos.title, videos.viewCount, ChannelDetails.Channel_name
                    FROM videos
                    JOIN ChannelDetails ON videos.channelTitle = ChannelDetails.Channel_name
                    ORDER BY videos.viewCount DESC
                    LIMIT 1;"""
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "Which video has the least number of views?":
                query = """SELECT videos.title, videos.viewCount, ChannelDetails.Channel_name
                    FROM videos
                    JOIN ChannelDetails ON videos.channelTitle = ChannelDetails.Channel_name
                    ORDER BY videos.viewCount ASC
                    LIMIT 1;"""
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "What is the average number of views for all videos?":
                query = "SELECT AVG(viewCount) FROM videos;"
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "Which channel has the highest average views per video?":
                query = ("SELECT channelTitle as Channel_name, AVG(viewCount) as avg_views_per_video FROM videos JOIN ChannelDetails ON videos.ChannelTitle = ChannelDetails.Channel_name GROUP BY Channel_name ORDER BY avg_views_per_video DESC LIMIT 1;")
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "What is the average number of likes per video?":
                query = "SELECT AVG(likeCount) FROM videos;"
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "Which video has the highest engagement rate?":
                query = "SELECT title, (likeCount + commentCount) / viewCount AS engagement_rate FROM videos ORDER BY engagement_rate DESC LIMIT 1;"
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "What is the distribution of video durations across all channels?":
                query = "SELECT Channel_name, AVG(video_duration_seconds) AS avg_video_duration_seconds FROM videos JOIN ChannelDetails ON videos.channelTitle = ChannelDetails.Channel_name GROUP BY Channel_name;"
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            elif selected_question == "Which day of the week has the highest average view count for videos?":
                query = "SELECT DAYNAME(publishedAt) AS day_of_week, AVG(viewCount) AS avg_views FROM videos GROUP BY day_of_week ORDER BY avg_views DESC LIMIT 1;"
                result = pd.read_sql(query,connection)

                st.write("Result:")
                st.write(result)

            server_placeholder.empty()
        else:
            st.error("No channel names available. Ensure the DataFrame is correctly populated.")

    # Close the MySQL connection after use
    connection.close()

if __name__ == "__main__":
    main()
