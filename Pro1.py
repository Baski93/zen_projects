from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import mysql.connector
from datetime import datetime
from datetime import timedelta
import pandas as pd
import streamlit as st

# 1) API key connection
def Api_connect():
    Api_Id="AIzaSyCRhr5X0RvJF7EJG2cH62skn4AxiW9LtR8"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

# 2) get channels information
def get_channel_info(Channel_id):
    request=youtube.channels().list(
                   part="snippet, ContentDetails, statistics",
                   id=Channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id=i["id"],
                  Subscribers=i['statistics']['subscriberCount'],
                  Views=i["statistics"]["viewCount"],
                  Total_Videos=i["statistics"]["videoCount"],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data

# 3 )get video ids
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

# 4 Get Video Information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channael_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Commends=item['statistics'].get('commentCount'),
                    Fav_Count=item [ 'statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data


# 5 get comment information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50

                )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# 6 get_playlist_details

def get_playlist_details(Channel_id):

        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=Channel_id,
                        maxResults=50,
                        pageToken=next_page_token

                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet'][ 'channelTitle'],
                                Channel_PublishedAT=item['snippet']['publishedAt'],
                                Channel_Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break  
        return All_data

# 7 setup Mangodb Client
Client =pymongo.MongoClient('mongodb://localhost:27017/')
db=Client['Youtube_data']

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    collection=db["channel_details"]
    collection.insert_one({"chennal_information":ch_details,"playlist_information": pl_details,
                           "video_information":vi_details,"comment_information": com_details})
    
    return "Uplode completed successfully"

#Channe details
import mysql.connector
from pymongo import MongoClient
import pandas as pd

def channel_table():
    try:
        mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="Baski6400@",
            database="youtubedata",
            port="3306"
        )
        cursor = mydb.cursor()

        # Corrected the SQL syntax
        drop_query = '''DROP TABLE IF EXISTS channels'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query = '''
        CREATE TABLE IF NOT EXISTS channels (
            Channel_Id varchar(100) primary key ,
            Channel_Name varchar(100),
            Subscribers bigint,
            Views bigint,
            Total_Videos bigint,
            Channel_Description text,
            Playlist_Id varchar(100)
        )'''
        cursor.execute(create_query)
        mydb.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    ch_list = []
    db = MongoClient()["Youtube_data"]
    collection = db["channel_details"]

    for ch_data in collection.find({}, {"_id": 0, "chennal_information": 1}):
        ch_list.append(ch_data["chennal_information"])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT IGNORE INTO channels
            (Channel_Id, Channel_Name, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row['Channel_Id'],
            row['Channel_Name'],
            row['Subscribers'], 
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")

    mydb.close()

#play list
import mysql.connector
from datetime import datetime

def playlist_table():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Baski6400@",
        database="youtubedata",
        port="3306"
    )

    cursor = mydb.cursor()
    drop_query = '''DROP TABLE IF EXISTS playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''
        CREATE TABLE IF NOT EXISTS playlists (
           Playlist_Id varchar(100) PRIMARY KEY,
            Title varchar(255),
            Channel_Id varchar(100),
            Channel_Name varchar(100),
            Channel_PublishedAT timestamp,
            Channel_Video_Count int
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=Client["Youtube_data"]
    collection=db["channel_details"]

    for pl_data in  collection.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=pd.DataFrame(pl_list) 

    for index, row in df1.iterrows():
        # Convert 'Channel_PublishedAT' to the correct datetime format
        published_at = datetime.strptime(row['Channel_PublishedAT'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        # Check if the entry with the Playlist_Id already exists
        check_query = "SELECT * FROM playlists WHERE Playlist_Id = %s"
        cursor.execute(check_query, (row['Playlist_Id'],))
        result = cursor.fetchone()

        if not result:
            # Entry doesn't exist, perform the insertion
            insert_query = '''
                INSERT INTO playlists (
                                       Playlist_Id,
                                       Title,
                                       Channel_Id,
                                       Channel_Name,
                                       Channel_PublishedAT,
                                       Channel_Video_Count
                                    )
                VALUES (%s, %s, %s, %s, %s, %s)'''

            values = (
                row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                published_at,
                row['Channel_Video_Count']
            )

            cursor.execute(insert_query, values)
            mydb.commit()


#videos Db 
import mysql.connector
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from datetime import timedelta


def videos_table():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Baski6400@",
        database="youtubedata",
        port="3306"
    )

    cursor = mydb.cursor()

    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            Channael_Name varchar(100),
            Channel_Id varchar(100),
            Video_Id varchar(30) PRIMARY KEY,
            Title varchar(150),
            Tags text,
            Thumbnail varchar(200),
            Description text,
            Published_Date datetime,
            Duration TIME,
            Views bigint,
            Likes bigint,
            Commends int,
            Fav_Count int,
            Definition varchar(10),
            Caption_Status varchar(50)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

    vi_list = []
    db = MongoClient()["Youtube_data"]
    collection = db["channel_details"]

    for vi_data in collection.find({}, {"_id": 0, "video_information": 1}):
        for video_info in vi_data.get("video_information", []):
            vi_list.append(video_info)

    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        try:
            # Convert Published_Date string to datetime object
            published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            published_date = None

        # Convert duration format to timedelta
        duration = timedelta(seconds=pd.to_timedelta(row['Duration']).seconds)

        # Convert Tags list to a string
        tags_str = str(row['Tags'])

        insert_query = '''INSERT INTO videos (
            Channael_Name,
            Channel_Id,
            Video_Id,
            Title,
            Tags,
            Thumbnail,
            Description,
            Published_Date,
            Duration,
            Views,
            Likes,
            Commends,
            Fav_Count,
            Definition,
            Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (
            row['Channael_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Title'],
            tags_str,  # Use the converted string
            row['Thumbnail'],
            row['Description'],
            published_date,
            duration,  # Use the converted duration
            row['Views'],
            row['Likes'],
            row['Commends'],
            row['Fav_Count'],
            row['Definition'],
            row['Caption_Status']
        )

        print("Values to be inserted:", values)

        cursor.execute(insert_query, values)
        mydb.commit()

    

#COMMENT DB CREATION

#comments 
def comments_table():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Baski6400@",
        database="youtubedata",
        port="3306"
    )

    cursor = mydb.cursor()
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''
        CREATE TABLE IF NOT EXISTS comments (Comment_Id varchar(100) primary key,
                                            Video_Id varchar(50),
                                            Comment_Text text,
                                            Comment_Author varchar(150),
                                            Comment_Published_Date timestamp
                                            )'''
    cursor.execute(create_query)
    mydb.commit()

    #18) COMMENT DATA FRAME

    com_list=[]
    db=Client["Youtube_data"]
    collection=db["channel_details"]

    for com_data in  collection.find({},{"_id":0,"comment_information":1}):
            for i in range (len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    # ... (your existing code)

    insert_query = '''
        INSERT INTO comments (
            Comment_Id,
            Video_Id,
            Comment_Text,
            Comment_Author,
            Comment_Published_Date
        ) VALUES (%s, %s, %s, %s, %s)
    '''

    for index, row in df3.iterrows():
        try:
            # Convert Comment_Published_Date string to datetime object
            comment_published_date = datetime.strptime(row['Comment_Published_Date'], '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            comment_published_date = None

        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            comment_published_date
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as e:
            # Duplicate entry error, handle accordingly (e.g., update existing record)
            update_query = '''
                UPDATE comments
                SET Comment_Text = %s,
                    Comment_Author = %s,
                    Comment_Published_Date = %s
                WHERE Comment_Id = %s
            '''

            update_values = (
                row['Comment_Text'],
                row['Comment_Author'],
                comment_published_date,
                row['Comment_Id']
            )

            cursor.execute(update_query, update_values)
            mydb.commit()
           

def tables():
    comments_table()
    playlist_table()
    videos_table()
    channel_table()

    return "tables created successfully"

def show_channels_table():
    ch_list = []
    db = MongoClient()["Youtube_data"]
    collection = db["channel_details"]

    for ch_data in collection.find({}, {"_id": 0, "chennal_information": 1}):
        ch_list.append(ch_data["chennal_information"])

    df = st.dataframe(ch_list)
    return df


#show_channels_table()

def show_playlist_table():
    pl_list=[]
    db=Client["Youtube_data"]
    collection=db["channel_details"]

    for pl_data in  collection.find({},{"_id":0,"playlist_information":1}):
        for i in range (len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list) 
    return df1



def show_video_table():
    vi_list = []
    db = MongoClient()["Youtube_data"]
    collection = db["channel_details"]

    for vi_data in collection.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
         vi_list.append(vi_data["video_information"][i])
    df2 = st.dataframe(vi_list)
    return df2


def show_comment_table():
    com_list=[]
    db=Client["Youtube_data"]
    collection=db["channel_details"]

    for com_data in  collection.find({},{"_id":0,"comment_information":1}):
            for i in range (len(com_data["comment_information"])):
                com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3


#Streanlit code

with st.sidebar:
    st.title(":violet[YOUTUBE DATA HARVESTING AND WAREHOUSING - BASHKAR T]")
    st.header("SKILLS GAINED")
    st.caption("Python Scripting")
    st.caption("MongoDB")
    st.caption("Data Collection")
    st.caption("Data Management")
    st.caption("MySql for Data Management")
    st.caption("Mongo for Data Management")
    st.caption("VSCODE - IDLE")

channel_id=st.text_input("Enter The Channel ID")

if st.button("FETCH AND RETAIN DATA"):
    ch_ids=[]
    db=Client["Youtube_data"]
    collection=db["channel_details"]
    for ch_data in collection.find({}, {"_id": 0, "chennal_information": 1}):
        ch_ids.append(ch_data["chennal_information"]["Channel_Id"])

        if channel_id in ch_ids:
            st.success("The Channel Id is Already Exists")

        else:
            insert=channel_details(channel_id)
            st.success(insert)

if st.button("TRANSFER TO SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("PICK THE TABLE FOR VIEWING",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlist_table()

elif show_table== "VIDEOS":
    show_video_table()

elif show_table== "COMMENTS":
    show_comment_table()


#Sql Connection

mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Baski6400@",
        database="youtubedata",
        port="3306"
    )
cursor = mydb.cursor()

question=st.selectbox("PICK YOUR QUERY IN THIS STATEMENT",("1.LIST ALL VIDEO NAMES AND THEIR ASSOCIATED CHANNELS",
                                                           "2.IDENTIFY CHANNELS WITH THE MOST VIDEOS AND THEIR COUNT",
                                                           "3.DISPLAY TOP 10 MOST VIEWED VIDEOS AND THEIR CHANNELS",
                                                           "4.COUNT COMMENTS ON EACH VIDEO WITH RESPECTIVE NAMES",
                                                           "5.FIND VIDEOS WITH THE MOST LIKES AND THEIR CHANNELS",
                                                           "6.SHOW TOTAL LIKES AND DISLIKES FOR EACH VIDEO AND NAMES",
                                                           "7.SUM UP VIEWS FOR EACH CHANNEL AND THEIR NAMES",
                                                           "8.LIST CHANNEL NAMES WITH VIDEOS PUBLISHED IN 2022",
                                                           "9.CALCULATE AVERAGE DURATION OF VIDEOS IN EACH CHANNEL",
                                                           "10.DETERMINE VIDEOS WITH HIGHEST COMMENTS AND CHANNEL NAMES"))

if question =="1.LIST ALL VIDEO NAMES AND THEIR ASSOCIATED CHANNELS":

    query1 = '''SELECT title AS videos, Channael_Name AS channelname FROM videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["Video_Title", "Channel_Name"])
    df
    st.write(df)

elif question =="2.IDENTIFY CHANNELS WITH THE MOST VIDEOS AND THEIR COUNT":

    query2 = '''select Channel_Name as channelname,Total_Videos as No_Videos from Channels order by Total_Videos desc'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videosd"])
    df2
    st.write(df2)

elif question =="3.DISPLAY TOP 10 MOST VIEWED VIDEOS AND THEIR CHANNELS":
    query3 = '''SELECT Views as views, Channael_Name as channelname, Title as videotitle FROM videos WHERE Views IS NOT NULL ORDER BY views DESC LIMIT 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name","videotitle"])
    df3
    st.write(df3) 

elif question =="4.COUNT COMMENTS ON EACH VIDEO WITH RESPECTIVE NAMES":
    query4 = '''SELECT Commends as No_comments, Title as videotitle FROM videos WHERE Commends IS NOT NULL'''
    cursor.execute(query4)
    t4 = cursor.fetchall()

    df4 = pd.DataFrame(t4, columns=["No of comments","videotitle"])
    st.write(df4)

elif question =="5.FIND VIDEOS WITH THE MOST LIKES AND THEIR CHANNELS":
    query5 = '''SELECT Title AS videotitle, Channael_Name AS channelname, Likes AS likescount FROM videos WHERE Likes IS NOT NULL ORDER BY Likes DESC'''
    cursor.execute(query5)
    t5 = cursor.fetchall()

    df5 = pd.DataFrame(t5, columns=["videotitle","channelname","likescount"])
    st.write(df5)

elif question =="6.SHOW TOTAL LIKES AND DISLIKES FOR EACH VIDEO AND NAMES":
    query6 = '''SELECT Likes AS likescount, Title AS videotitle FROM videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()

    df6 = pd.DataFrame(t6, columns=["likescount","videotitle"])
    st.write(df6)

# ... (previous code)

elif question =="7.SUM UP VIEWS FOR EACH CHANNEL AND THEIR NAMES":
    query7 = '''SELECT Channel_Name AS channelname, Views AS totalviews FROM channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()

    df7 = pd.DataFrame(t7, columns=["channelname","totalviews"])
    df7
    st.write(df7)  # Corrected indentation

elif question =="8.LIST CHANNEL NAMES WITH VIDEOS PUBLISHED IN 2022":
    query8 = '''SELECT Title AS video_title, Published_Date AS videosrelese,Channael_Name AS channelname FROM videos Where extract(year from Published_Date)=2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()

    df8 = pd.DataFrame(t8, columns=["video_title","publish_data","channelname"])
    df8
    st.write(df8)


elif question =="9.CALCULATE AVERAGE DURATION OF VIDEOS IN EACH CHANNEL":
    query9 = '''SELECT Channael_Name AS channel_name, AVG(Duration) AS averageduration FROM videos GROUP BY Channael_Name'''
    cursor.execute(query9)
    t9 = cursor.fetchall()

    df9 = pd.DataFrame(t9, columns=["channel name","averageduration"])
    df9

    T9=[]
    for index,row in df9.iterrows():
        chennal_title=row["channel name"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=chennal_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)  # Corrected indentation

elif question =="10.DETERMINE VIDEOS WITH HIGHEST COMMENTS AND CHANNEL NAMES":
    query10 = '''SELECT Title AS videotitle, Channael_Name AS channelname,Commends as comments  FROM videos WHERE Commends is NOT NULL ORDER BY Commends DESC'''
    cursor.execute(query10)
    t10 = cursor.fetchall()

    df10 = pd.DataFrame(t10, columns=["video title","channel name","comments"])
    df10
    st.write(df10)  # Corrected indentation
