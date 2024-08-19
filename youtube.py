import pandas as pd
from googleapiclient.discovery import build
import pymongo
import psycopg2
import streamlit as st

def Api_connect():
    Api_id ="AIzaSyAjfdt6xzw_pg6x1Llkk6obt2SfT0LOLUs"

    Api_service_name = "youtube"
    Api_version = "v3"

    youtube = build(Api_service_name,Api_version,developerKey=Api_id)
    return youtube

youtube = Api_connect()


#get channel information
def get_Channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )
    response = request.execute()       

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id=i["id"],
                  Subscribers=i["statistics"]["subscriberCount"],
                  Views=i["statistics"]["viewCount"],
                  Total_Videos=i["statistics"]["videoCount"],
                  Channel_Describtion=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])

   
    return data
    


# Get video ids
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    
    return video_ids


#get video information
def get_videos_info(video_ids):
   video_data=[]
   for video_id in video_ids:
        request=youtube.videos().list(
                            part="snippet,ContentDetails,statistics",
                            id=video_id)
        response=request.execute()

        for item in response['items']:
             data=dict(Channel_Name=item['snippet']['channelTitle'],
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
        Favorite_count=item['statistics'].get('favoriteCount'),
        Comments=item['statistics'].get('commentCount'),
        Definition=item['contentDetails']['definition'],
        Caption_Status=item['contentDetails']['caption']
                  )
        
             video_data.append(data)
   return video_data
           
      

#get comment information
def get_comment_info(video_ids):
    next_page_token=None
    Comment_data=[]
    try:
      for video_id in video_ids:
          request=youtube.commentThreads().list(
              part="snippet",
              videoId= video_id,
              maxResults=50,
              pageToken=next_page_token
              
      )
      response=request.execute()

      for item in response[ 'items']:
        data=dict(Comment_Id=item[ 'snippet']['topLevelComment'] ['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
        
            
        Comment_data.append(data)

    except:
        pass   
    return Comment_data


#get playlist-details
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                                channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet'][ 'channelId'],
                        Channerl_Name=item['snippet'][ 'channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break

        return All_data


#upload to mongodb
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["Youtube_data"]



def channel_details(channel_id):
    ch_details=get_Channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_videos_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information": pl_details,
                      "video_information": vi_details,"comment_information":com_details})
    
    return "upload completed successfully"
    

#table creation for channels,plylist,videos,comments
def channels_table(channel_nmae_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="admin",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()



   
    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Describtion text,
                                                        Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()


    print("Channels table already created")

    single_channel_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_nmae_s},{"_id":0,}):
       single_channel_details.append(ch_data["channel_information"])

    df_single_channel_details=pd.DataFrame(single_channel_details)

    for index,row in  df_single_channel_details.iterrows():
        insert_query = '''insert into channels(Channel_Name ,                                   
                                                Channel_Id,                                     
                                                Subscribers,                                                      
                                                Views,                                              
                                                Total_Videos,                                                        
                                                Channel_Describtion,    
                                                Playlist_Id)
                                            
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Describtion'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            news= f"Your Provided Channel Name {channel_nmae_s} is Already exists"


            return news
              
              

            



def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="admin",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

 
    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()
    
    


    single_playlist_detalis=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0,}):
       single_playlist_detalis.append(ch_data["playlist_information"])


    df_single_playlist_details=pd.DataFrame(single_playlist_detalis[0])


    for index, row in  df_single_playlist_details.iterrows():
            insert_query = '''insert into playlists(Playlist_Id,                                   
                                                    Title,                                     
                                                    Channel_Id,                                              
                                                    Channel_Name,                                                        
                                                    PublishedAt,    
                                                    Video_Count)
                                                    
                                                    values(%s, %s, %s, %s, %s, %s)'''

            values = (
                row['Playlist_id'],
                row['Title'],
                row['Channel_Id'],
                row['Channerl_Name'],
                row['PublishedAt'],
                row['Video_Count']
            )

            cursor.execute(insert_query, values)
            mydb.commit()
                


def videos_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="admin",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()

    
    # Create the videos table with adjusted data types
    create_query = '''CREATE TABLE IF NOT EXISTS videos(
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Video_Id VARCHAR(30) PRIMARY KEY,
                        Title VARCHAR(150),
                        Tags TEXT,
                        Thumbnail VARCHAR(200),
                        Description TEXT,
                        Published_Date TIMESTAMP,
                        Duration INTERVAL,
                        Views BIGINT,
                        Likes BIGINT,
                        Favorites BIGINT, 
                        Comments BIGINT,   
                        Definition VARCHAR(50),
                        Caption_Status VARCHAR(50)
                      )'''

    cursor.execute(create_query)
    mydb.commit()

    single_videos_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_s},{"_id":0,}):
        single_videos_details.append(ch_data["video_information"])


    df_single_videos_details=pd.DataFrame(single_videos_details[0])

    # Insert data into PostgreSQL
    for index, row in df_single_videos_details.iterrows():
        insert_query = '''INSERT INTO videos(
                            Channel_Name,
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
                            Favorites,
                            Comments,
                            Definition,
                            Caption_Status
                          )
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Favorite_Count'],  # Ensure 'Favorites' is in the correct position
                  row['Comments'],   # Ensure 'Comments' is in the correct position
                  row['Definition'],
                  row['Caption_Status'])

        
        
        cursor.execute(insert_query, values)
        mydb.commit()
    




def comments_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="admin",
                            database="youtube_data",
                            port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    cursor=mydb.cursor()
    mydb.commit()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                        Video_Id varchar(50),
                        Comment_Text text,
                        Comment_Author varchar(150),
                        Comment_Published timestamp
                        )'''

    cursor.execute(create_query)
    mydb.commit()



    single_comments_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0,}):
        single_comments_details.append(ch_data["comment_information"])


    df_single_comments_details=pd.DataFrame(single_comments_details[0]) 


    for index,row in df_single_comments_details.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                                Video_Id ,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published )
                        
                                            values(%s, %s, %s, %s, %s)'''

        values = (row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
                )
        
        cursor.execute(insert_query,values)
        mydb.commit()



def tables(single_channel):

    news=channels_table(single_channel)
    if news:
        return news
    
    else:
        playlist_table(single_channel)
        videos_table(single_channel)
        comments_table(single_channel)
    
        return "Tables created successfully"


def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)


def show_playlists_table():
    pl_list=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
            for i in range(len(pl_data["playlist_information"])):
                    pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
      
    return df2
   

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

import streamlit as st
from pymongo import MongoClient

# Initialize the client (assuming you have this set up)
client = MongoClient("mongodb://localhost:27017/")

# Sidebar content
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")   
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id = st.text_input("Enter the channel ID")

# Initialize ch_ids in session state if not already done
if 'ch_ids' not in st.session_state:
    st.session_state.ch_ids = []

if st.button("collect and store data"):
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    ch_ids = [ch_data["channel_information"]["Channel_Id"] for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1})]
    st.session_state.ch_ids = ch_ids  # Store ch_ids in session state

# Ensure ch_ids is available before checking
if channel_id:
    if channel_id in st.session_state.ch_ids:
        st.success("Channel Details of the given channel ID already exist")
    else:
        if st.button("Insert channel data"):
            insert = channel_details(channel_id)  # Assuming channel_details is defined elsewhere
            st.success(insert)


all_channels=[]
db=client["Youtube_data"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
   all_channels.append(ch_data["channel_information"]["Channel_Name"])


unique_channel=st.selectbox("select the channel",all_channels)


# Migration to SQL
if st.button("Migrate to SQL"):
    Table = tables(unique_channel)  # Assuming tables() is defined elsewhere
    st.success(Table)

# Radio button to select the table to view
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

# Display the selected table
if show_table == "CHANNELS":
    show_channels_table()  # Assuming show_channels_table() is defined elsewhere
elif show_table == "PLAYLISTS":
    show_playlists_table()  # Assuming show_playlists_table() is defined elsewhere
elif show_table == "VIDEOS":
    show_videos_table()  # Assuming show_videos_table() is defined elsewhere
elif show_table == "COMMENTS":
    show_comments_table()  # Assuming show_comments_table() is defined elsewhere





#SQL connection

mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="admin",
                        database="youtube_data",
                        port="5432")
cursor=mydb.cursor()


question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                               "3. 10 most viewed videos ",
                                               "4. comments in each videos",
                                               "5. Videos with highest likes",
                                               "6. likes of all videos",
                                               "7. views of each channels",
                                               "8. videos published in the year of 2022",
                                               "9. average duration of all videos in each channel",
                                               "10. videos with highest number of comment"))


mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="admin",
                        database="youtube_data",
                        port="5432")
cursor=mydb.cursor()

if question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)


elif question=="2. channels with most number of videos":
    query2 = '''
        SELECT channel_name AS channelname, total_videos AS no_videos 
        FROM channels
        ORDER BY total_videos DESC
    '''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "no of videos"])
    st.write(df2)

elif question=="3.10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from videos
                    where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)


elif question== "4. comments in each videos":
    query4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments","videotitle"])
    st.write(df4)

elif question== "5. Videos with highest likes":
    query5 = '''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc  '''
    cursor.execute(query5)
    mydb.commit()
    t5= cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question== "6. likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos  '''
    cursor.execute(query6)
    mydb.commit()
    t6= cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question== "7. views of each channels":
    query7 = '''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7= cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

    
elif question== "8. videos published in the year of 2022":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8= cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

    
elif question== "9. average duration of all videos in each channel":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name '''
    cursor.execute(query9)
    mydb.commit()
    t9= cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))

    df1=pd.DataFrame(T9)
    st.write(df1)

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))

    df1=pd.DataFrame(T9)
    st.write(df1)



elif question==  "10. videos with highest number of comment":
    query10 = '''select title as videotitle,channel_name as channelname,comments as comments from videos where comments
                is not null order by comments desc '''
    cursor.execute(query10)
    mydb.commit()
    t10= cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channel name","comments"])
    st.write(df10)