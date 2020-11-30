# DataLake


# Song Play Project .. 

##### sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
##### The analytics team is particularly interested in understanding what songs users are listening to. 

### Firstly : recognize the dataset ..
##### - The first dataset is a subset of real data from the Million Song Dataset.
##### - The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above.

#### both of them wrriten as JSON format ..

### Secondly : Schema for Song Play Analysis ..
#### - **Fact Table** :
**songplays** - records in log data associated with song plays i.e. records with page NextSong.
* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### - **Dimension Tables** : 
**users** - users in the app .
* user_id, first_name, last_name, gender, level
**songs** - songs in music database .
* song_id, title, artist_id, year, duration
**artists** - artists in music database .
* artist_id, name, location, latitude, longitude
**time** - timestamps of records in songplays broken down into specific units .
* start_time, hour, day, week, month, year, weekday


### Thirdly : Project Steps ..
#### Open etl.py 
* import koalas


#### Open dl.cfg ..
* write AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

#### Open etl.py 
* read AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from dl.cfg file using ConfigParser.
* config = configparser.ConfigParser()
* Normally this file should be in ~/.aws/credentials
  config.read('dl.cfg')
  os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
  os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
  
#### Open etl Note Book ..
* import koalas
* read AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from dl.cfg file using ConfigParser.
* Create spark session with hadoop-aws package
  spark = SparkSession.builder\
                    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()
* Load data from S3
  input_data = "s3a://udacity-dend/"
  output_data = "s3a://project4dend/"
* read song data files
  kdf = ks.read_json("data/song_data/*/*/*/*.json")
  
#### Open etl.py 
##### Song Data
* Load data by adding 
  output_data = "s3a://project4dend/" in main .
* read song data files
  df = ks.read_json("data/song_data/*/*/*/*.json")

#### Open etl Note Book ..
- **Start with songs table**.
- import  pyspark.sql.functions as F
  songs_table = (ks.sql('''
               SELECT 
               DISTINCT
               song_id,
               title,
               artist_id,
               year,
               duration
               FROM 
                   {kdf}''')
              )
- songs_table
  pyspark_songs_table = (songs_table
                        .to_spark()
                        .withColumn("id", F.monotonically_increasing_id())
                        )
- convert to koalas again
  songs_table = ks.DataFrame(pyspark_songs_table)
  songs_table.head()


- **then artists table**.
- import  pyspark.sql.functions as F
  artists_table = (ks.sql('''
               SELECT 
               DISTINCT
               artist_id,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude
               FROM 
                   {kdf}''')
              )
- songs_table
  pyspark_artists_table = (artists_table
                        .to_spark()
                        .withColumn("id", F.monotonically_increasing_id())
                        )
- convert to koalas again
  artists_table = ks.DataFrame(pyspark_artists_table)
  artists_table.head()



##### Log Data ..
- get filepath to log data file
  log_data = "data/*.json"
- read log data file
  df_log = ks.read_json(log_data)

- **then Time table**.
- sample = ks.DataFrame(data = df_log.head().ts)
- creare extract_time_features to get datetime from unix time by using this def.
  def extract_time_features(df):
    df['timestamp'] = ks.to_datetime(df.ts ,unit='ms')
    df['hour'] = df.timestamp.dt.hour
    df['dayofweek'] = df.timestamp.dt.dayofweek
    df['year'] = df.timestamp.dt.year
    df['month'] = df.timestamp.dt.month   
    return df

  sample.pipe(extract_time_features)


- **then User table**.
- Extract the required columns 
- import  pyspark.sql.functions as F
  users_table = (ks.sql('''
               SELECT 
               DISTINCT
               userId,
               firstName,
               lastName,
               gender,
               level
               FROM 
                   {df_log}''')
              )
  users_table.head()


##### Lastly SongPlay Fact table**.
- Extract the required columns 
- import  pyspark.sql.functions as F
  kdf = ks.read_json("data/song_data/*/*/*/*.json")# s3a://udacity-dend/song_data/*/*/*/*.json to read the whole data
  songplays_table = (ks.sql('''SELECT e.ts,
       t.month,
       t.year,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent
       FROM 
       {df_log} e
       JOIN {kdf} s ON (e.song = s.title AND e.artist = s.artist_name)
       JOIN {sample} t ON (e.ts = t.ts)''')   
                  )
   songplays_table.head()
   

### Apache Parquet Introduction
Apache Parquet is a columnar file format that provides optimizations to speed up queries and is a far more efficient file format than CSV or JSON, supported by many data processing systems.

### Spark Write DataFrame to Parquet file format
Using spark.write.parquet() function we can write Spark DataFrame to Parquet file.


##### Song Table Parquet 
- using to Parquet song table partition by year and artist_id
  (songs_table
   .to_spark()
   .write
   .mode('overwrite')
   .partitionBy("year", "artist_id")
   .parquet('songs/')
   )
- see folder called " Songs" that created .


##### Artist Table Parquet 
- using to Parquet artist table.
  artists_table.to_spark().write.mode('overwrite').parquet('artists/')
- see folder called " artists" that created .

##### Time Table Parquet 
- using to Parquet time table partition by year and month.
  sample.to_spark().write.mode('overwrite').partitionBy("year", "month").parquet('time/')
- see folder called " time" that created .

##### User Table Parquet 
- using to Parquet user table.
  users_table.to_spark().write.mode('overwrite').parquet('users/')
- see folder called "users" that created .

##### SongPlay  Table Parquet 
- using to Parquet SongPlay table partition by year and month.
  songplays_table.to_spark().write.mode('overwrite').partitionBy("year", "month").parquet('songplays/')
- see folder called "SongPlays" that created .

#### Open etl.py 
- **in create_spark_session def write **
  def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

- **in process_song_data def write**
- get filepath to song data file
    song_data = "data/song_data/*/*/*/*.json"
    
    # read song data file
    df = ks.read_json(song_data)

    # extract columns to create songs table
    songs_table = (ks.sql('''
               SELECT 
               DISTINCT
               song_id,
               title,
               artist_id,
               year,
               duration
               FROM 
                   {df}''')
              )

    # write songs table to parquet files partitioned by year and artist
    songs_table.to_spark().write.mode('overwrite').partitionBy("year", "artist_id").parquet('songs/')

    # extract columns to create artists table
    artists_table = (ks.sql('''
               SELECT 
               DISTINCT
               artist_id,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude
               FROM 
                   {df}''')
              )
    
    # write artists table to parquet files
    artists_table.to_spark().write.mode('overwrite').parquet('artists/')


- **in process_log_data def write**
      # get filepath to log data file
    log_data = "data/*.json"

    # read log data file
    df = ks.read_json(log_data)
    
    # extract columns for users table    
    users_table = (ks.sql('''
               SELECT 
               DISTINCT
               userId,
               firstName,
               lastName,
               gender,
               level
               FROM 
                   {df}''')
              )

    # write users table to parquet files
    users_table.to_spark().write.mode('overwrite').parquet('users/')


    # create timestamp and datetime column from original timestamp column
    unix_time_series = df.head().ts.copy()
    get_datetime_timestamp = ks.DataFrame(data = unix_time_series)                               
    # extract columns to create time table

    
    get_datetime_timestamp.pipe(extract_time_features)
    
    # write time table to parquet files partitioned by year and month
    get_datetime_timestamp.to_spark().write.mode('overwrite').partitionBy("year", "month").parquet('time/')

    # read in song data to use for songplays table
    song_df = ks.read_json("data/song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (ks.sql('''SELECT e.ts,
       t.month,
       t.year,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent
       FROM 
       {df} e
       JOIN {song_df} s ON (e.song = s.title AND e.artist = s.artist_name)
       JOIN {get_datetime_timestamp} t ON (e.ts = t.ts)''')   
                  )
    # write songplays table to parquet files partitioned by year and month
    songplays_table.to_spark().write.mode('overwrite').partitionBy("year", "month").parquet('songplays/')


- write extract_time_features to convert unix time to datetime and extract from it
  def extract_time_features(df):
    df['timestamp'] = ks.to_datetime(df.ts ,unit='ms')
    df['hour'] = df.timestamp.dt.hour
    df['dayofweek'] = df.timestamp.dt.dayofweek
    df['year'] = df.timestamp.dt.year
    df['month'] = df.timestamp.dt.month   
    return df


#### Lastly : Open Terminal ..
- run python etl.py
- folders created using parquet
