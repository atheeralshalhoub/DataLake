import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import databricks.koalas as ks
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()



#Normally this file should be in ~/.aws/credentials
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

    """
    - create create_spark_session
    """

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

    """
    - create process_song_data to deal with song data
    - create song table and artist table
    """
def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file
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

    """
    - create process_log_data to deal with log data
    - create user table ,time table and songplay table
    """
def process_log_data(spark, input_data, output_data):
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

    """
    - create extract_time_features to convert unix ti datetime
    - extract each hour , dayofweek, year, month
    """
def extract_time_features(df):
    df['timestamp'] = ks.to_datetime(df.ts ,unit='ms')
    df['hour'] = df.timestamp.dt.hour
    df['dayofweek'] = df.timestamp.dt.dayofweek
    df['year'] = df.timestamp.dt.year
    df['month'] = df.timestamp.dt.month   
    return df

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4dend/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
