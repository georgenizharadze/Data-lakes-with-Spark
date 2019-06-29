import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Instantiate a Spark driver process"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ETL json song files from S3 into parquet files back on S3
    
    Ingests json files containing song data into Spark's distributed 
    dataframes, selects columns and ensures data type integrity according to
    predefined schema; writes transformed data tables to parquet files. 
    
    Arguments:
    spark -- Spark driver process (Spark session) 
    input_data -- S3 bucket where the json files reside
    output_data -- S3 bucket to write parquet files to
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/A/*/*/*.json'
    
    # read song data file
    df = spark.read.format('json').load(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 
                            'title', 
                            'artist_id', 
                            'year', 
                            'duration') \
                           .distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songstables/', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
                              'artist_name', 
                              'artist_location', 
                              'artist_latitude', 
                              'artist_longitude') \
                             .distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artiststables/', mode='overwrite')

def process_log_data(spark, input_data, output_data):
    """ETL json log files from S3 into parquet files back to S3
    
    Ingests json files containing user logs into Spark's distributed 
    dataframes, selects columns and ensures data type integrity according to
    predefined schema; writes transformed data tables to parquet files. 
    
    Arguments:
    spark -- Spark driver process (Spark session) 
    input_data -- S3 bucket where the json files reside
    output_data -- S3 bucket to write parquet files to
    """
    # get filepaths to log and song data files
    log_data = input_data + 'log_data/2018/11/*.json'
    song_data = input_data + 'song_data/A/*/*/*.json'

    # read log data file
    df = spark.read.format('json').load(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId AS user_id', 
                                'firstName AS first_name', 
                                'lastName AS last_name', 
                                'gender',
                                'level') \
                               .distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'userstables/', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType()) 
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create a table view for sql-type join and queries
    df.createOrReplaceTempView('events_table')
    
    # extract columns to create time table
    time_table = df.selectExpr('ts AS ts_key',
                               'timestamp AS start_time',
                               'hour(timestamp) AS hour',
                               'dayofmonth(timestamp) AS day',
                               'weekofyear(timestamp) AS week',
                               'month(timestamp) AS month',
                               'year(timestamp) AS year',
                               'dayofweek(timestamp) AS weekday') \
                              .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'timetables/', mode='overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.read.format('json').load(song_data)
    
    # create a table view for sql-type join and queries
    song_df.createOrReplaceTempView('songs_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT ts            AS ts_key, 
               timestamp     AS start_time,
               userId        AS user_id,
               level         AS level,
               song_id       AS song_id,
               artist_id     AS artist_id,
               sessionId     AS session_id,
               location      AS location,
               userAgent     AS user_agent
        FROM events_table events
        LEFT JOIN songs_table songs
        ON events.song=songs.title AND events.artist=songs.artist_name
    """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/', mode='overwrite')
    

def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://dend-data-lake-p4/sparkify/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()