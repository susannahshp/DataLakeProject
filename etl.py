import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Summary:
    This is a function to create a spark session.    
    It builds a session by the imported SparkSession.
    It also uses config method to configurate and uses the getOrCreate method.
    
    Returns: 
    Spark object.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Summary:
    This is a function to process song data.
    It gets the filepath to song data file by using the os module and reads the JSON file with the spark module and make it as a spark dataframe.
    It extracts columns to create dimensional tables and transforms the data by using select method of dataframe object.
    Then it parquets the file and loads the data to the S3 output bucket by using the parquet method.

    Parameters:
    Spark object, input data and output data which holds the S3 bucket paths.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs_table'))

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists_table'))


def process_log_data(spark, input_data, output_data):
    """
    Summary:
    This is a function to process log data.
    It uses the os module to get the log data JSON files and make it as a spark dataframe.
    It filters and extracts columns from the dataframe to create dimensional tables.
    Then it loads the data to the output S3 bucket by using write and parquet methods.
    
    Parameters:
    Spark object, input data and output data which holds the S3 bucket paths.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log-data/2018/*/*.json")

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users_table'))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp((x/1000.0)), T.TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('timestamp').alias('hour'),
                           dayofmonth('timestamp').alias('day'),
                           weekofyear('timestamp').alias('week'),
                           month('timestamp').alias('month'),
                           year('timestamp').alias('year'),
                           date_format('timestamp','E').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'time_table'))

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data).dropDuplicates()
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song==song_df.title, how='inner')\
    .withColumn('songplay_id', F.monotonically_increasing_id())\
    .select(\
    'songplay_id',\
    col('timestamp').alias('start_time'),\
    col('userId').alias('user_id'),\
    'level','song_id','artist_id',col('sessionId').alias('session_id'),'location',\
    col('userAgent').alias('user_agent')
    )

    songplays_table = songplays_table\
    .join(time_table, songplays_table.start_time == time_table.start_time, how='inner') \
    .select('songplay_id', songplays_table.start_time, 'user_id', 'level', 'song_id', \
    'artist_id', 'session_id', 'location', 'user_agent', time_table.year, time_table.month)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'songplays_table'))



def main():
    """
    Summary:
    This is the main function which creates the spark session and processes the song and log data by calling the functions altogether.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://susannah-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
