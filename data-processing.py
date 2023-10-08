import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import sum, window, count, approx_count_distinct, to_timestamp, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType

def extract(spark):
    netflix_ratings_schema = StructType([
        StructField('date', DateType(), True),
        StructField('film_id', IntegerType(), True),
        StructField('user_id', StringType(), True),
        StructField('rate', IntegerType(), True)])
    
    netflix_ratings_df = spark \
        .readStream \
        .option('maxFilesPerTrigger', 2) \
        .option('header', True) \
        .schema(netflix_ratings_schema) \
        .csv('./data/netflix-prize-data')


    movie_titles_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('year', IntegerType(), True), 
        StructField('title', StringType(), True)
    ])

    movie_titles_df = spark.read.format('csv') \
        .option('header', True) \
        .schema(movie_titles_schema) \
        .load('./data/movie_titles.csv')
    
    return netflix_ratings_df, movie_titles_df
    

def transform(netflix_ratings_df, movie_titles_df, anomaly_detection, time_length, ratings_number, avg_rating):
    ratings_with_titles_df = netflix_ratings_df.join(movie_titles_df, 
                                                netflix_ratings_df.film_id == movie_titles_df.id, 'inner')
    
    # ETL - films aggregation in particular months, agg values - ratings_number, ratings_sum, unique_voting_people_number
    if not anomaly_detection:
        result_df = ratings_with_titles_df \
            .withColumn('timestamp', to_timestamp(col('date'), 'yyyy-MM-dd')) \
            .withWatermark('timestamp', '1 day') \
            .groupBy(
                window('timestamp', '30 days'),
                col('film_id'),
                col('title')) \
            .agg(count('rate').alias('ratings_number'),
                sum('rate').alias('ratings_sum'),
                approx_count_distinct('user_id').alias('unique_voting_people_number')) \
            .select('window.start', 'window.end', 'film_id', 'title', 'ratings_number', 'ratings_sum', 'unique_voting_people_number')

    else:
        result_df = ratings_with_titles_df \
            .withColumn('timestamp', to_timestamp(col('date'), 'yyyy-MM-dd')) \
            .withWatermark('timestamp', '1 day') \
            .groupBy(
                window('timestamp', f'{time_length} days', '1 day'),
                col('film_id'),
                col('title')) \
            .agg(count('rate').alias('ratings_number'),
                avg('rate').alias('avg_rating')) \
            .where((col('ratings_number') >= ratings_number) & (col('avg_rating') >= avg_rating)) \
            .select('window.start', 'window.end', 'title', 'ratings_number', 'avg_rating')
    
    return result_df

def load(result_df, delay):
    if delay == 'A':
        result_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start() \
            .awaitTermination()
    elif delay == 'C':
        result_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start() \
            .awaitTermination()


def main():
     
    spark = SparkSession \
        .builder \
        .appName("StructuredNetflixRatesApp") \
        .getOrCreate()

    # delay parameter - A (minimum latency, non-terminal results) or C (terminal results)
    delay = sys.argv[1]

    anomaly_detection = False
    # Anomaly detection mode
    if len(sys.argv) == 5:
        time_length, ratings_number, avg_rating = sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
        anomaly_detection = True

    # data processing
    netflix_ratings_df, movie_titles_df = extract(spark)
    result_df = transform(netflix_ratings_df, movie_titles_df, anomaly_detection,
                           time_length, ratings_number, avg_rating)
    load(result_df, delay)








