import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, window, count, approx_count_distinct, to_timestamp, col, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

def extract(spark, cluster_name):

    netflix_ratings_schema = StructType([
        StructField('date', DateType(), True),
        StructField('film_id', IntegerType(), True),
        StructField('user_id', StringType(), True),
        StructField('rate', IntegerType(), True)])

    movie_titles_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('year', IntegerType(), True), 
        StructField('title', StringType(), True)
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"${cluster_name}-w-0:9092") \
        .option("subscribe", "kafka-input") \
        .load()
    
    json_df = df.selectExpr("CAST(value AS STRING) as value") 

    netflix_ratings_df = json_df.withColumn("value", from_json(json_df["value"], netflix_ratings_schema)) \
        .select("value.*")
    
    movie_titles_df = spark.read.format('csv') \
        .option('header', True) \
        .schema(movie_titles_schema) \
        .load('./data/movie_titles.csv')
    
    return netflix_ratings_df, movie_titles_df
    

def transform(netflix_ratings_df, movie_titles_df, anomaly_detection, time_length=0, ratings_number=0, avg_rating=0):
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


def load(result_df, delay, cluster_name, anomaly_detection):

    dbtable = 'anomalies' if anomaly_detection else 'netflix_movie_ratings_agg'

    def postgres_sink(batch_df, batch_id):
        # db passes can be parametrized
        batch_df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", f"jdbc:postgresql://{cluster_name}-m:8432/streamoutput") \
            .option("dbtable", dbtable) \
            .option("user", "postgres") \
            .option("password", "secret") \
            .option("truncate", "true") \
            .save()

    if delay == 'A':
        result_df \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(postgres_sink) \
            .start() \
            .awaitTermination()
    elif delay == 'C':
        result_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(postgres_sink) \
            .start() \
            .awaitTermination()


def main():
     
    spark = SparkSession \
        .builder \
        .appName("StructuredNetflixRatesApp") \
        .getOrCreate()
    
    
    cluster_name, delay = sys.argv[1], sys.argv[2]

    # delay parameter - A (minimum latency, non-terminal results) or C (terminal results)

    anomaly_detection = False
    time_length, ratings_number, avg_rating = [0] * 3
    # Anomaly detection mode
    if len(sys.argv) == 6:
        time_length, ratings_number, avg_rating = sys.argv[3], int(sys.argv[4]), float(sys.argv[5])
        anomaly_detection = True

    # data processing
    netflix_ratings_df, movie_titles_df = extract(spark, cluster_name)
    result_df = transform(netflix_ratings_df, movie_titles_df, anomaly_detection,
                           time_length, ratings_number, avg_rating)
    load(result_df, delay, cluster_name, anomaly_detection)


if __name__ == '__main__':
    main()





