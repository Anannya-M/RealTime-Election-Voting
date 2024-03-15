import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'


if __name__ == "__main__":
    # print(pyspark.__version__)
    print("Initializing the spark object.....")
    spark = (SparkSession.builder.appName("RealtimeElection").master("local[*]")
             .config('spark.jars.packages',
                     'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')  # Spark kafka integration
             .config('spark.jars', 'D:\\Clg_Projects\\Spark\\Playing-with-Data\\RealTimeVoting\\postgresql-42.7.2.jar')
             .config('spark.sql.adaptive.enable', 'false')
             .getOrCreate())

    print("Specifying the schema for the voters....")
    vote_schema = StructType([
        StructField('voter_id', StringType(), True),
        StructField('candidate_id', StringType(), True),
        StructField('voting_time', TimestampType(), True),
        StructField('voter_name', StringType(), True),
        StructField('party_affiliation', StringType(), True),
        StructField('biography', StringType(), True),
        StructField('campaign_platform', StringType(), True),
        StructField('photo_url', StringType(), True),
        StructField('candidate_name', StringType(), True),
        StructField('date_of_birth', DateType(), True),
        StructField('gender', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('registration_number', IntegerType(), True),
        StructField('address', StructType([
            StructField('street', StringType(), True),
            StructField('city', StringType(), True),
            StructField('state', StringType(), True),
            StructField('country', StringType(), True),
            StructField('postcode', StringType(), True)

        ]), True),
        StructField('email', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('picture', StringType(), True),
        StructField('registered_age', IntegerType(), True),
        StructField('vote', IntegerType(), True)
    ])

    print("Starting the preprocessing of data....")

    votes_data = (spark.readStream
                  .format('kafka')
                  .option('kafka.bootstrap.servers', 'localhost:9092')
                  .option('subscribe', 'voter_topic')
                  .option('startingOffsets', 'earliest')
                  .option("failOnDataLoss", "false")
                  .load())
    # .selectExpr("CAST(value AS STRING)")
    # .select(from_json(col('value'), vote_schema).alias('data'))
    # .select('data.*'))

    votes_data = votes_data.selectExpr("CAST(value AS STRING)")
    votes_data = votes_data.select(from_json(col('value'), vote_schema).alias('data')).select('data.*')

    votes_data = votes_data.withColumn('voting_time', col('voting_time').cast(TimestampType())) \
        .withColumn('vote', col('vote').cast(IntegerType()))

    cleaned_data = votes_data.withWatermark('voting_time', '1 minute')

    votes_per_candidate = cleaned_data.groupBy('candidate_id', 'candidate_name', 'party_affiliation', 'photo_url').agg(
        sum('vote').alias("total_votes"))

    turnout_by_location = cleaned_data.groupBy('address.state').count().alias('total_votes')

    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) AS value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_votes_per_candidate')
                                    .option('checkpointLocation',
                                            'D:\Clg_Projects\Spark\Playing-with-Data\RealTimeVoting\Checkpoints\Checkpoint1')
                                    .outputMode('update')
                                    .start()
                                    )

    turnout_by_location_to_kafka = (turnout_by_location.selectExpr('to_json(struct(*)) AS value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_turnout_by_location')
                                    .option('checkpointLocation',
                                            'D:\Clg_Projects\Spark\Playing-with-Data\RealTimeVoting\Checkpoints\Checkpoint2')
                                    .outputMode('update')
                                    .start()
                                    )

    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
