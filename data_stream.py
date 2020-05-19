import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", StringType(), True),
                     StructField("call_date", StringType(), True),
                     StructField("offense_date", StringType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("common_location", StringType(), True)])

def run_spark_job(spark):
    """
    Create Spark configuration
    """
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "department.police.service.call") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 1000) \
        .option("maxOffsetsPerTrigger", 45000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    service_table.printSchema()
    
    # Select original_crime_type_name and disposition
    distinct_table = service_table \
        .select("call_date_time", "original_crime_type_name", "disposition") \
        .withWatermark("call_date_time", "60 minutes")

    # count the number of original crime type
    agg_df = distinct_table \
        .groupBy(psf.window("call_date_time", "60 minutes"), "original_crime_type_name") \
        .count()

    agg_df.printSchema()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .queryName("aggregates")\
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    radio_code_df.printSchema()

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, "disposition")\
        .writeStream \
        .queryName("joined_aggregates") \
        .outputMode("append")\
        .format("console") \
        .trigger(processingTime="60 seconds") \
        .option("truncate", False) \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    """
    Create and Run Spark Streaming application in Standalone mode
    """
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000)\
        .config('spark.driver.memory', '3g') \
        .config('spark.sql.shuffle.partitions', 10) \
        .config('spark.default.parallelism', 48) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
