from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from dotenv import load_dotenv
import os
import time

load_dotenv()

# Define Schema
schema = StructType([
    StructField('ethbtc', StringType()),
    StructField('ethbtc_timestamp', StringType()),
    StructField('ethusd', StringType()),
    StructField('ethusd_timestamp', StringType())
])

# Create a Spark Session and fetch packages dynamically from Maven repository
appname = 'kafka-spark-process' # Define your appName

spark_session = SparkSession \
    .builder \
    .master('local') \
    .appName(appname) \
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,') \
    .getOrCreate()

spark_session.sparkContext.setLogLevel('ERROR')

kafka_topic = os.getenv('KAFKA_TOPIC_NAME')
kafka_bootstrap_server = os.getenv('KAFKA_SERVER_PORT')

# Read kafka topic data into a spark DataFrame
spark_df = spark_session \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap_server) \
    .option('subscribe', kafka_topic) \
    .option('startingOffsets', 'earliest') \
    .load()

# Extract and parse the data from kafka message
parsed_spark_df = spark_df \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json('value', schema).alias('data')) \
    .select(col('data.ethbtc').alias('ethbtc'), 
            col('data.ethbtc_timestamp').alias('ethbtc_timestamp'), 
            col('data.ethusd').alias('ethusd'), 
            col('data.ethusd_timestamp').alias('ethusd_timestamp'))

# Define postgresql parameters
postgresql_database = os.getenv('POSTGRESQL_DB_NAME')
postgresql_user = os.getenv('POSTGRESQL_USER')
postgresql_pw = os.getenv('POSTGRESQL_PASSWORD')
postgresql_server = os.getenv('POSTGRESQL_SERVER_PORT')
postgresql_url = f'jdbc:postgresql://{postgresql_server}/{postgresql_database}'
postgresql_table = os.getenv('POSTGRESQL_DB_TABLE_NAME')
postgresql_properties = {
    'user': postgresql_user,
    'password': postgresql_pw,
    'driver': 'org.postgresql.Driver'
}

running = True
time_interval = 60 # Unit = seconds, Select your time interval to consume data

# Write the stream data to a Postgresql database in infinite loop
if __name__ == '__main__':
    try:
        while running:
            query = parsed_spark_df \
            .writeStream \
            .foreachBatch(
                lambda batchDF, batchId: batchDF.write.jdbc(
                    url=postgresql_url, 
                    table=postgresql_table, 
                    mode='append', 
                    properties=postgresql_properties
                )
            ) \
            .start()
            time.sleep(time_interval)
    except KeyboardInterrupt: # Ctrl + C to stop consumer
        running = False
    
    query.awaitTermination(15) # Terminate consumer after 15 seconds