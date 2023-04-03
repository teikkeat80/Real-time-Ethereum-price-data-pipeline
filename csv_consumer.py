from kafka import KafkaConsumer
import json
from utils import write_eth_data_to_csv
from dotenv import load_dotenv
import os

load_dotenv()

# Deserialise the serialised JSON object
def json_deserializer(data):
    return json.loads(data)

kafka_topic = os.getenv('KAFKA_TOPIC_NAME')
kafka_bootstrap_server = os.getenv('KAFKA_SERVER_PORT')

# Define Kafka consumer
consumer = KafkaConsumer(
kafka_topic,
bootstrap_servers=[kafka_bootstrap_server],
auto_offset_reset='earliest',
value_deserializer=json_deserializer
)

# Loop through Kafka messages and write data to CSV file
for msg in consumer:
    write_eth_data_to_csv(msg.value)