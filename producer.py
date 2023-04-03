from kafka import KafkaProducer
import json
from utils import get_eth_data_from_API
import time
from dotenv import load_dotenv
import os

load_dotenv()

# Serialise a JSON serialisable object
def json_serialiser(data):
    return json.dumps(data).encode('utf-8')

kafka_topic = os.getenv('KAFKA_TOPIC_NAME')
kafka_bootstrap_server = os.getenv('KAFKA_SERVER_PORT')

# Define Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[kafka_bootstrap_server],
    value_serializer=json_serialiser
)

time_interval = 60 # Unit = seconds, Select your time interval for producer to send data
running = True

# Run Kafka producer in infinite loop
if __name__ == '__main__':
    try:
        while running:
            message = get_eth_data_from_API() # Retrieve Ethereum price data from API
            producer.send(kafka_topic, message) # Send data to Kafka topic
            producer.flush()
            time.sleep(time_interval)
    except KeyboardInterrupt: # Ctrl + C to stop producer
        running = False
    
producer.close() # Close Kafka producer connection