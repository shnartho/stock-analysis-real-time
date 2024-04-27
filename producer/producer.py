import json
import time
import logging
from confluent_kafka import Producer
import pandas as pd
import os

logging.basicConfig(filename='producer.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaProducer:
    def __init__(self, broker_address, topic_name):
        self.producer = Producer({'bootstrap.servers': broker_address})
        self.topic_name = topic_name

    def send_messages_from_csv(self, csv_file):
        try:
            df = pd.read_csv(csv_file)

            for index, row in df.iterrows():
                message = row.to_dict()
                message_bytes = json.dumps(message).encode('utf-8')
                self.producer.produce(self.topic_name, value=message_bytes)
                time.sleep(2)
                logging.info(f"Message sent to Kafka topic: {self.topic_name}")

            self.producer.flush()
            logging.info("All messages sent successfully!")

        except Exception as e:
            logging.error(f"Error occurred: {str(e)}")

        finally:
            self.producer.flush()
            logging.info("Producer flushed and closed.")

if __name__ == "__main__":
    broker_address = os.getenv('KAFKA_BROKER_ADDR') #'35.172.219.231:9092'
    topic_name = os.getenv('KAFKA_TOPIC_NAME') #'testKafka'
    csv_file = 'stockData.csv'

    kafka_producer = KafkaProducer(broker_address, topic_name)
    kafka_producer.send_messages_from_csv(csv_file)