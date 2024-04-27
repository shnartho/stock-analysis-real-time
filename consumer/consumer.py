import json
import logging
import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError
import time
import os

logging.basicConfig(filename='consumer.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaConsumer:
    def __init__(self, broker_address, group_id, topic_name, bucket_name, aws_access_key_id, aws_secret_access_key):
        self.consumer = Consumer({
            'bootstrap.servers': broker_address,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([topic_name])
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info('Reached end of partition')
                    else:
                        logging.error(f'Error: {msg.error()}')
                else:
                    value = msg.value()
                    if value is None:
                        logging.info('Received null message')
                    else:
                        try:
                            message = value.decode('utf-8')
                            message = json.loads(message)
                            logging.info(f'Received JSON message: {message}')
                            self.upload_to_s3(message)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            logging.info(f'Received non-JSON message: {value}')
                            self.upload_to_s3(value)

        except KeyboardInterrupt:
            logging.info('Consumer stopped by user')

        except Exception as e:
            logging.error(f'Error occurred: {str(e)}')

        finally:
            self.consumer.close()
            logging.info('Consumer closed')

    def upload_to_s3(self, data):
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=f'{time.time()}.json', Body=json.dumps(data))
            logging.info(f'Data uploaded to S3 bucket: {self.bucket_name}')
        except ClientError as e:
            logging.error(f'Error uploading data to S3: {e}')

if __name__ == "__main__":
    broker_address = os.getenv('KAFKA_BROKER_ADDR') #'35.172.219.231:9092'
    group_id = os.getenv('GROUP_ID') #'my-consumer-group'
    topic_name = os.getenv('KAFKA_TOPIC_NAME') #'testKafka'
    bucket_name = os.getenv("BUCKET_NAME") #'kafka-stock-price'
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID") #'AKIAXFXWMOGNSVP2KART'
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") 

    kafka_consumer = KafkaConsumer(broker_address, group_id, topic_name, bucket_name, aws_access_key_id, aws_secret_access_key)
    kafka_consumer.consume_messages()