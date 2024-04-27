import unittest
import json
import time
import logging
from unittest.mock import patch, MagicMock
from confluent_kafka import Consumer, KafkaError

class TestKafkaConsumer(unittest.TestCase):
    def setUp(self):
        self.broker_address = 'localhost:9092'
        self.group_id = 'test-group'
        self.topic_name = 'test-topic'

    @patch('confluent_kafka.Consumer')
    def test_consume_messages(self, mock_consumer):
        # Mock the poll method
        mock_poll = MagicMock()
        mock_poll.side_effect = [
            # Simulate a null message
            MagicMock(value=None),
            # Simulate a JSON message
            MagicMock(value=json.dumps({'key': 'value'}).encode('utf-8')),
            # Simulate a non-JSON message
            MagicMock(value=b'non-json-message'),
            # Simulate a KeyboardInterrupt
            KeyboardInterrupt()
        ]
        mock_consumer.return_value.poll.side_effect = mock_poll

        # Capture logs
        log_capture_string = io.StringIO()
        logging.basicConfig(stream=log_capture_string, level=logging.INFO)

        # Create a KafkaConsumer instance with mocked dependencies
        kafka_consumer = KafkaConsumer(self.broker_address, self.group_id, self.topic_name)
        kafka_consumer.consumer = mock_consumer.return_value

        # Call the consume_messages method
        kafka_consumer.consume_messages()

        # Check logs
        logs = log_capture_string.getvalue()
        self.assertIn('Received null message', logs)
        self.assertIn("Received JSON message: {'key': 'value'}", logs)
        self.assertIn('Received non-JSON message: b\'non-json-message\'', logs)
        self.assertIn('Consumer stopped by user', logs)

    @patch('confluent_kafka.Consumer')
    def test_consume_messages_with_error(self, mock_consumer):
        # Mock the poll method to simulate an error
        mock_poll = MagicMock()
        mock_poll.side_effect = [
            MagicMock(error=KafkaError(KafkaError._PARTITION_EOF)),
            MagicMock(error=Exception('Test error'))
        ]
        mock_consumer.return_value.poll.side_effect = mock_poll

        # Capture logs
        log_capture_string = io.StringIO()
        logging.basicConfig(stream=log_capture_string, level=logging.ERROR)

        # Create a KafkaConsumer instance with mocked dependencies
        kafka_consumer = KafkaConsumer(self.broker_address, self.group_id, self.topic_name)
        kafka_consumer.consumer = mock_consumer.return_value

        # Call the consume_messages method
        kafka_consumer.consume_messages()

        # Check logs
        logs = log_capture_string.getvalue()
        self.assertIn('Reached end of partition', logs)
        self.assertIn('Error occurred: Test error', logs)

if __name__ == '__main__':
    unittest.main()