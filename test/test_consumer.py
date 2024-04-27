import unittest
from unittest.mock import patch, MagicMock
from consumer import KafkaConsumer

class KafkaConsumerTest(unittest.TestCase):

    @patch('confluent_kafka.Consumer')
    @patch('boto3.client')
    def test_consume_messages(self, mock_boto3, mock_consumer):
        # Mock methods
        mock_consumer.poll.side_effect = [
            MagicMock(value=b'{"message": "test message"}', error=None),
            None
        ]
        mock_boto3.put_object.return_value = None

        consumer = KafkaConsumer()

        consumer.consume_messages()

        mock_consumer.poll.assert_called()
        mock_boto3.put_object.assert_called_once_with(
            Bucket="bucket_name", Body=b'{"message": "test message"}'
        )

if __name__ == '__main__':
    unittest.main()
