import json

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'pipeline_logs_topic'

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        record = json.loads(msg.value)
        print('Alert: {} registered at time: {}'.format(record['log'], record['timestamp']))

    if consumer is not None:
        consumer.close()
