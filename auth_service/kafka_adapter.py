import json
from logging import log, ERROR, INFO

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))


def publish_event(event_dict):
    producer.send('users-stream', event_dict).add_callback(on_send_success).add_errback(on_send_error)


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)
    log(INFO, record_metadata.topic)
    log(INFO, record_metadata.partition)
    log(INFO, record_metadata.offset)


def on_send_error(excp):
    log(ERROR, 'I am an errback', exc_info=excp)
