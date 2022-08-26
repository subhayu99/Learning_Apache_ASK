from json import dumps
from kafka import KafkaProducer
from config import (
    bootstrap_servers,
    sasl_username,
    sasl_mechanisms,
    sasl_password,
    session_timeout_ms,
    topic
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    # sasl_mechanism=sasl_mechanisms,
    # sasl_plain_username=sasl_username,
    # sasl_plain_password=sasl_password,
    request_timeout_ms=session_timeout_ms,
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

def send_data(value, key = None):
    producer.send(topic, key=key, value=value)
    print(f"Sent data: {value}")
