from kafka.consumer.fetcher import ConsumerRecord
from kafka.consumer import KafkaConsumer
from json import loads
import requests
from config import (
    bootstrap_servers,
    sasl_username,
    sasl_mechanisms,
    sasl_password,
    session_timeout_ms,
    topic
)

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    # sasl_mechanism=sasl_mechanisms,
    # sasl_plain_username=sasl_username,
    # sasl_plain_password=sasl_password,
    request_timeout_ms=session_timeout_ms,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='loc-group1',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
print("Starting!")


def write_data(row: str):
    with open("data.csv", "a") as f:
        f.write(row)


if __name__ == "__main__":
    for data in consumer:
        data: ConsumerRecord = data

        partition = data.partition
        print(f"\nPartition: {partition}")

        value = data.value
        print(f"Data: {value}")

        address = requests.get("http://api.positionstack.com/v1/reverse", params={
                               "access_key": "6cfa6a95ed324a6b9800fa6858a37245", 
                               "query": f"{value['lat']},{value['lon']}", 
                               "limit": "1"}).json()["data"][0]["label"]

        row = f"{value['system_time']},{value['lat']},{value['lon']},{value['alt']},\"{address}\"\n"
        write_data(row)
