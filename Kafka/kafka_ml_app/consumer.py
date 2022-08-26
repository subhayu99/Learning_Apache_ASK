from kafka.consumer.fetcher import ConsumerRecord
from kafka.consumer import KafkaConsumer
from random import choice
from time import sleep
from json import loads
import requests
import os

consumer = KafkaConsumer(
    'user',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='user-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

def write_data(row: str):
    with open("data.csv", "a") as f:
        f.write(row)

def determine_gender(fname: str) -> str:
    gender_data = requests.get("https://api.genderize.io/", params={"name": fname})
    gender_data = loads(gender_data.text)
    # gender_data = {"gender": choice(["male", "female"])}
    return gender_data["gender"]

if __name__ == "__main__":
    for msg in consumer:
        msg: ConsumerRecord = msg
        partition = msg.partition
        user = msg.value
        fname = user["fname"]
        original_gender = user["gender"]
        determined_gender = determine_gender(fname)
        
        res = False
        if determined_gender == original_gender:
            res = True
        
        row = f"{partition},{fname},{original_gender},{determined_gender},{res}\n"
        write_data(row)
        
        print(f"Data: {user}")