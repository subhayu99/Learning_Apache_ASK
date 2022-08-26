from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
import requests

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x:
    dumps(x).encode('utf-8')
)


if __name__ == "__main__":
    data = requests.get("https://randomuser.me/api/", params={"results":100})
    data = loads(data.text)

    for user in data["results"]:
        send_data = {
            "fname": user["name"]["first"],
            "gender": user["gender"]
        }
        producer.send('user', value=send_data)
        print(f"Sent data: {send_data}")
        # sleep(2)