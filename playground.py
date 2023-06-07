import json
import requests
from time import sleep
from kafka import KafkaProducer

# Define kafka broker details
kafka_broker = 'localhost:9092'


# Set up the kafka producer
# producer = KafkaProducer(bootstrap_servers=kafka_broker)
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

# Fetch the improvised streamed data and send to kafka


def get_data(url):
    response = ''
    try:
        response = requests.get(url)
    except:
        response = {'status_code':500}
        raise Exception('Error fetching data')
    return response

def stream_data(topic, url):
    while True:
        count = 1
        data = get_data(url).json()
        print(data)
        producer.send(topic, data)
        producer.flush()
        sleep(60)

