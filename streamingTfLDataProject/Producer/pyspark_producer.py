import json
import requests
from time import sleep
from kafka import KafkaProducer

# Define kafka broker details
kafka_broker = 'localhost:9092'
topic = 'TfL-bus-arrival'

# Define the target url
url = "https://api.tfl.gov.uk/Stoppoint?lat=51.4929&lon=" \
      "0.053929&stoptypes=NaptanBusCoachStation," \
      "NaptanPublicBusCoachTram"

# Set up the kafka producer
# producer = KafkaProducer(bootstrap_servers=kafka_broker)
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

# Fetch the improvised streamed data and send to kafka


def get_bus_arrivals():
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Error getting bus arrivals: {}".format(response.status_code))


def stream_bus_arrivals():
    while True:
        count = 1
        arrivals = get_bus_arrivals()
        print(arrivals)
        producer.send(topic, arrivals)
        producer.flush()
        sleep(60)


if __name__ == "__main__":
    stream_bus_arrivals()
