import csv
import json
from confluent_kafka import Producer
import sys
CSV_PATH = sys.argv[1]

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "covid-datos"

def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    def delivery_report(err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Sent to {msg.topic()} offset={msg.offset()}")

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.produce(TOPIC, value=json.dumps(row).encode("utf-8"),
                             callback=delivery_report)

    producer.flush()

if __name__ == "__main__":
    main()
