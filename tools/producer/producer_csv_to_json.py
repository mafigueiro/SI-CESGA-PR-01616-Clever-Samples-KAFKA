import csv
import json
from confluent_kafka import Producer
import sys

# Argumentos:
#   1 → ruta del CSV
#   2 → número de filas a enviar
if len(sys.argv) < 3:
    print("Uso: python producer_csv_to_json.py <csv_path> <num_filas>")
    sys.exit(1)

CSV_PATH = sys.argv[1]
MAX_ROWS = int(sys.argv[2])

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

        count = 0
        for row in reader:
            if count >= MAX_ROWS:
                break

            producer.produce(
                TOPIC,
                value=json.dumps(row).encode("utf-8"),
                callback=delivery_report
            )

            count += 1

    producer.flush()
    print(f"Enviadas {count} filas al topic '{TOPIC}'")


if __name__ == "__main__":
    main()
