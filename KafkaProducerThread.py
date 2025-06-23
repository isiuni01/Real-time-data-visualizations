import csv
from pykafka import KafkaClient
import json
from time import sleep
import threading
from datetime import datetime, timezone

KAFKA_BROKER = "localhost:9091"
client = KafkaClient(hosts=KAFKA_BROKER)

START_ROW = 1
END_ROW = 620402

file_paths = [
    "orderedData/ITA.csv","orderedData/USA.csv","orderedData/GBR.csv",
    "orderedData/NZL.csv","orderedData/FRA.csv","orderedData/SUI.csv"
]
topics = ["ITA","USA","GBR","NZL","FRA","SUI"]

num_threads = len(file_paths)
barrier = threading.Barrier(num_threads)

def invia_messaggi_a_kafka(file_path, start_row, end_row, topic_name):
    topic = client.topics[topic_name]
    producer = topic.get_sync_producer()
    print(f"Avvio l'invio dei messaggi dal file: {file_path} al topic: {topic_name} \n")

    with open(file_path, 'r') as file:
        reader = list(csv.DictReader(file))
        for i in range(start_row, min(end_row, len(reader))):
            row = reader[i]
            current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            row['time'] = current_time
            row['timestamp'] = current_time
            messaggio = json.dumps(row).encode('utf-8')
            producer.produce(messaggio)
            if (i - start_row + 1) % 100 == 0:
                print(f"[{file_path}] Inviate {i - start_row + 1} righe...")
            sleep(0.05)
            barrier.wait()

threads = []
for file_path, topic in zip(file_paths, topics):
    thread = threading.Thread(target=invia_messaggi_a_kafka, 
        args=(file_path, START_ROW, END_ROW, "boat_data"))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Tutti i file sono stati processati.")
