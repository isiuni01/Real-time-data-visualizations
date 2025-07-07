#!/usr/bin/env python3
"""
Multi-threaded Kafka Boat Data Producer

This script processes multiple boat racing CSV files simultaneously using threading,
sending data from each boat to Kafka topics. All threads are synchronized using a barrier
to ensure coordinated data transmission.

Each boat runs in its own thread and sends data to the shared 'boat_data' topic.
"""

import csv
from pykafka import KafkaClient
import json
from time import sleep
import threading
from datetime import datetime, timezone

# =============================================================================
# CONFIGURATION
# =============================================================================

# Kafka configuration
KAFKA_BROKER = "localhost:9091"
TOPIC_NAME = "boat_data"

# Data range configuration
START_ROW = 1
END_ROW = 620402

# File paths and corresponding boat names
file_paths = [
    "orderedData/ITA.csv", "orderedData/USA.csv", "orderedData/GBR.csv",
    "orderedData/NZL.csv", "orderedData/FRA.csv", "orderedData/SUI.csv"
]
boat_names = ["ITA", "USA", "GBR", "NZL", "FRA", "SUI"]

# Threading setup
num_threads = len(file_paths)
barrier = threading.Barrier(num_threads)

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function to coordinate all boat data processing."""
    print("=" * 70)
    print("🚤 Multi-threaded Kafka Boat Data Producer")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  📊 Boats: {', '.join(boat_names)}")
    print(f"  📤 Kafka Topic: {TOPIC_NAME}")
    print(f"  🔌 Kafka Server: {KAFKA_BROKER}")
    print(f"  📝 Data Range: rows {START_ROW} to {END_ROW}")
    print(f"  🧵 Threads: {num_threads} simultaneous threads")
    print("-" * 70)
    
    # Initialize Kafka client
    print("🔌 Connecting to Kafka...")
    try:
        global client
        client = KafkaClient(hosts=KAFKA_BROKER)
        print(f"✓ Connected to Kafka broker: {KAFKA_BROKER}")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        exit(1)

def invia_messaggi_a_kafka(file_path, start_row, end_row, topic_name):
    """Send boat data messages to Kafka topic with synchronized threading."""
    # Extract boat name from file path for cleaner logging
    boat_name = file_path.split('/')[-1].replace('.csv', '')
    
    try:
        topic = client.topics[topic_name.encode('utf-8')]
        producer = topic.get_sync_producer()
        print(f"🚀 [Thread-{boat_name}] Starting data transmission to topic: {topic_name}")
        
        with open(file_path, 'r') as file:
            reader = list(csv.DictReader(file))
            total_rows = min(end_row, len(reader)) - start_row
            
            print(f"📁 [Thread-{boat_name}] Loaded {len(reader)} total rows, sending {total_rows} rows")
            
            sent_count = 0
            for i in range(start_row, min(end_row, len(reader))):
                row = reader[i]
                
                # Add timestamp fields
                current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                row['time'] = current_time
                row['timestamp'] = current_time
                
                # Send message
                messaggio = json.dumps(row).encode('utf-8')
                producer.produce(messaggio)
                sent_count += 1
                
                # Progress indicator
                if sent_count % 100 == 0:
                    print(f"   [Thread-{boat_name}] Sent {sent_count}/{total_rows} messages...")
                
                sleep(0.01)
                barrier.wait()  # Synchronize all threads
            
            print(f"✓ [Thread-{boat_name}] Completed {boat_name}: {sent_count} messages sent")
            producer.stop()
            
    except Exception as e:
        print(f"✗ [Thread-{boat_name}] Error processing {boat_name}: {e}")

if __name__ == "__main__":
    # Run main initialization
    main()
    
    print("\n🚀 Starting all boat threads simultaneously...")
    
    # Create and start threads
    threads = []
    for file_path in file_paths:
        thread = threading.Thread(
            target=invia_messaggi_a_kafka, 
            args=(file_path, START_ROW, END_ROW, TOPIC_NAME)
        )
        threads.append(thread)
        thread.start()
    
    print(f"⏳ All {len(threads)} threads started, waiting for completion...")
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Final summary
    print("\n" + "=" * 70)
    print("📊 PROCESSING COMPLETE")
    print("=" * 70)
    print(f"🚤 Boats processed: {', '.join(boat_names)}")
    print(f"📤 Kafka topic: {TOPIC_NAME}")
    print(f"📝 Data range: rows {START_ROW} to {END_ROW}")
    print(f"✓ All {len(file_paths)} boat data files have been processed successfully!")
    print("=" * 70)
