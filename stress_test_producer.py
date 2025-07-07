#!/usr/bin/env python3
"""
Kafka Stress Test Producer

Single-threaded producer that every 50ms sends N messages (one per boat) as fast as possible.
Uses ALL CSV columns, only changes boat name and timestamp.

Usage:
    python stress_test_producer.py --boats 10 --duration 60
    python stress_test_producer.py -n 100 -d 120
"""

import csv
from pykafka import KafkaClient
import json
from time import sleep, time
from datetime import datetime, timezone
import argparse
import sys
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor

# =============================================================================
# CONFIGURATION
# =============================================================================

KAFKA_BROKER = "localhost:9091"
TOPIC_NAME = "boat_data"
DATA_FOLDER = "orderedData"
REFERENCE_BOAT = "ITA"
BATCH_INTERVAL = 0.05  # 50ms

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Kafka stress test producer")
    parser.add_argument('-n', '--boats', type=int, default=6, help='Number of boats to simulate (default: 6)')
    parser.add_argument('-d', '--duration', type=int, help='Duration in seconds (default: unlimited)')
    parser.add_argument('--kafka-broker', default=KAFKA_BROKER, help=f'Kafka broker (default: {KAFKA_BROKER})')
    parser.add_argument('--topic', default=TOPIC_NAME, help=f'Kafka topic (default: {TOPIC_NAME})')
    parser.add_argument('--data-folder', default=DATA_FOLDER, help=f'Data folder (default: {DATA_FOLDER})')
    
    args = parser.parse_args()
    
    if args.boats <= 0:
        print("✗ Number of boats must be positive")
        sys.exit(1)
    
    # Generate boat names: BOAT_001, BOAT_002, etc.
    boat_names = [f"BOAT_{i+1:03d}" for i in range(args.boats)]
    
    print("=" * 70)
    print("🚤 Kafka Stress Test Producer")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  📊 Boats: {args.boats} boats ({boat_names[0]} to {boat_names[-1]})")
    print(f"  📤 Kafka Topic: {args.topic}")
    print(f"  🔌 Kafka Server: {args.kafka_broker}")
    print(f"  📁 Data Folder: {args.data_folder}")
    print(f"  ⏰ Duration: {args.duration}s" if args.duration else "  ⏰ Duration: unlimited")
    print(f"  ⚡ Batch interval: {BATCH_INTERVAL*1000}ms")
    print(f"  🎯 Expected rate: ~{args.boats / BATCH_INTERVAL:.0f} msg/sec")
    print("-" * 70)
    
    # Initialize Kafka client and multiple producers for threading
    print("🔌 Connecting to Kafka...")
    try:
        client = KafkaClient(hosts=args.kafka_broker)
        topic = client.topics[args.topic.encode('utf-8')]
        
        # Create multiple producers for parallel sending
        num_producer_threads = min(args.boats, 20)  # Max 20 threads
        producers = []
        for i in range(num_producer_threads):
            producer = topic.get_sync_producer()
            producers.append(producer)
        
        print(f"✓ Connected to Kafka broker: {args.kafka_broker}")
        print(f"✓ Created {num_producer_threads} producer threads")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        sys.exit(1)
    
    # Load CSV data - ALL columns like original
    reference_file = Path(args.data_folder) / f"{REFERENCE_BOAT}.csv"
    
    if not reference_file.exists():
        print(f"✗ Reference file {reference_file} not found!")
        sys.exit(1)
    
    print(f"📁 Loading data from {reference_file}...")
    with open(reference_file, 'r') as file:
        csv_data = list(csv.DictReader(file))
        total_rows = len(csv_data)
        print(f"✓ Loaded {total_rows:,} rows with ALL columns")
    
    # Start stress test
    print(f"\n🚀 Starting stress test...")
    start_time = time()
    end_time = start_time + args.duration if args.duration else None
    
    current_row_index = 0
    total_messages_sent = 0
    batch_count = 0
    
    try:
        while True:
            batch_start_time = time()
            
            # Check duration limit
            if end_time and batch_start_time >= end_time:
                break
            
            # Check if we have more data
            if current_row_index >= total_rows:
                print("📄 Reached end of data")
                break
            
            # Get current row data
            row_data = csv_data[current_row_index].copy()
            
            # Send N messages (one per boat) in parallel using threading
            def send_boat_message(boat_name, producer_idx):
                """Send message for one boat using assigned producer."""
                boat_row_data = row_data.copy()
                boat_row_data['boat'] = boat_name
                current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                boat_row_data['time'] = current_time
                boat_row_data['timestamp'] = current_time
                
                # Use assigned producer for this thread
                producer = producers[producer_idx % len(producers)]
                messaggio = json.dumps(boat_row_data).encode('utf-8')
                producer.produce(messaggio)
                return 1
            
            # Use ThreadPoolExecutor to send all messages in parallel
            with ThreadPoolExecutor(max_workers=num_producer_threads) as executor:
                # Submit all boat messages to thread pool
                futures = []
                for i, boat_name in enumerate(boat_names):
                    future = executor.submit(send_boat_message, boat_name, i)
                    futures.append(future)
                
                # Wait for all messages to be sent
                messages_sent_in_batch = 0
                for future in futures:
                    try:
                        messages_sent_in_batch += future.result(timeout=0.04)  # 40ms timeout
                    except Exception as e:
                        print(f"✗ Error sending message: {e}")
                
                total_messages_sent += messages_sent_in_batch
            
            # Move to next row for all boats
            current_row_index += 1
            batch_count += 1
            
            # Progress report
            if batch_count % 100 == 0:
                elapsed = time() - start_time
                rate = total_messages_sent / elapsed
                print(f"📊 Batch {batch_count:,} | Row {current_row_index:,}/{total_rows:,} | "
                      f"{total_messages_sent:,} msgs | {rate:.0f} msg/s")
            
            # Wait for next batch time (50ms interval)
            batch_duration = time() - batch_start_time
            sleep_time = BATCH_INTERVAL - batch_duration
            if sleep_time > 0:
                sleep(sleep_time)
            elif batch_duration > BATCH_INTERVAL * 1.5:
                print(f"⚠️  Slow batch: {batch_duration*1000:.1f}ms")
            
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    
    # Final stats
    total_duration = time() - start_time
    avg_rate = total_messages_sent / total_duration
    
    print("\n" + "=" * 70)
    print("📊 STRESS TEST COMPLETE")
    print("=" * 70)
    print(f"🚤 Boats simulated: {args.boats}")
    print(f"📤 Total messages sent: {total_messages_sent:,}")
    print(f"📦 Total batches sent: {batch_count:,}")
    print(f"⏰ Duration: {total_duration:.1f} seconds")
    print(f"🎯 Average rate: {avg_rate:.1f} msg/sec")
    print(f"⚡ Target rate: {args.boats / BATCH_INTERVAL:.0f} msg/sec")
    print(f"📈 Efficiency: {(avg_rate / (args.boats / BATCH_INTERVAL)) * 100:.1f}%")
    print("=" * 70)
    
    # Stop all producers
    for producer in producers:
        producer.stop()

if __name__ == "__main__":
    main()
