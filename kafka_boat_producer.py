#!/usr/bin/env python3
"""
Simple Kafka Boat Data Producer with Threading

This script reads boat racing data from CSV files using Polars library,
filters data based on opponent boats and race, and sends the data to a Kafka topic.
Uses two threads to send data from both boats simultaneously.

Configuration is done via variables at the top of the script.
"""

import json
from time import sleep
from datetime import datetime, timezone
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor

import polars as pl
from pykafka import KafkaClient

# =============================================================================
# CONFIGURATION - MODIFY THESE VARIABLES AS NEEDED
# =============================================================================

# Boat selection (choose 2 boats from: FRA, GBR, ITA, NZL, SUI, USA)
BOAT1 = "FRA"
BOAT2 = "SUI"

# Race selection (will filter by this race)
RACE_NAME = "RR1_Match_1"

# Kafka configuration
KAFKA_TOPIC = "boat_racing_data"
KAFKA_SERVER = "localhost:9092"

# Data folder
DATA_FOLDER = Path("orderedData")

# =============================================================================
# FUNCTIONS
# =============================================================================

def setup_kafka_producer():
    """Create and return a Kafka producer instance."""
    try:
        client = KafkaClient(hosts=KAFKA_SERVER)
        topic = client.topics[KAFKA_TOPIC.encode('utf-8')]
        producer = topic.get_sync_producer()
        return producer
    except Exception as e:
        print(f"✗ Error connecting to Kafka: {e}")
        raise

def load_boat_data(boat_name):
    """Load boat data from CSV file using Polars."""
    csv_file = DATA_FOLDER / f"{boat_name}.csv"
    
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    
    print(f"📁 [Thread-{boat_name}] Loading data for boat: {boat_name}")
    
    try:
        # Read CSV with '|' separator (based on the data structure we observed)
        df = pl.read_csv(
            csv_file,
            separator='|',
            has_header=True,
            ignore_errors=True
        )
        
        print(f"   [Thread-{boat_name}] Loaded {len(df)} total rows")
        return df
    
    except Exception as e:
        print(f"✗ [Thread-{boat_name}] Error loading data for boat {boat_name}: {e}")
        raise

def filter_data(df, opponent_boat, race_name, boat_name):
    """Filter DataFrame based on opponent and race."""
    print(f"🔍 [Thread-{boat_name}] Filtering data where opponent={opponent_boat} and race={race_name}")
    
    # Check if required columns exist
    if 'opponent' not in df.columns:
        raise ValueError("'opponent' column not found in the data!")
    if 'race' not in df.columns:
        raise ValueError("'race' column not found in the data!")
    
    # Filter data
    filtered_df = df.filter(
        (pl.col('opponent') == opponent_boat) & 
        (pl.col('race') == race_name)
    )
    
    print(f"   [Thread-{boat_name}] Found {len(filtered_df)} matching rows")
    return filtered_df

def send_to_kafka(producer, boat_name, data):
    """Send boat data to Kafka topic."""
    if len(data) == 0:
        print(f"⚠️  [Thread-{boat_name}] No data to send for {boat_name}")
        return 0
    
    print(f"📤 [Thread-{boat_name}] Sending {len(data)} rows for {boat_name}")
    
    # Convert DataFrame to list of dictionaries
    rows = data.to_dicts()
    
    sent_count = 0
    
    for i, row in enumerate(rows):
        try:
            # Aggiorna il campo 'time' con il tempo corrente nel formato richiesto
            current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            row['time'] = current_time
            messaggio = json.dumps(row).encode('utf-8')
            producer.produce(messaggio)
            sleep(0.05)
            
            sent_count += 1
            
            # Progress indicator
            if sent_count % 50 == 0:
                print(f"   [Thread-{boat_name}] Sent {sent_count}/{len(rows)} messages...")
                
        except Exception as e:
            print(f"✗ [Thread-{boat_name}] Failed to send message {i}: {e}")
    
    print(f"✓ [Thread-{boat_name}] Completed {boat_name}: {sent_count} messages sent")
    return sent_count

def process_boat_data(boat_name, opponent_boat, race_name):
    """Process and send data for a single boat (used in threading)."""
    try:
        print(f"🔄 [Thread-{boat_name}] Starting processing for {boat_name}...")
        
        # Setup Kafka producer for this thread
        producer = setup_kafka_producer()
        
        # Load and filter data
        boat_data = load_boat_data(boat_name)
        boat_filtered = filter_data(boat_data, opponent_boat, race_name, boat_name)
        
        # Send to Kafka
        sent_count = send_to_kafka(producer, boat_name, boat_filtered)
        
        # Close producer
        producer.stop()
        
        return sent_count
        
    except Exception as e:
        print(f"✗ [Thread-{boat_name}] Error processing {boat_name}: {e}")
        return 0

def main():
    """Main function."""
    print("=" * 60)
    print("🚤 Kafka Boat Data Producer (Multi-threaded)")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  Boats: {BOAT1} vs {BOAT2}")
    print(f"  Race: {RACE_NAME}")
    print(f"  Kafka Topic: {KAFKA_TOPIC}")
    print(f"  Kafka Server: {KAFKA_SERVER}")
    print(f"  Threading: 2 simultaneous threads")
    print("-" * 60)
    
    # Validate boats are different
    if BOAT1 == BOAT2:
        print("✗ Error: BOAT1 and BOAT2 must be different!")
        return
    
    # Check data folder exists
    if not DATA_FOLDER.exists():
        print(f"✗ Error: Data folder '{DATA_FOLDER}' not found!")
        return
    
    try:
        # Test Kafka connection first
        print(f"🔌 Testing Kafka connection...")
        test_producer = setup_kafka_producer()
        test_producer.stop()
        print(f"✓ Kafka connection successful!")
        
        print(f"\n🚀 Starting simultaneous data processing with 2 threads...")
        
        # Use ThreadPoolExecutor to run both boats simultaneously
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both boat processing tasks
            future1 = executor.submit(process_boat_data, BOAT1, BOAT2, RACE_NAME)
            future2 = executor.submit(process_boat_data, BOAT2, BOAT1, RACE_NAME)
            
            print(f"⏳ Both threads started, waiting for completion...")
            
            # Wait for both threads to complete and get results
            sent1 = future1.result()
            sent2 = future2.result()
        
        total_sent = sent1 + sent2
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 SUMMARY")
        print("=" * 60)
        print(f"Race: {RACE_NAME}")
        print(f"Boats: {BOAT1} vs {BOAT2}")
        print(f"Messages sent from {BOAT1}: {sent1}")
        print(f"Messages sent from {BOAT2}: {sent2}")
        print(f"Total messages sent: {total_sent}")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print("✓ Processing completed!")
        
    except Exception as e:
        print(f"✗ Fatal error: {e}")

if __name__ == "__main__":
    main()
