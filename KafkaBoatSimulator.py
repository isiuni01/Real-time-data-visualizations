#!/usr/bin/env python3
"""
Kafka Boat Data Simulator

This script simulates `n` boats using the same 6 CSV files for all boats.
Each boat gets a unique ID and all timestamps are set to current time.
Data is sent to the 'boat_data' Kafka topic.
"""

import csv
from pykafka import KafkaClient
import json
from time import sleep
from datetime import datetime, timezone
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import time

# =============================================================================
# CONFIGURATION - MODIFY THESE VARIABLES AS NEEDED
# =============================================================================

# Number of boats to simulate
N_BOATS = 10000  # Set this to your desired number of simulated boats

# Thread pool configuration
MAX_WORKER_THREADS = 20  # Number of worker threads in the pool
BOATS_PER_THREAD = None  # Will be calculated automatically

# Kafka configuration
KAFKA_BROKER = "localhost:9091"
TOPIC_NAME = "boat_data"

# Data range configuration
START_ROW = 1
END_ROW = 10000  # Reduced for simulation purposes

# File paths for boat data (all boats will use these files)
file_paths = [
    "orderedData/ITA.csv", "orderedData/USA.csv", "orderedData/GBR.csv",
    "orderedData/NZL.csv", "orderedData/FRA.csv", "orderedData/SUI.csv"
]

# Global Kafka client and shared data structures
client = None
preloaded_data = {}  # Will store CSV data in memory
boat_queue = queue.Queue()  # Queue of boats waiting to send data
active_boats = {}  # Currently active boats with their state

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def preload_csv_files():
    """Prepare file paths and configuration for each boat task."""
    global preloaded_data
    
    print("📚 Preparing CSV file configurations...")
    
    for file_path in file_paths:
        try:
            print(f"  📖 Preparing configuration for {file_path}...")
            with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                # Only store file headers needed
                preloaded_data[file_path] = reader.fieldnames
                print(f"  ✓ Configuration ready for {file_path}")
        except Exception as e:
            print(f"  ✗ Failed to prepare configuration for {file_path}: {e}")
            exit(1)
    
    print("✓ All CSV configurations prepared successfully!")
    print("-" * 70)

def calculate_boats_per_thread():
    """Calculate optimal boats per thread based on total boats and worker threads."""
    global BOATS_PER_THREAD
    
    # Calculate base boats per thread
    base_boats_per_thread = max(1, N_BOATS // MAX_WORKER_THREADS)
    
    # Add buffer to handle uneven distribution and ensure smooth operation
    # This allows threads to pick up extra boats when others finish early
    buffer_factor = 1.5 if N_BOATS > MAX_WORKER_THREADS else 1.0
    BOATS_PER_THREAD = max(1, int(base_boats_per_thread * buffer_factor))
    
    # Ensure reasonable limits
    BOATS_PER_THREAD = min(BOATS_PER_THREAD, 50)  # Cap at 50 boats per thread
    BOATS_PER_THREAD = max(BOATS_PER_THREAD, 1)   # Minimum 1 boat per thread
    
    return BOATS_PER_THREAD

def main():
    """Main function to coordinate all boat data simulation."""
    # Calculate optimal boats per thread
    boats_per_thread = calculate_boats_per_thread()
    
    print("=" * 70)
    print("🚤 Kafka Boat Data Simulator")
    print("=" * 70)
    print(f"Configuration:")
    print(f"  🔢 Number of boats to simulate: {N_BOATS}")
    print(f"  📤 Kafka Topic: {TOPIC_NAME}")
    print(f"  🔌 Kafka Server: {KAFKA_BROKER}")
    print(f"  📝 Data Range: rows {START_ROW} to {END_ROW}")
    print(f"  📁 Data files: {len(file_paths)} CSV files available")
    print(f"  🧵 Worker threads: {MAX_WORKER_THREADS} threads in pool (processing {N_BOATS} boats)")
    print(f"  ⚙️  Boats per thread: {boats_per_thread} (auto-calculated)")
    print(f"  📊 Load distribution: {N_BOATS/MAX_WORKER_THREADS:.1f} boats per thread average")
    print("-" * 70)
    
    # Preload CSV files into memory
    preload_csv_files()
    
    # Initialize Kafka client
    print("🔌 Connecting to Kafka...")
    try:
        global client
        client = KafkaClient(hosts=KAFKA_BROKER)
        print(f"✓ Connected to Kafka broker: {KAFKA_BROKER}")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        exit(1)

class BoatSimulationState:
    """Holds the state of a boat simulation."""
    def __init__(self, boat_id, file_path, start_row, end_row, topic_name):
        self.boat_id = boat_id
        self.file_path = file_path
        self.start_row = start_row
        self.end_row = end_row
        self.topic_name = topic_name
        self.unique_boat_id = f"BOAT_{boat_id:03d}"
        self.original_boat = file_path.split('/')[-1].replace('.csv', '')
        self.data_lines = None
        self.current_index = 0
        self.sent_count = 0
        self.last_send_time = 0
        self.completed = False
        self.load_data()
    
    def load_data(self):
        """Load only the required lines for this boat."""
        with open(self.file_path, 'r') as file:
            reader = csv.DictReader(file)
            # Skip to start_row
            for _ in range(self.start_row):
                next(reader, None)
            # Read only the lines we need
            self.data_lines = []
            for i, row in enumerate(reader):
                if i >= (self.end_row - self.start_row):
                    break
                self.data_lines.append(row)
    
    def has_next_message(self):
        """Check if boat has more messages to send."""
        return self.current_index < len(self.data_lines) and not self.completed
    
    def can_send_now(self):
        """Check if enough time has passed for the next message (50ms interval)."""
        current_time = time.time()
        return (current_time - self.last_send_time) >= 0.05
    
    def get_next_message(self):
        """Get the next message to send."""
        if not self.has_next_message():
            return None
        
        row = self.data_lines[self.current_index].copy()
        
        # Set current timestamp (NOW)
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        row['time'] = current_time
        row['timestamp'] = current_time
        
        # Change boat column to unique ID
        row['boat'] = self.unique_boat_id
        
        self.current_index += 1
        self.sent_count += 1
        self.last_send_time = time.time()
        
        if self.current_index >= len(self.data_lines):
            self.completed = True
        
        return row

def worker_thread_simulation():
    """Worker thread that processes boat data in synchronized rounds."""
    thread_id = threading.current_thread().name
    
    # Initialize Kafka producer for this thread
    topic = client.topics[TOPIC_NAME.encode('utf-8')]
    producer = topic.get_sync_producer()
    
    print(f"🔧 [Thread-{thread_id}] Worker thread started")
    
    # 1. Acquire a share of boats from the main queue
    local_boats = []
    while len(local_boats) < BOATS_PER_THREAD:
        try:
            boat_state = boat_queue.get_nowait()
            local_boats.append(boat_state)
        except queue.Empty:
            break  # No more boats in the queue

    if not local_boats:
        print(f"🏁 [Thread-{thread_id}] No boats to simulate. Worker exiting.")
        producer.stop()
        return 0

    print(f"📥 [Thread-{thread_id}] Acquired {len(local_boats)} boats to simulate.")
    
    total_messages_sent = 0
    max_rounds = 0
    for boat in local_boats:
        max_rounds = max(max_rounds, len(boat.data_lines))

    print(f"   [Thread-{thread_id}] Will perform {max_rounds} rounds of message sending.")

    # 2. Loop through rounds (message indices)
    for round_index in range(max_rounds):
        
        # 3. Inner loop through this thread's boats for the current round
        for boat_state in local_boats:
            
            # Skip if boat is already finished with all its data
            if not boat_state.has_next_message():
                continue

            # Wait until it's time for this specific boat to send its message
            while not boat_state.can_send_now():
                time.sleep(0.001)  # Sleep briefly to avoid busy-waiting

            # Get and send the next message for the boat
            message_data = boat_state.get_next_message()
            if message_data:
                message = json.dumps(message_data).encode('utf-8')
                producer.produce(message)
                total_messages_sent += 1

                # Progress indicator
                if boat_state.sent_count % 100 == 0:
                    print(f"   📡 [Thread-{thread_id}][{boat_state.unique_boat_id}] Sent {boat_state.sent_count}/{len(boat_state.data_lines)} messages")

        # Log the completion of a round for this thread
        if (round_index + 1) % 10 == 0:
            print(f"   [Thread-{thread_id}] Completed round {round_index + 1}/{max_rounds}")

    # Final logging for all completed boats in this thread
    for boat_state in local_boats:
        if boat_state.completed:
            print(f"✅ [Thread-{thread_id}][{boat_state.unique_boat_id}] Completed: {boat_state.sent_count} messages sent")

    producer.stop()
    print(f"🏁 [Thread-{thread_id}] Worker completed. Total messages sent: {total_messages_sent}")
    return total_messages_sent

if __name__ == "__main__":
    # Run main initialization
    main()
    
    print(f"\n🚀 Starting lock-step simulation of {N_BOATS} boats using {MAX_WORKER_THREADS} worker threads...")
    print(f"💾 Each boat sends telemetry every 50ms (real-time interval respected)")
    print(f"🔄 Threads will process boats in synchronized rounds (lock-step)")
    print(f"⚙️  Auto-calculated: {BOATS_PER_THREAD} boats per thread capacity")
    
    # Create boat simulation states and add to queue
    print(f"📋 Creating {N_BOATS} boat simulation states...")
    for boat_id in range(1, N_BOATS + 1):
        # Each boat gets a random file to simulate diversity
        selected_file = random.choice(file_paths)
        boat_state = BoatSimulationState(boat_id, selected_file, START_ROW, END_ROW, TOPIC_NAME)
        boat_queue.put(boat_state)
    
    print(f"✅ All {N_BOATS} boats added to simulation queue")
    print(f"⏳ Starting {MAX_WORKER_THREADS} worker threads...")
    
    # Start worker threads
    start_time = time.time()
    total_messages = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS, thread_name_prefix="BoatWorker") as executor:
        print(f"\n🚀 Lock-step boat simulation started!")
        print(f"📡 Each boat transmits every 50ms, threads process in synchronized rounds")
        print("-" * 70)
        
        # Submit worker threads
        futures = [executor.submit(worker_thread_simulation) for _ in range(MAX_WORKER_THREADS)]
        
        # Wait for all threads to complete and collect results
        for future in as_completed(futures):
            try:
                messages_sent = future.result()
                total_messages += messages_sent
            except Exception as exc:
                print(f"❌ Worker thread generated an exception: {exc}")
    
    end_time = time.time()
    simulation_time = end_time - start_time
    
    # Final summary
    print("\n" + "=" * 70)
    print("📊 LOCK-STEP BOAT SIMULATION COMPLETE")
    print("=" * 70)
    print(f"🚤 Total boats simulated: {N_BOATS}")
    print(f"📤 Total messages sent: {total_messages:,}")
    print(f"⏱️  Total simulation time: {simulation_time:.2f} seconds")
    print(f"📡 Average message rate: {total_messages/simulation_time:.1f} messages/second")
    print(f"📤 Kafka topic: {TOPIC_NAME}")
    print(f"📝 Data range per boat: rows {START_ROW} to {END_ROW}")
    print(f"🧵 Worker threads used: {MAX_WORKER_THREADS}")
    print(f"🔄 Each boat transmitted every 50ms (processed in lock-step rounds)")
    print(f"⚡ Thread efficiency: Round-based processing")
    print("=" * 70)
