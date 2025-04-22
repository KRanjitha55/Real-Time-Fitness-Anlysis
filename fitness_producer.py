# fitness_producer.py

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_fitness_data():
    return {
        "user_id": str(random.randint(1000, 2000)),
        "heart_rate": random.randint(60, 180),
        "steps": random.randint(0, 10000),
        "calories": round(random.uniform(100.0, 800.0), 2),
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Fixed format
    }

if __name__ == "__main__":
    # Number of records to produce
    num_records = 10  # Change this number as needed
    
    print(f"Producing {num_records} records...")
    
    for i in range(num_records):
        data = generate_fitness_data()
        print(f"[Producer] Sending record {i+1}/{num_records}: {data}")
        producer.send('fitness-topic', value=data)
        time.sleep(1)  # 1 second delay between records
    
    # Flush and close the producer
    producer.flush()
    producer.close()
    print(f"\n✅ Successfully produced {num_records} records")