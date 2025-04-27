# fitness_producer.py

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import uuid

# ================================
# Initialize Kafka Producer
# ================================
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# ================================
# Generate Random User IDs
# ================================
USERS = [f"user_{i}" for i in range(1, 6)]  # Create 5 sample users

# ================================
# Data Generation Functions
# ================================
def generate_fitness_data():
    """Generate real-time fitness data"""
    return {
        "user_id": random.choice(USERS),
        "heart_rate": random.randint(60, 150),
        "steps": random.randint(0, 500),
        "calories": round(random.uniform(0, 100), 2),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def generate_daily_summary():
    """Generate daily summary data"""
    return {
        "user_id": random.choice(USERS),
        "total_steps": random.randint(5000, 15000),
        "total_calories": round(random.uniform(1500, 3000), 2),
        "avg_heart_rate": random.randint(70, 100),
        "date": datetime.now().strftime("%Y-%m-%d")
    }

def generate_alert():
    """Generate alert data"""
    alert_types = ["high_heart_rate", "low_activity", "goal_achieved"]
    alert_messages = {
        "high_heart_rate": "Heart rate is above normal range!",
        "low_activity": "No activity detected in the last hour!",
        "goal_achieved": "Daily step goal achieved! Congratulations!"
    }
    
    alert_type = random.choice(alert_types)
    return {
        "user_id": random.choice(USERS),
        "alert_type": alert_type,
        "alert_message": alert_messages[alert_type],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# ================================
# Send Data to Kafka
# ================================
def send_to_kafka(topic, data):
    """Send data to specified Kafka topic"""
    try:
        producer.send(topic, value=data)
        producer.flush()
        print(f"✅ Sent to {topic}: {data}")
    except Exception as e:
        print(f"❌ Error sending to {topic}: {e}")

# ================================
# Main Production Loop
# ================================
def main():
    print("🚀 Starting Fitness Data Producer...\n")
    
    while True:
        try:
            # Send real-time fitness data (every 2 seconds)
            send_to_kafka("fitness-topic", generate_fitness_data())
            
            # Occasionally send daily summaries (10% chance)
            if random.random() < 0.1:
                send_to_kafka("daily-summary-topic", generate_daily_summary())
            
            # Occasionally send alerts (5% chance)
            if random.random() < 0.05:
                send_to_kafka("alert-topic", generate_alert())
            
            time.sleep(2)  # Wait for 2 seconds before next batch
            
        except KeyboardInterrupt:
            print("\n⚠️ Stopping the producer...")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            continue

if __name__ == "__main__":
    main()
