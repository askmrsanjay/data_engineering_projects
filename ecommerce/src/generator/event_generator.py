import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker
fake = Faker()

# Kafka Configuration
BOOTSTRAP_SERVERS = ['localhost:29092']  # Using the host-accessible port
TOPIC_NAME = 'ecommerce-topic'

def get_producer():
    """Create and return a Kafka Producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        return producer
    except Exception as e:
        print(f"Error creating producer: {e}")
        return None

def generate_event(user_id, session_id):
    """Generate a single e-commerce event."""
    event_types = ['view_item', 'add_to_cart', 'purchase']
    platforms = ['web', 'ios', 'android']
    categories = ['Electronics', 'Apparel', 'Home', 'Books', 'Beauty']
    
    event_type = random.choice(event_types)
    category = random.choice(categories)
    
    # Prices vary by category roughly
    price = round(random.uniform(10.0, 1000.0), 2) if event_type in ['add_to_cart', 'purchase'] else None
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": f"P_{fake.bothify(text='??-####')}",
        "category": category,
        "price": price,
        "platform": random.choice(platforms)
    }
    return event

def run_generator():
    """Continuously generate and send events to Kafka."""
    producer = get_producer()
    if not producer:
        return

    print(f"Starting event generator. Sending to topic: {TOPIC_NAME}...")
    
    # Active users/sessions pool for realism
    users = [f"U{i:04d}" for i in range(1, 101)]
    active_sessions = {user: str(uuid.uuid4()) for user in users}

    try:
        while True:
            # Pick a random user
            user_id = random.choice(users)
            
            # Occasionally rotate session
            if random.random() < 0.05:
                active_sessions[user_id] = str(uuid.uuid4())
                
            session_id = active_sessions[user_id]
            
            # Generate and send event
            event = generate_event(user_id, session_id)
            producer.send(TOPIC_NAME, value=event)
            
            print(f"Sent: {event['event_type']} | User: {event['user_id']} | Product: {event['product_id']}")
            
            # Flush periodically
            producer.flush()
            
            # Sleep between events (simulating traffic)
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        print("Stopping generator...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_generator()
