import time
import psycopg2
from confluent_kafka import Consumer, KafkaException
import json


# Database connection settings
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "chakradhar"
DB_USER = "chakradhar"
DB_PASSWORD = "1234"

# Kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'event-consumer',
    'auto.offset.reset': 'earliest',  # Set to 'earliest' to start from the beginning of the topic
}
kafka_consumer = Consumer(kafka_config)
kafka_consumer.subscribe(['events-topic2'])  # Subscribe to the Kafka topic

#creating a set to store unique event IDs (to handle duplicate data coming from client)
unique_events = set()

# Function to insert events into the database
def insert_events(events):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        for event in events:
            # Create a events_table if it does not exist 
            # Inserting events into my database table.
            sql = """CREATE TABLE IF NOT EXISTS app_events_table_v3 (
                    event_timestamp BIGINT,
                    user_id INTEGER,
                    event_type VARCHAR(255),
                    city VARCHAR(20)
                   );
                   INSERT INTO app_events_table_v3 (event_timestamp, user_id, event_type, city) VALUES (%s, %s, %s, %s)"""
            data = (event['timestamp'], event['user_id'], event['event_type'], event['city'])
            cursor.execute(sql, data)

        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error inserting events into the database: {str(e)}")
        return False

# Function to insert events into the database with retrial mechanism
def insert_events_with_retrial(events):
    max_retries = 3
    retry_delay = 1

    for retry in range(max_retries):
        if insert_events(events):
            print(f"Inserted {len(events)} events into the database.")
            break
        else:
            print(f"Retry {retry + 1}/{max_retries}: Failed to insert events into the database. Retrying...")
            time.sleep(retry_delay)
    else:
        print(f"Event insertion failed after {max_retries} retries")


def queue_consumer():
    try:
        while True:
            batch = []
            for i in range(BATCH_SIZE):
                # Poll for events from Kafka
                msg = kafka_consumer.poll(1.0)  # Adjust the timeout as needed
            
                if msg is None:
                    continue
            
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    event_str = msg.value().decode('utf-8')
                    # Deserialize the event (e.g., JSON parsing)
                    event_str = event_str.replace("'",'"')
                    event = json.loads(event_str)
                    
                    #Handling the duplicate events and adding only unique events to the batch
                    if event['event_id'] not in unique_events:
                        batch.append(event)

            if batch:
                # Attempt to insert events with a retrial mechanism
                insert_events_with_retrial(batch)
                # Add the unique event identifiers to the set
                unique_events.update(event['event_id'] for event in batch)

            time.sleep(1)  # Sleep for a while before processing the next batch

    except KeyboardInterrupt:
        print('Canceled by user')



if __name__ == "__main__":
    BATCH_SIZE = 10  # Adjust batch size as needed
    queue_consumer()
