from flask import Flask, request, jsonify
import time
from confluent_kafka import Producer
import json

app = Flask(__name__)


# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'webhook',
}
kafka_producer = Producer(kafka_config)

@app.route('/webhook', methods=['POST'])
def webhook():
    events = request.get_json()

    # Process events and handle retries
    if process_events(events):
        return jsonify({'message': 'Events received and processed successfully'})
    else:
        return jsonify({'message': 'Event processing failed after retries'})

# Function to push events to the in-memory queue i.e Kafka and implement a retry mechanism
def process_events(events):
    max_retries = 3
    retry_delay = 1

    for event in events:
        for retry in range(max_retries):
            try:
                # Pushing the event to the in-memory queue (kafka)

                # Serialize the event as a string (you can use JSON or any format)
                event_json = json.dumps({
                    "timestamp": event["timestamp"],
                    "user_id": event["user_id"],
                    "event_type": event["event_type"],
                    "city": event["city"],
                    "event_id":str(event["timestamp"]) + "_" + str(event["user_id"]) + "_" + event["event_type"]
                })
                
                # Produce the event to a Kafka topic
                kafka_producer.produce('events-topic2', value=event_json)
                
                # Flush the producer to send the event
                kafka_producer.flush()
                break  # Event successfully pushed to the queue
            
            except Exception as e:
                print(f"Retry {retry + 1}/{max_retries}: Error pushing event to the queue - {str(e)}")
                time.sleep(retry_delay)
        else:
            print(f"Event failed after {max_retries} retries")
            return False

    return True

if __name__ == '__main__':
    app.run(host='localhost', port=8888, threaded=True)
