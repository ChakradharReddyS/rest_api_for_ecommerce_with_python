import threading
import requests
import random
import time

# Webhook URL to send events
WEBHOOK_URL = 'http://localhost:8888/webhook'

# Constants for event types
EVENT_TYPES = ["access_app", "click_banner", "view_products", "select_product", "add_to_cart", "place_order"]

# Simulated user behavior ratios
EVENT_PROBABILITIES = [0.2, 0.2, 0.2, 0.2, 0.15, 0.05]

# Indian city names for users
INDIAN_CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad", "Jaipur", "Lucknow"]

# Number of users and batch size
NUM_USERS = 50
BATCH_SIZE = 5

# Function to generate events
def generate_event(user_id):
    event_type = random.choices(EVENT_TYPES, EVENT_PROBABILITIES)[0]
    city = random.choice(INDIAN_CITIES)
    return {"timestamp": time.time(), "user_id": user_id, "event_type": event_type, "city": city}

# Function to send events to the Webhook with a retry mechanism
def event_webhook(events):
    max_retries = 3
    retry_delay = 1

    for retry in range(max_retries):
        try:
            response = requests.post(WEBHOOK_URL, json=events)
            if response.status_code == 200:
                return True
            else:
                print(f"Retry {retry + 1}/{max_retries}: Event sending failed - {response.status_code}")
                time.sleep(retry_delay)
        except Exception as e:
            print(f"Retry {retry + 1}/{max_retries}: Error sending event - {str(e)}")
            time.sleep(retry_delay)

    print(f"Events failed to send after {max_retries} retries")
    return False

# Event Producer (DRIVER) class
class DRIVER(threading.Thread):
    def __init__(self, user_id):
        threading.Thread.__init__(self)
        self.user_id = user_id

    def run(self):
        
        #Creating batches of events
        batch = []
        for i in range(BATCH_SIZE):
            event = generate_event(self.user_id)
            batch.append(event)

        #logging the response time of API calls
        start_time = time.time()
        response = event_webhook(batch)
        end_time=time.time()
        response_time = (end_time - start_time) * 1000
       
        if response:
            print(f"User {self.user_id} - Events successfully sent - Response time:{response_time:.2f} ms ")
        else:
            print(f"User {self.user_id} - Events failed to send")


if __name__ == "__main__":
    # Create and start user threads
    user_threads = []
    for user_id in range(NUM_USERS):
        user_thread = DRIVER(user_id)
        user_thread.start()
        user_threads.append(user_thread)

    # Wait for user threads to finish (you can set a time limit or use other exit conditions)
    for user_thread in user_threads:
        user_thread.join()
