
import json
import time
import uuid
import random
from faker import Faker
from datetime import timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prefect import flow, task

# Initialize Faker
fake = Faker()

# Define a list of cities
CITIES = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
    'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'
]

def generate_trip_data(city: str) -> dict:
    """
    Generates a single, realistic-looking ride-hailing trip record.
    """
    trip_duration_seconds = random.randint(300, 3600)
    start_time = fake.date_time_this_year()
    end_time = start_time + timedelta(seconds=trip_duration_seconds)

    return {
        "trip_id": str(uuid.uuid4()),
        "driver_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "pickup_datetime": start_time.isoformat(),
        "dropoff_datetime": end_time.isoformat(),
        "pickup_location": {
            "latitude": str(fake.latitude()),
            "longitude": str(fake.longitude())
        },
        "dropoff_location": {
            "latitude": str(fake.latitude()),
            "longitude": str(fake.longitude())
        },
        "fare_amount": round(random.uniform(5.0, 150.0), 2),
        "tip_amount": round(random.uniform(0.0, 50.0), 2),
        "city": city,
        "event_timestamp": time.time()
    }

# This is a simplified and more Prefect-idiomatic task.
@task(retries=3, retry_delay_seconds=10)
def create_kafka_producer(broker: str) -> KafkaProducer:
    """
    Creates and returns a KafkaProducer instance.
    This task will retry on failure thanks to the @task decorator.
    """
    print(f"Attempting to connect to Kafka broker at {broker}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Successfully connected to Kafka!")
        return producer
    except NoBrokersAvailable:
        print(f"Could not find any brokers at {broker}. Retrying...")
        raise  # Raise the exception to let Prefect handle the retry.

@flow(name="Ride-Hailing Data Producer Flow")
def producer_flow(broker: str = "localhost:9092", topic: str = "ride_events", rate: int = 1):
    """
    Prefect flow that orchestrates running the Kafka producer.
    """
    producer = create_kafka_producer(broker)
    delay = 1.0 / rate
    print(f"Starting to send data to topic '{topic}' at a rate of {rate} messages/sec.")
    
    try:
        while True:
            city = random.choice(CITIES)
            trip_data = generate_trip_data(city)
            producer.send(topic, value=trip_data)
            print(f"Sent: {trip_data['trip_id']} for city {trip_data['city']}")
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nProducer flow stopped.")
    except Exception as e:
        print(f"An unexpected error occurred in the producer flow: {e}")
    finally:
        print("Flushing remaining messages and closing producer...")
        if producer:
            producer.flush()
            producer.close()
        print("Producer closed.")

# This block now simply runs the flow directly, allowing for easy local testing.
if __name__ == "__main__":
    producer_flow(rate=2)




# The code without connecting to prefect
# import json
# import time
# import uuid
# import random
# import argparse
# from faker import Faker
# from datetime import timedelta
# from kafka import KafkaProducer
# from kafka.errors import NoBrokersAvailable

# # Initialize Faker to generate mock data
# fake = Faker()

# # Define a list of cities to simulate data from
# CITIES = [
#     'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
#     'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'
# ]

# def generate_trip_data(city: str) -> dict:
    
#     # Generates a single, realistic-looking ride-hailing trip record.
#     # Simulate a trip that lasts between 5 and 60 minutes
#     trip_duration_seconds = random.randint(300, 3600)
#     start_time = fake.date_time_this_year()
#     end_time = start_time + timedelta(seconds=trip_duration_seconds)

#     return {
#         "trip_id": str(uuid.uuid4()),
#         "driver_id": fake.uuid4(),
#         "customer_id": fake.uuid4(),
#         "pickup_datetime": start_time.isoformat(),
#         "dropoff_datetime": end_time.isoformat(),
#         "pickup_location": {
#             "latitude": str(fake.latitude()),
#             "longitude": str(fake.longitude())
#         },
#         "dropoff_location": {
#             "latitude": str(fake.latitude()),
#             "longitude": str(fake.longitude())
#         },
#         "fare_amount": round(random.uniform(5.0, 150.0), 2),
#         "tip_amount": round(random.uniform(0.0, 50.0), 2),
#         "city": city,
#         "event_timestamp": time.time()
#     }

# def create_kafka_producer(broker: str) -> KafkaProducer:
    
#     # Creates and returns a KafkaProducer instance.
#     # Handles connection retries, which is crucial for distributed systems.
    
#     print(f"Attempting to connect to Kafka broker at {broker}...")
#     while True:
#         try:
#             # Create the producer client
#             producer = KafkaProducer(
#                 # The address of our Kafka broker
#                 bootstrap_servers=[broker],
#                 # We serialize our dictionary to JSON, then encode it to bytes for sending
#                 value_serializer=lambda v: json.dumps(v).encode('utf-8')
#             )
#             print("Successfully connected to Kafka!")
#             return producer
#         except NoBrokersAvailable:
#             print(f"Could not find any brokers at {broker}. Retrying in 10 seconds...")
#             time.sleep(10)
#         except Exception as e:
#             # Catch other potential exceptions during connection
#             print(f"An unexpected error occurred: {e}. Retrying in 10 seconds...")
#             time.sleep(10)

# def main(args):
#     """
#     The main function that orchestrates running the producer.
#     """
#     producer = create_kafka_producer(args.broker)
#     # Calculate the sleep time based on the desired message rate
#     delay = 1.0 / args.rate
#     print(f"Starting to send data to topic '{args.topic}' at a rate of {args.rate} messages/sec.")
    
#     try:
#         # Infinite loop to continuously generate and send data
#         while True:
#             city = random.choice(CITIES)
#             trip_data = generate_trip_data(city)
            
#             # Send the data to the specified Kafka topic
#             producer.send(args.topic, value=trip_data)
            
#             # Log to the console so we can see the script is working
#             print(f"Sent: {trip_data['trip_id']} for city {trip_data['city']}")
            
#             # Wait for the calculated delay
#             time.sleep(delay)

#     except KeyboardInterrupt:
#         print("\nProducer stopped by user.")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#     finally:
#         # This block is crucial for ensuring that even if the script is stopped
#         # or crashes, it attempts to send any messages still in the buffer.
#         print("Flushing remaining messages and closing producer...")
#         if producer:
#             producer.flush()
#             producer.close()
#         print("Producer closed.")

# if __name__ == "__main__":
#     # '__main__' is the entry point of any Python script.
#     # The argparse library lets us define and parse command-line arguments.
#     # This makes our script configurable without changing the code.
#     parser = argparse.ArgumentParser(description="Real-Time Ride-Hailing Data Producer")
    
#     parser.add_argument(
#         "--broker",
#         default="localhost:9092",
#         help="The Kafka broker address (e.g., 'localhost:9092')."
#     )
#     parser.add_argument(
#         "--topic",
#         default="ride_events",
#         help="The Kafka topic to which messages are sent."
#     )
#     parser.add_argument(
#         "-r", "--rate",
#         type=int,
#         default=1,
#         help="The rate (messages per second) at which to send data."
#     )
    
#     # Parse the arguments provided from the command line
#     cli_args = parser.parse_args()
    
#     # Run the main logic of the script
#     main(cli_args)
