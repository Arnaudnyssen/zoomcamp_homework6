import json
import time 
import pandas as pd

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

connected=producer.bootstrap_connected()
print("Producer connected to Kafka servers:", connected)
t0 = time.time()

topic_name = 'green-trips'

df_green = pd.read_csv('/Users/arnaud/Downloads/green_tripdata_2019-10.csv.gz',compression='gzip')

# Define columns to keep
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

# Iterate over the records in the dataframe and send them to Kafka
for row in df_green[columns_to_keep].itertuples(index=False):
    # Convert row to dictionary
    row_dict = {col: getattr(row, col) for col in columns_to_keep}
    
    # Send data to Kafka topic
    producer.send(topic_name, value=row_dict)
    
# Flush messages
producer.flush()
t1 = time.time()
print(f'it took {(t1-t0)} seconds')
# Close the producer
producer.close()
