import json 
from kafka import KafkaConsumer  # type: ignore
import pandas as pd
from datetime import datetime
from minio_client import upload_dataframe


consumer = KafkaConsumer(
    'fire_raw',
    bootstrap_servers='localhost:9092',#Same broker address as the producer — both connect to the same Kafka server.
    auto_offset_reset='earliest',#This controls where to start reading when the consumer first connects
    group_id=None,
    enable_auto_commit=False# Automatically marks messages as "read" after processing,
    ,value_deserializer=lambda m: json.loads(m.decode('utf-8'))#This is the exact reverse of the producer's serializer: Producer:  dict → JSON string → bytes   (to send)   Consumer:  bytes → JSON string → dict   (to receive)
    ,consumer_timeout_ms=10000,

)

print("👂 Listening for fire events on topic 'fires_raw'...")


BATCH_SIZE=100
# Uploading one file per message = thousands of tiny files = very inefficient
# Instead we collect 100 messages, then save them as ONE CSV file.
# This is called "micro-batching" — a very common pattern in data engineering.


batch=[]
for message in consumer :
    fire=message.value # is an infinite loop — it waits and processes every message as it arrives. message is a Kafka object that contains several things :We only care about message.value — the fire data itself.
    batch.append(fire) #bsh nzidou e record hedha lel batch ili nekhdmou aliha 

    print(f" Fire detected | lat: {fire.get('latitude')}, "
          f"lon: {fire.get('longitude')}, "
          f"FRP: {fire.get('frp')} MW, "
          f"confidence: {fire.get('confidence')}%")
    
    if len(batch) >= BATCH_SIZE:
        # tw saye lamina e nb ili hachtna bih fil batch bsh n saviwhom 
        df=pd.DataFrame(batch)# Convert our list of dicts back into a DataFrame
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        object_name = f"viirs/{timestamp}.csv"
        upload_dataframe(df, object_name) #save data in MinIO bucket

        batch = []

        