import json # Kafka only speaks bytes, not Python objects. json converts our data to text first
import time 
from kafka import KafkaProducer  # type: ignore
from nasa_firms_client import fetch_fire_data

#tw bsh naamel l cration mtaa lproducer 
##A producer is like a news reporter — it goes out, collects information (NASA fire data), and broadcasts it to a channel (Kafka topic). It doesn't care who's listening, it just keeps sending.

producer = KafkaProducer (
    bootstrap_servers='localhost:9092', #This is the address of your Kafka broker (the server running in Docker)
    value_serializer=lambda v: json.dumps(v).encode('utf-8')#This is a function that automatically runs on every message before sending.:
)

TOPIC='fire_raw' #A topic is like a named mailbox inside Kafka : Producers drop messages into a topic,Consumers pick messages from a topic ,Later we'll have topics like fires_enriched, fires_alerts, etc.
def produce_fire_events():
    print('Fetching fire data from nasa....')
    df=fetch_fire_data()
    records=df.to_dict(orient='records') #Why convert to dict?Kafka sends individual messages, not full tables.# Each dict = one independent Kafka message = one fire detection
    for i, record in enumerate(records):
        producer.send(TOPIC, value=record)#We loop through every fire detection and send it as one individual message to Kafka.( asynchronous )
        if i % 10 == 0:
            print(f"  → Sent {i}/{len(records)} records...")
            # WHY print every 10?
            # So you can SEE progress instead of wondering
            # if the script is frozen
    producer.flush()#Because send() is async, messages are stored in an internal buffer first. flush() says: "Stop, wait, make sure EVERY buffered message is actually delivered to Kafka before continuing."
    print(f" All {len(records)} events sent to Kafka!")

while True :
    produce_fire_events()
    print('waiting 1 hour before next fetch ....')
    time.sleep(3600)