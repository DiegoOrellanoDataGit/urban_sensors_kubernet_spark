from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers= 'kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    msg = {
        "sensor_id": random.randint(1,5),
        "temperature": round(random.uniform(15,35),2),
        "humidity": round(random.uniform(30,70),2),
        "air_quality_index": random.randint(50,150),
        "timestamp": int(time.time()* 1000)
    }

    producer.send('urban-sensors', msg)
    print("Sent: ", msg)
    time.sleep(1)