import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime
import pandas as pd
import numpy as np

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():
    topic = "vdt2024"
    producer = SerializingProducer(
        {
            "bootstrap.servers": "localhost:9093"
        }
    )

    df = pd.read_csv("./data/log_action.csv",header=None)
    df
    it = 0
    while it < len(df):
        try:
            record_send ={
                "student_code":int(df.iloc[it,0]),
                "activity":df.iloc[it,1],
                "numberOfFile":int(df.iloc[it,2]),
                "timestamp":df.iloc[it,3],
            }
            print(record_send)

            producer.produce(
                topic,
                value=json.dumps(record_send),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            it += 1
            # wait for 5 seconds before sending the next transaction
            time.sleep(1)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()