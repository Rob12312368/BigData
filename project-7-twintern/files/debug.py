from kafka import KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from report_pb2 import *
import json


broker = "localhost:9092"
consumer = KafkaConsumer(bootstrap_servers=[broker], group_id = "debug")
consumer.subscribe(['temperatures'])

while True:
    batch = consumer.poll(1000)
    for partition, messages in batch.items():
        #print(partition, messages)
        for msg in messages:
            tmp = {}
            s = Report.FromString(msg.value)
            tmp['partition'] = msg.partition
            tmp['key'] = msg.key.decode('utf-8')
            tmp['date'] = s.date
            tmp['degrees'] = s.degrees
            print(tmp)

        