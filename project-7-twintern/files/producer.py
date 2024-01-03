from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
from report_pb2 import *
import time
import weather

map = {1:'January', 2:'February', 3:'March', 4:'April', 5:'May', 6:'June', 7:'July', 8:'August', 9:'September', 10:'October', 11:'November', 12:'December'}
broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])
producer = KafkaProducer(bootstrap_servers=[broker], acks = 'all', retries = 10)
try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

# TODO: Create topic 'temperatures' with 4 partitions and replication factor = 1
try:
    admin_client.create_topics([NewTopic('temperatures', num_partitions=4, replication_factor=1)])
except TopicAlreadyExistsError:
    print('already exists')
print("Topics:", set(admin_client.list_topics()))
for date, degrees in weather.get_next_weather(delay_sec=0.1):
    value = Report(date = date, degrees = degrees).SerializeToString()
    month = int(date.split('-')[1])
    producer.send('temperatures', value = value, key = bytes(map[month], "utf-8"))