from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


broker = "localhost:9092"
admin = KafkaAdminClient(bootstrap_servers=[broker])
try:
    admin.create_topics([NewTopic(name="even_nums", num_partitions=1, replication_factor=1)])
except TopicAlreadyExistsError:
    print("already exists")
print(admin.list_topics())