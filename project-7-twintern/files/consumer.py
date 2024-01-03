import sys
from kafka import KafkaConsumer
from kafka import TopicPartition
import os 
import json
from report_pb2 import *

parameters = sys.argv[1:]
partitions = {}
jsonfilenames = {}
jsonfilecontent = {}
broker = "localhost:9092"
consumer = KafkaConsumer(bootstrap_servers = [broker])

for p in parameters:
    partitions[int(p)] = TopicPartition('temperatures', int(p))
    jsonfilenames[int(p)] = (f'partition-{p}.json', int(p))

consumer.assign(partitions.values())
#print(consumer.assignment())
for name, p in jsonfilenames.values():
    if os.path.exists(name):
        with open(name, 'r') as f:
            jsonfilecontent[p] = json.loads(f.read())
            #consumer.seek(partitions[p], jsonfilecontent[p]['offset'])
            #print(consumer.position(TopicPartition('temperatures', p)))
            consumer.seek(partitions[p], consumer.position(TopicPartition('temperatures', p)))
    else:
        jsonfilecontent[p] = {'partition':p, 'offset':0}
        consumer.seek(partitions[p], 0)

#print(jsonfilecontent)

while True:
    batch = consumer.poll(1000)
    for part , messages in batch.items():
        for msg in messages:
            s = Report.FromString(msg.value)
            key = msg.key.decode('utf-8')
            date = s.date
            year = date.split('-')[0]
            degrees = s.degrees
            p = msg.partition
            #print(p,key)
            if p == 0 and key == 'January':
                #print(part, msg)
                print('fuck!!!!!!!!!!!')
            if key not in jsonfilecontent[p]:
                jsonfilecontent[p][key] = {}
            if year not in jsonfilecontent[p][key]:
                jsonfilecontent[p][key][year] = {}
                jsonfilecontent[p][key][year]['start'] = date
                jsonfilecontent[p][key][year]['end'] = date
                jsonfilecontent[p][key][year]['count'] = jsonfilecontent[p][key][year].get('count',0) + 1
                jsonfilecontent[p][key][year]['sum'] = degrees
                jsonfilecontent[p][key][year]['avg'] = degrees
            #jsonfilecontent[p]['offset'] = consumer.position(TopicPartition('temperatures', p))
            if date <= jsonfilecontent[p][key][year]['end']:
                continue
            #jsonfilecontent[p]['offset'] = msg.offset
            jsonfilecontent[p]['offset'] = consumer.position(TopicPartition('temperatures', p))
            jsonfilecontent[p][key][year]['count'] = jsonfilecontent[p][key][year].get('count',0) + 1
            jsonfilecontent[p][key][year]['sum'] = jsonfilecontent[p][key][year].get('sum',0) + degrees
            jsonfilecontent[p][key][year]['end'] = date
            jsonfilecontent[p][key][year]['avg'] = jsonfilecontent[p][key][year]['sum'] / jsonfilecontent[p][key][year]['count']
        #print(jsonfilecontent)          


    for p, content in jsonfilecontent.items():
        with open('/files/' + jsonfilenames[p][0]+'.tmp', 'w') as f:
            json.dump(content, f, indent=2)
            os.rename('/files/' + jsonfilenames[p][0]+'.tmp', '/files/' + jsonfilenames[p][0])
    








