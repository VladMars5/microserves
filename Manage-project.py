from bson import ObjectId
from kafka import KafkaConsumer, KafkaProducer
from json import loads
import threading
import flask
from flask import request, jsonify
import pymongo
from pymongo import MongoClient
from json import dumps


client = MongoClient('localhost', 27017)
database = client.get_database(name = "project1")

def consume():
    consumer = KafkaConsumer(
        'registration',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message = message.value
        database['tasks'].insert_one(message)
        print(message)


th = threading.Thread(target=consume)
th.start()


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=lambda x: 
dumps(x).encode('utf-8'))

app = flask.Flask('__main__')

@app.route('/api/projects/all', methods = ['GET'])
def api_all():
    data = database['tasks'].find()
    projects = []
    for project in data:
        project["_id"] = str(project['_id'])
        projects.append(project)
    return jsonify(projects)


@app.put('/api/projects/update')
def update():
    project = request.json
    database['tasks'].update_one({'_id': project['_id']}, {'$set': {'tasks': project['tasks']}})
    project['_id'] = str(project['_id'])
    producer.send('management', value={'_id': project['_id'], 'tasks': project['tasks']})
    return jsonify(project)

app.run(port=5001)

