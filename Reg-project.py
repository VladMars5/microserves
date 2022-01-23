import flask, json
from json import dumps
from flask import request, jsonify
from pymongo import MongoClient
from bson import ObjectId
from kafka import KafkaProducer
 
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=lambda x: 
dumps(x).encode('utf-8'))

client = MongoClient('localhost', 27017)
database = client.get_database(name = "project1")

app = flask.Flask('__main__')

@app.route('/api/projects/all', methods = ['GET'])
def api_all():
    data = database['projects'].find()
    projects = []
    for project in data:
        project["_id"] = str(project["_id"])
        projects.append(project)
    return jsonify(projects)

@app.route('/api/projects/<id>', methods = ['GET'])
def api_find_project(id):
    data = database['projects'].find_one({"_id": ObjectId(id)})
    if data != None:
        data["_id"] = str(data["_id"])
    project = data
    return jsonify(project)

@app.route('/api/projects/create', methods = ['POST'])
def add_student():
    project = request.json
    database['projects'].insert_one(project)
    project["_id"] = str(project["_id"])
    producer.send('registration', value={'_id': project['_id'], 'name': project['name'],\
                                         'start_time': project['start_time'], 'end_time': project['end_time'],\
                                         'type': project['type']})
    return jsonify(project)

app.run(port=5000)

