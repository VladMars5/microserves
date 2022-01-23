from kafka import KafkaConsumer
import threading
import flask
from pymongo import MongoClient
from json import loads

port = 5002


client = MongoClient('localhost', 27017)
database = client.get_database(name = "project1")

def consume():
    consumer = KafkaConsumer(
        'management',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message = message.value
        data_stat = message['tasks']

        if len(data_stat) != 0:
            stat_pip = 0
            stat_time = 0
            stat_price = 0

            for task in data_stat:
                if "count_pip" not in task:
                    task['count_pip'] = 0

                if 'time' not in task:
                    task['time'] = 0

                if 'price' not in task:
                    task['price'] = 0

                stat_pip += task['count_pip']
                stat_time += task['time']
                stat_price += task['price']

            indicators = {}
            stat_pip = stat_pip/len(data_stat)
            stat_time = stat_time/len(data_stat)
            stat_price = stat_price/len(data_stat)
            indicators["Сред кол-во людей"] = stat_pip
            indicators["Сред время выполнения"] = stat_time
            indicators["Сред стоимость"] = stat_price
        else:
            indicators = 0

        database['statistic'].insert_one({
            'project_id': message['_id'],
            'indicators': indicators,
            #'client_ip': request.environ['REMOTE_ADDR'],
            #'server_ip': request.remote_addr,
            'port': port
        })

th = threading.Thread(target=consume)
th.start()

app = flask.Flask('__main__')

@app.get('/api/projects/all')
def api_all():
    data = database['statistic'].find()
    projects = []
    for project in data:
        project["_id"] = str(project["_id"])
        projects.append(project)
    return f"{projects}"


app.run(port=port)
