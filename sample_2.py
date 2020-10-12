import requests
import json
from pprint import pprint
from pymongo import MongoClient
import time
import dateutil.parser

atlas = MongoClient('mongodb+srv://romain:isen123456@cluster0.1xili.mongodb.net/bicycle?retryWrites=true&w=majority')

db = atlas.bicycle

db.datas.create_index([('station_id', 1),('date', -1)], unique=True)

def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

def get_station_id(id_ext):
    tps = db.stations.find_one({ 'source.id_ext': id_ext }, { '_id': 1 })
    return tps['_id']

while True:
    print('update')
    vlilles = get_vlille()
    datas = [
        {
            "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": get_station_id(elem.get('fields', {}).get('libelle'))
        }
        for elem in vlilles
    ]

    try:
        db.datas.insert_many(datas, ordered=False)
    except:
        pass

    time.sleep(10)