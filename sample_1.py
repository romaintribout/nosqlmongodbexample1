import requests
import json
from pprint import pprint
from pymongo import MongoClient

def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


vlilles = get_vlille()

vlilles_to_insert = [
    {
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]

# pprint(vlilles_to_insert)

atlas = MongoClient('mongodb+srv://romain:isen123456@cluster0.1xili.mongodb.net/bicycle?retryWrites=true&w=majority')

db = atlas.bicycle

# db.stations.insert_many(vlilles_to_insert)

for vlille in vlilles_to_insert:
    db.stations2.insert_one(vlille)