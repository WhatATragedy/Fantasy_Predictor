import pandas as pd
import requests
import json
import datetime
from elasticsearch import Elasticsearch
import numpy as np

_mapping = {

}
def es_connect():
    res = requests.get("http://localhost:9200")
    print(res.content)
    es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
    return es

def send_elements(es, data):
    for node in data:
        _id = node['code']
        node['ingest_date'] = datetime.datetime.now()
        node['form'] = float(node['form'])
        node['team'] = str(node['team'])
        # for key, value in node.items():
        #     print(f"Key: {key}, Val: {value}, Type: {type(value)}")
        es.index(index='fantasy-elements', id=_id, body=node)

def merge_data(elements, teams, positions):
    elements_df = pd.DataFrame(elements)
    teams_df = pd.DataFrame(teams)
    positions_df = pd.DataFrame(positions)
    elements_df['position'] = elements_df.element_type.map(positions_df.set_index('id').singular_name)
    elements_df['team'] = elements_df.team.map(teams_df.set_index('id').name)
    elements_df = pd.DataFrame(elements_df).replace({np.nan:None})
    return elements_df.to_dict('records')

def send_teams(es, data):
    for node in data:
        node['ingest_date'] = datetime.datetime.now()
        es.index(index='fantasy-teams', id=node['id'], body=node)

def get_mapping_from_index(es, index):
    response = es.indices.get_mapping(index=index)
    return response

def create_index(es, mapping, index_name):
    es.indices.delete(index_name, ignore=404)
    es.indices.create(
        index=index_name,
        body=json.load(open(mapping, 'r'))
    )
    return True




url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
r = requests.get(url)
fantasy = r.json()
data = merge_data(fantasy['elements'], fantasy['teams'], fantasy['element_types'])
es = es_connect()
create_index(es, "mapping.json", "fantasy-elements")
send_elements(es, data)
send_teams(es, fantasy['teams'])




