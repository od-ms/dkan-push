from dkan.client import DatasetAPI
import argparse

def getDkanData(data):
    if (not(data['name'] and data['desc'] and data['tags'])):
        raise Exception('Missing data entry', data)

    return {
        "type": "dataset",
        "title": data['name'],
        "body": {"und":[{"value":data['desc']}]},
        "field_author": {"und":[{"value":"Stadt Münster"}]},
        "field_license": {"und":{"select":"Datenlizenz Deutschland – Namensnennung – Version 2.0"}},
        "field_spatial_geographical_cover": {"und":[{"value":"Münster"}]},
        "field_granularity": {"und":[{"value":"longitude/latitude"}]},
        "field_spatial": {"und":{"master_column":"wkt","wkt":"{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[7.5290679931641,51.89293553285],[7.5290679931641,52.007625513725],[7.7350616455078,52.007625513725],[7.7350616455078,51.89293553285]]]},\"properties\":[]}]}"}},
        "field_tags": {"und":{"value_field": ("\"\"" + data['tags'] +  "\"\"")}}
    }

def create(data):
    global api
    print("Creating", data['name'])
    res = api.node('create', data=getDkanData(data))
    print("result", res)

def update(nodeId, data):
    global api
    print("Updating", data['name'])
    res = api.node('update', node_id=nodeId, data=getDkanData(data))
    print("result", res)

def connect():
    global api
    parser = argparse.ArgumentParser()
    parser.add_argument("uri")
    parser.add_argument("user")
    parser.add_argument("passw")
    args = parser.parse_args()
    print("DKAN url:", args.uri)

    api = DatasetAPI(args.uri, args.user, args.passw)


def find(title):
    global api
    params = {
        'parameters[type]': 'dataset',
        'parameters[title]': title
    }
    results = api.node(params=params).json()
    if len(results):
        return results[0]
    else:
        return 0
