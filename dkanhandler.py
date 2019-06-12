from dkan.client import DatasetAPI
import argparse

def getDkanData(data):
    if (not(data['name'] and data['desc'] and data['tags'])):
        raise Exception('Missing data entry', data)

    dkanData = {
        "type": "dataset",
        "title": data['name'],
        "body": {"und":[{"value":data['desc']}]},
        "field_author": {"und":[{"value":"Stadt Münster"}]},
        "field_license": {"und":{"select":"Datenlizenz Deutschland – Namensnennung – Version 2.0"}},
        "field_spatial_geographical_cover": {"und":[{"value":"Münster"}]},
        "field_granularity": {"und":[{"value":"longitude/latitude"}]},
        "field_spatial": {"und":{"master_column":"wkt","wkt":"{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[7.5290679931641,51.89293553285],[7.5290679931641,52.007625513725],[7.7350616455078,52.007625513725],[7.7350616455078,51.89293553285]]]},\"properties\":[]}]}"}},
        "field_tags": {"und":{"value_field": ("\"\"" + data['tags'] +  "\"\"")}},
    }
    if "musterds" in data:
        dkanData["field_additional_info"] = {"und":[
            {"first":"Kategorie","second":data['musterds']},

            # TODO => For some reason the second entry does not work!
            {"first":"Identifier","second":data['id']}
        ]}

    return dkanData

def connect():
    global api
    parser = argparse.ArgumentParser()
    parser.add_argument("uri")
    parser.add_argument("user")
    parser.add_argument("passw")
    args = parser.parse_args()
    print("DKAN url:", args.uri)

    api = DatasetAPI(args.uri, args.user, args.passw)

def create(data):
    global api
    print("Creating", data['name'])
    res = api.node('create', data=getDkanData(data))
    print("result", res.json())
    return res.json()['nid']

def update(nodeId, data):
    global api
    print("Updating", data['name'])
    res = api.node('update', node_id=nodeId, data=getDkanData(data))
    print("result", res.json())


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

def getDatasetDetails(nid):
    global api
    r = api.node('retrieve', node_id=nid)
    return r.json()

def createResource(resource, nid, title):
    global api
    print("[create]", resource)
    rData = {
        "type": "resource",
        "field_dataset_ref": {"und": [{"target_id": "Name (" + nid + ")"}]},
        "title": title + " (" + resource['type'] + ")",
        "field_link_api": {"und":[{"url":resource['url']}]},
        "field_format": {"und":{"textfield":resource['type']}}
    }
    r = api.node('create', data=rData)
    print(r, r.json())

def updateResources(resources, existingResources, dataset):
    print("CHECKING RESOURCES")
    for existingResource in existingResources:
        print(existingResource['target_id'], end=' ')
        resourceData = getDatasetDetails(existingResource['target_id'])
        if "und" in resourceData['field_link_api']:
            rUrl = resourceData['field_link_api']['und'][0]['url']
        elif 'und' in resourceData['field_link_remote_file']:
            rUrl = resourceData['field_link_remote_file']['und'][0]['uri']
        else:
            raise Exception('Unknown resource: no url or uri', resourceData)

        el = [x for x in resources if x['url'] == rUrl]
        if el:
            # Found url => remove it from the resources that will be created
            resources = [x for x in resources if x['url'] != rUrl]
            print("[no-change]", rUrl)
        else:
            # This seems to be an old url that we dont want anymore => delete it
            print("[remove]", existingResource)
            op = api.node('delete', node_id=existingResource['target_id'])
            print (op.status_code, op.text)

    # Create new resources
    for resource in resources:
        createResource(resource, dataset['nid'], dataset['title'])

