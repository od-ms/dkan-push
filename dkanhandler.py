import os
import hashlib
import requests
from dkan.client import DatasetAPI

# Default data for DATASETS
def getDkanData(data):
    if not(data['name'] and data['desc'] and data['tags']):
        raise Exception('Missing data entry', data)

    # if description does not contain html, then add html linebreaks
    description = data['desc']
    if ('\n' in description) and not ('<' in description):
        description = description.replace('\n', '<br />')

    dkanData = {
        "type": "dataset",
        "title": data['name'],
        "body": {"und": [{
            "value": description,
            "format": "full_html"  # plain_text, full_html, ...
        }]},
        "field_author": {"und": [{"value": "Stadt Münster"}]},
        "og_group_ref": {"und": [40612]},
        "field_license": {"und": {"select": "Datenlizenz Deutschland – Namensnennung – Version 2.0"}},
        "field_spatial_geographical_cover": {"und": [{"value": "Münster"}]},
        "field_granularity": {"und": [{"value": "longitude/latitude"}]},
        "field_spatial": {"und": {"master_column": "wkt", "wkt": "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[7.5290679931641,51.89293553285],[7.5290679931641,52.007625513725],[7.7350616455078,52.007625513725],[7.7350616455078,51.89293553285]]]},\"properties\":[]}]}"}},
        "field_tags": {"und": {"value_field": ("\"\"\"" + data['tags'] + "\"\"\"")}},
    }

    groupData = {
        "Stadtwerke Münster": {
            "field_author": {"und": [{"value": "Stadtwerke Münster"}]},
            "og_group_ref": {"und": [40845]},
            "field_license": {"und": {"select": "notspecified"}}
        },
        "Leihleeze": {
            "field_author": {"und": [{"value": "Leihleeze.de"}]},
            "og_group_ref": {"und": [40865]},
        }
    }

    if ("group" in data) and data["group"]:
        if not data["group"] in groupData:
            raise Exception("groupData not found for group. Please define the following group in dkanhandler.py:", data["group"])
        dkanData.update(groupData[data["group"]])

    if "musterds" in data:
        additional_fields = [
            {"first": "Kategorie", "second": data['musterds'], "_weight": 0},
            {"first": "Kennziffer", "second": data['id'], "_weight": 1}
        ]
        if "Koordinatenreferenzsystem" in data:
            additional_fields.append({"first": "Koordinatenreferenzsystem", "second": data['Koordinatenreferenzsystem'], "_weight": 2})
        if "Quelle" in data:
            additional_fields.append({"first": "Quelle", "second": data['Quelle'], "_weight": 3})

        dkanData["field_additional_info"] = {"und": additional_fields}

    return dkanData


def connect(args):
    global api
    print("DKAN url:", args.dkan_url)

    api = DatasetAPI(args.dkan_url, args.user, args.passw)


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
    if r.status_code == 404:
        raise Exception('Did not find existing entry to update:', nid)

    return r.json()

# Base data for RESOURCE URLS
def getResourceDkanData(resource, nid, title):
    isUpload = False

    rFormat = resource['type']
    if (rFormat[0:3] == "WFS"):  # omit WFS Version in type
        rFormat = "WFS"

    if (rFormat[0:3] == "WMS"):  # omit WMS Details in type
        rFormat = "WMS"

    if (rFormat[-7:] == '-upload'):
        resource['type'] = rFormat = rFormat[0:-7]
        isUpload = True

    rTitle = title + " - " + resource['type']
    if ('title' in resource) and resource['title']:
        rTitle = resource['title']

    rData = {
        "type": "resource",
        "field_dataset_ref": {"und": [{"target_id": "Name (" + nid + ")"}]},
        "title": rTitle,
        "body": {"und": [{
            "value": resource['body'] if ("body" in resource) and resource['body'] else "",
            "format": "plain_text"
        }]},
        "field_format": {"und": {"textfield": rFormat.lower()}},
        "field_link_remote_file": {"und": [{
            "filefield_dkan_remotefile": {"url": ""},
            "fid": 0,
            "display": 1
        }]}
    }
    if isUpload:
        rData.update({
            "upload_file": resource['url'],
            "field_link_api": {"und": [{"url": ""}]},
        })
    else:
        rData.update({
            "field_link_api": {"und": [{"url": resource['url']}]},
        })

    return rData


def createResource(resource, nid, title):
    global api
    print("[create]", resource)
    data = getResourceDkanData(resource, nid, title)
    r = api.node('create', data=data)
    resourceResponse = r.json()
    print(r, resourceResponse)
    raise Exception("need to find out resource node id here")
    handleFileUpload(data, nid)

def updateResource(data, nodeId):
    global api
    print("[update]", data['title'])
    r = api.node('update', node_id=nodeId, data=data)
    if r.status_code != 200:
        raise Exception('Error during update:', r, r.json())
    handleFileUpload(data, nodeId)

def handleFileUpload(data, nodeId):
    global api

    if "upload_file" in data:
        # download remote content
        downloadUrl = data["upload_file"]
        filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.', 'temp-files',
            hashlib.md5(downloadUrl.encode('utf-8')).hexdigest()) + '.csv'
        print(nodeId, "[file-upload]", downloadUrl)
        print("    Temp file:", filename)
        del data['upload_file']
        r = requests.get(downloadUrl)
        if r.status_code != 200:
            raise Exception('Error during download')

        # save content to temp file
        f = open(filename, "w")
        f.write(r.text)
        f.close()

        # attach temp file to resource
        a = api.attach_file_to_node(filename, nodeId, 'field_upload')
        print(a.status_code, a. text        )

def updateResources(newResources, existingResources, dataset, forceUpdate):
    print("CHECKING RESOURCES")
    for existingResource in existingResources:

        print(existingResource['target_id'], end=' ')
        # TODO: existingResource is crap, because it's only a list of target_ids!
        # TODO: Don't use existingResource, use resourceData instead!

        resourceData = getDatasetDetails(existingResource['target_id'])
        if "und" in resourceData['field_link_api']:
            rUrl = resourceData['field_link_api']['und'][0]['url']
        elif 'und' in resourceData['field_link_remote_file']:
            rUrl = resourceData['field_link_remote_file']['und'][0]['uri']
        elif 'und' in resourceData['field_upload']:
            rUrl = resourceData['field_upload']['und'][0]['filename']
            print("WARNING -- uploaded file .. update does not work yet")
            raise Exception('todo this does not work', resourceData)
        else:
            raise Exception('Unknown resource: no url or uri', resourceData)

        # check if the existing resource url also is in the new resource urls
        el = [x for x in newResources if x['url'] == rUrl]
        if el:
            # Found url => That means this is an update

            # remove element from the resources that will be created
            newResources = [x for x in newResources if x['url'] != rUrl]

            # Only update if resource title has changed
            newData = getResourceDkanData(el[0], dataset['nid'], dataset['title'])
            if (newData['title'] != resourceData['title']) or forceUpdate:
                updateResource(newData, existingResource['target_id'])
            else:
                print("[no-change]", rUrl)
        else:
            # This seems to be an old url that we dont want anymore => delete it
            print("[remove]", existingResource)
            op = api.node('delete', node_id=existingResource['target_id'])
            print (op.status_code, op.text)

    # Create new resources
    for resource in newResources:
        createResource(resource, dataset['nid'], dataset['title'])

