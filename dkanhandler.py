from dkan.client import DatasetAPI

# Default data for DATASETS
def getDkanData(data):
    if not(data['name'] and data['desc'] and data['tags']):
        raise Exception('Missing data entry', data)

    dkanData = {
        "type": "dataset",
        "title": data['name'],
        "body": {"und": [{
            "value": data['desc'],
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
        "Abfallwirtschaftsbetriebe Münster (AWM)": {
            "field_author": {"und": [{"value": "Abfallwirtschaftsbetriebe Münster (AWM)"}]},
            "og_group_ref": {"und": [40848]},
        },
        "Stadtbücherei Münster": {
            "field_author": {"und": [{"value": "Stadtbücherei Münster"}]},
            "og_group_ref": {"und": [40849]},
        }
    }

    if ("group" in data) and data["group"]:
        if not data["group"] in groupData:
            raise Exception("groupData not found for group. Please define the following group in dkanhandler.py:", data["group"])
        dkanData.update(groupData[data["group"]])

    if "musterds" in data:
        additional_fields = [
            {"first": "Kategorie", "second": data['musterds'], "_weight": 0},
            {"first": "Identifier", "second": data['id'], "_weight": 1}
        ]
        if "Koordinatenreferenzsystem" in data:
            additional_fields.append({"first": "Koordinatenreferenzsystem", "second": data['Koordinatenreferenzsystem'], "_weight": 2})

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
    rFormat = resource['type']
    if (rFormat[0:3] == "WFS"):  # omit WFS Version in type
        rFormat = "WFS"

    if (rFormat[0:3] == "WMS"):  # omit WMS Details in type
        rFormat = "WMS"

    rTitle = title + " - " + resource['type']
    if ('title' in resource) and resource['title']:
        rTitle = resource['title']

    rData = {
        "type": "resource",
        "field_dataset_ref": {"und": [{"target_id": "Name (" + nid + ")"}]},
        "title": rTitle,
        "field_link_api": {"und": [{"url": resource['url']}]},
        "field_link_remote_file": {"und": [{
            "filefield_dkan_remotefile": {"url": ""},
            "fid": 0,
            "display": 1
        }]},
        "body": {"und": [{
            "value": resource['body'] if ("body" in resource) and resource['body'] else "",
            "format": "plain_text"
        }]},
        "field_format": {"und": {"textfield": rFormat.lower()}}
    }
    return rData


def createResource(resource, nid, title):
    global api
    print("[create]", resource)
    r = api.node('create', data=getResourceDkanData(resource, nid, title))
    print(r, r.json())


def updateResource(data, nodeId):
    global api
    print("[update]", data['title'])
    r = api.node('update', node_id=nodeId, data=data)
    if r.status_code != 200:
        raise Exception('Error during update:', r, r.json())


def updateResources(resources, existingResources, dataset):
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
        else:
            raise Exception('Unknown resource: no url or uri', resourceData)

        el = [x for x in resources if x['url'] == rUrl]
        if el:
            # Found url => remove it from the resources that will be created
            resources = [x for x in resources if x['url'] != rUrl]

            # Check if resource title updated
            newData = getResourceDkanData(el[0], dataset['nid'], dataset['title'])
            if newData['title'] != resourceData['title']:
                updateResource(newData, existingResource['target_id'])
            else:
                print("[no-change]", rUrl)
        else:
            # This seems to be an old url that we dont want anymore => delete it
            print("[remove]", existingResource)
            op = api.node('delete', node_id=existingResource['target_id'])
            print (op.status_code, op.text)

    # Create new resources
    for resource in resources:
        createResource(resource, dataset['nid'], dataset['title'])

