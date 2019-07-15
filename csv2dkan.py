from contextlib import closing
import requests
import csv
import codecs
import dkanhandler
import config as cfg
import sys

url = cfg.csv_url
print("Data url:", url)

dkanhandler.connect(cfg)

onlyImportTheseIds = ""
if len(sys.argv) > 1:
    onlyImportTheseIds = str(sys.argv[1:])
    print("Only importing the following ids: ", onlyImportTheseIds)


def processDataset(data, resources):
    global dkanhandler, datasets, onlyImportTheseIds
    if (not onlyImportTheseIds) or (onlyImportTheseIds and (data['id'] in onlyImportTheseIds)):
        try:
            if ('nid' in data) and data['nid']:
                existingDataset = dkanhandler.getDatasetDetails(data['nid'])
            else:
                existingDataset = dkanhandler.find(data['name'])
            print()
            print('-----------------------------------------------------')
            print(data['name'], existingDataset['nid'] if existingDataset and 'nid' in existingDataset else ' => NEW')
            if existingDataset:
                nid = existingDataset['nid']
                dkanhandler.update(nid, data)
            else:
                nid = dkanhandler.create(data)

            dataset = dkanhandler.getDatasetDetails(nid)
            # print("RETRIEVED", dataset)
            updateResources(dataset, resources)
            datasets.append(nid)
        except:
            print("data", data)
            print("resources", resources)
            print("existingDataset", existingDataset)
            print("Unexpected error:", sys.exc_info())
            raise

def updateResources(dataset, resources):
    if (('field_resources' in dataset) and ('und' in dataset['field_resources'])):
        existingResources = dataset['field_resources']['und']
        if len(existingResources):
            dkanhandler.updateResources(resources, existingResources, dataset)
    else:
        # Create all resources
        for resource in resources:
            dkanhandler.createResource(resource, dataset['nid'], dataset['title'])

datasets = []
data = {}
resources = []

# Read our special CSV format
# and create or update DKAN entries
with closing(requests.get(url, stream=True)) as r:
    reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=cfg.csv_separator, quotechar='"')
    for row in reader:

        # Handle each row
        if row[0]:
            if 'id' in data:
                processDataset(data, resources)
                # exit(0)

            data = {"id": row[0]}
            resources = []
        if row[1]:
            if row[1][-9:] == "-external":
                fieldName = row[1][0:-9]
                downloadUrl = row[3]
                print("Downloading external content for '" + fieldName + "':", downloadUrl)
                r = requests.get(downloadUrl)
                data[fieldName] = (data[fieldName] if fieldName in data else '') + r.text
            else:
                data[row[1]] = row[3]
        if row[2]:
            resources.append({
                "type": row[2],
                "url": row[3],
                "title": row[4] if len(row) > 4 else '',
                "body": row[5] if len(row) > 5 else ''
            })

processDataset(data, resources)

print()
print("List of existing datasets", datasets)
print()
print("FINISHED")
