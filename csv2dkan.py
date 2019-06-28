from contextlib import closing
import requests
import csv
import codecs
import dkanhandler
import config as cfg

url = cfg.csv_url
print("Data url:", url)

dkanhandler.connect(cfg)


def processDataset(data, resources):
    global dkanhandler, datasets
    if ('nid' in data) and data['nid']:
        existingDataset = dkanhandler.getDatasetDetails(data['nid'])
    else:
        existingDataset = dkanhandler.find(data['name'])
    print()
    print('-----------------------------------------------------')
    print(data['name'], existingDataset['nid'] if 'nid' in existingDataset else ' => NEW')
    if existingDataset:
        nid = existingDataset['nid']
        dkanhandler.update(nid, data)
    else:
        nid = dkanhandler.create(data)

    dataset = dkanhandler.getDatasetDetails(nid)
    # print("RETRIEVED", dataset)
    updateResources(dataset, resources)
    datasets.append(nid)
    # print(data)
    # print(resources)


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
            data[row[1]] = row[3]
        if row[2]:
            resources.append({"type": row[2], "url": row[3], "title": row[4] if len(row) > 4 else ''})

processDataset(data, resources)

print()
print("List of existing datasets", datasets)
print()
print("FINISHED")
