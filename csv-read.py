import requests
from contextlib import closing
import csv
import codecs
import dkanhandler

url = "https://gist.githubusercontent.com/od-ms/7760fc049ae68df5b973dafd1c9d5be5/raw/test.csv"
print("Data url:", url)

dkanhandler.connect()

def processDataset(data, resources):
    global dkanhandler
    existingDataset = dkanhandler.find(data['name'])
    print(data['name'], existingDataset)
    if (existingDataset):
        dkanhandler.update(existingDataset['nid'], data)
    else:
        dkanhandler.create(data)
    # print(data)
    # print(resources)

datasets = []
data = {}
resources = []

with closing(requests.get(url, stream=True)) as r:
    reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter="\t", quotechar='"')
    for row in reader:
        # Handle each row here...
        if (row[0]):
            if ('id' in data):
                processDataset(data, resources)

            data = {"id": row[0]}
            resources = []
        if (row[1]):
            data[row[1]] = row[3]
        if (row[2]):
            resources.append({"type": row[2], "url": row[3]})

processDataset(data, resources)
