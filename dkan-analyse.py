from contextlib import closing
import re
import csv
import sys
import json
import codecs
import requests
import config as cfg

url = cfg.csv_url
print("Data url:", url)

importOptions = ""
onlyImportTheseIds = ""
skipSoManyDatasets = 0
if len(sys.argv) > 1:
    importOptions = str(sys.argv[1:])
    print("Import options:", importOptions)
    match = re.search(r'-skipCount=(\d+)', importOptions)
    if match:
        skipSoManyDatasets = int(match.group(1))
        print('Skipping', skipSoManyDatasets, 'Datasets.')

    match = re.search(r'-ids=([-\w\d,]+)', importOptions)
    if match:
        onlyImportTheseIds = match.group(1)
        print("Only importing the following ids: ", onlyImportTheseIds)


def processDataset(data, resources):
    """Create a DKAN dataset entry"""
    global dkanhandler, datasets, onlyImportTheseIds, skipSoManyDatasets, importOptions
    skipSoManyDatasets -= 1
    if skipSoManyDatasets < 0 and ((not onlyImportTheseIds) or (onlyImportTheseIds and (data['id'] in onlyImportTheseIds))):
        try:
            # Special feature: download external resources into a field.
            # E.g. this can be used for "desc-external" => if this contains a url then "desc" is filled with the content from that url.
            #for hashkey in data:
                #if hashkey[-9:] == "-external":
                    #fieldName = hashkey[0:-9]
                    #downloadUrl = data[hashkey]
                    #print("Downloading external content for '" + fieldName + "':", downloadUrl)
                    #r = requests.get(downloadUrl)
                    #data[fieldName] = (data[fieldName] if fieldName in data else '') + r.text

            #print()
            #print('-----------------------------------------------------')
            #print("data", data)
         
            datasets.append(data)
        except:
            print("resources", resources)
            print("Unexpected error:", sys.exc_info())
            raise



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
            resources.append({
                "type": row[2],
                "url": row[3],
                "title": row[4] if len(row) > 4 else '',
                "body": row[5] if len(row) > 5 else ''
            })

processDataset(data, resources)

print()
print("Found datasets: ", len(datasets))
print()


#
# dkan dataset source analysis
#
sourceCount = {}
sourceFiles = {}
othersCount = {}
for dataset in datasets:
    if 'group' in dataset:
        print(dataset['group'])

        quelle = dataset['group']
        if quelle in othersCount:
            othersCount[quelle] += 1
        else: 
            othersCount[quelle] = 1

    else:
        if ('Quelle' in dataset):
            quelle = dataset['Quelle']
            if quelle in sourceCount:
                sourceCount[quelle] += 1
            else: 
                sourceCount[quelle] = 1

            if quelle in sourceFiles: 
                sourceFiles[quelle].append(dataset['name'])
            else: 
                sourceFiles[quelle]=[dataset['name']]

        else:
            print("Missing Quelle in dataset:")
            print(dataset)


print("--------------------RESULTS------------------")
infoFile = {
    'counts': sourceCount,
    'datasets': sourceFiles,
    'groups': othersCount
}
print(infoFile)

with open('public/muenster-dkan.json', 'w') as outfile:
    json.dump(infoFile, outfile)
