import requests
from contextlib import closing
import csv
import codecs

url = "https://gist.githubusercontent.com/od-ms/7760fc049ae68df5b973dafd1c9d5be5/raw/66d4d138e99a0454d654714d2209e91e5b7f4dce/test.csv"

print("processing url ", url)

with closing(requests.get(url, stream=True)) as r:
    reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter="\t", quotechar='"')
    for row in reader:
        # Handle each row here...
        print(row[3])

